'use strict'

const async = require('async')
const CoreBase = require('./base.core')
const msgLib = require('./utils/msg.parse')

class CoreMsg extends CoreBase {
  constructor (conf, ctx) {
    super(conf, ctx)

    this.chan = ctx.chan
    this.prefix += '-' + this.chan

    console.log('WORKER', this.wtype, this.chan)
  }

  init () {
    super.init()

    this.lib_msg = msgLib

    this.setInitFacs([
      ['fac', 'bfx-facs-lru', '0', '0', { maxAge: 900000, max: 10000 }],
      ['fac', 'bfx-facs-redis', 'engine', 'engine', {}]
    ])
  }

  handleRedisStreamHook0 (act, msg, md, buf) {
    switch (act) {
      // GLOBAL_CONF, GLOBAL_SNAPSHOT AND VAR_DATA MANAGEMENT

      case 'global_conf':
        buf.push(
          [0, { a: 'conf', o: md }, 'int.global'],
          [0, { a: 'conf', o: md }, 'global']
        )
        break
      case 'te_sync_data':
        buf.push.apply(
          buf,
          this.lib_msg.queueSyncEngine(this, msg, md)
        )
        break
      case 'te_update_ticker_mem':
        buf.push([0, msg, `int.ticker.t${md.pair}`])
        break
      case 'te_update_status_mem':
        buf.push([0, msg, `int.status.${md.type}`])
        break
      case 'trigger_fx_update':
        buf.push(
          [0, { a: 'trigger_fx_update', o: md }, 'int.global']
        )
        break

        // USER DATA MANAGEMENT

      case 'te_trade_mem':
        buf.push.apply(
          buf,
          this.lib_msg.queuePubTrade(this, md)
        )
        break

      case 'te_update_order_mem':
        buf.push.apply(
          buf,
          this.lib_msg.queueOrder(this, md)
        )
        break
    }
  }

  handleRedisStreamHook1 (act, msg, md, buf) {
    switch (act) {
      case 'te_sync_start':
        buf.push(
          [0, { a: act, o: md }, 'int.global']
        )
        console.log('ACTION', act, md)
        break
    }
  }

  handleRedisStream (channel, msg, src) {
    const qipfx = this.conf.gw_ipfx

    if (channel === `${qipfx}.int.global`) {
      if (msg.a === 'conf') {
        this.handleIntGlobal(msg)
      }
      return
    }

    const buf = []

    const now = Date.now()

    const act = msg.a
    const md = msg.o || {}
    md.t_gm = now

    this.handleRedisStreamHook0(act, msg, md, buf)
    this.handleRedisStreamHook1(act, msg, md, buf)

    for (let i = 0; i < buf.length; i++) {
      const b = buf[i]
      const e = b[3] && b[3].encoded ? b[1] : JSON.stringify(b[1])

      if (b[2] === 'te_rpush') {
        this.qout_0.push(
          ['te.msg', 'rpush', 'global_engine_v2_0_1', e, b[0]]
        )
      } else {
        this.qout_0.push(
          ['core.msg', 'publish', `${qipfx}.${b[2]}`, e, b[0]]
        )
      }
    }
  }

  oJobHandle (job) {
    let ret = null

    switch (job[0]) {
      case 'te.msg':
        ret = {}
        ret['engine'] = [[job[1], job[2], job[3]]]
        break
      case 'core.msg':
        ret = {}
        ret[`gw${job[4]}`] = [[job[1], job[2], job[3]]]
        break
    }

    return ret
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        const qesrc = this.conf.gw_esrc

        for (let i = 0; i < this.conf.shards.bus; i++) {
          const gw_sub = `redis_gw${i}`
          this[gw_sub].cli_sub.subscribe(`${qesrc}${this.chan}`)
        }

        const qipfx = this.conf.gw_ipfx
        this.redis_gw0.cli_sub.subscribe(`${qipfx}.int.global`)

        next()
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      next => {
        const qesrc = this.conf.gw_esrc

        for (let i = 0; i < this.conf.shards.bus; i++) {
          const gw_sub = `redis_gw${i}`
          this[gw_sub].cli_sub.unsubscribe(`${qesrc}${this.chan}`)
        }

        const qipfx = this.conf.gw_ipfx
        this.redis_gw0.cli_sub.unsubscribe(`${qipfx}.int.global`)

        next()
      }
    ], cb)
  }
}

module.exports = CoreMsg
