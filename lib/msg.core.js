'use strict'

const _ = require('lodash')
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
    // GLOBAL CONF MANAGEMENT

    switch (act) {
      case 'global_conf':
        buf.push(
          [0, { a: 'conf', o: md }, 'int.global'],
          [0, { a: 'conf', o: md }, 'global']
        )
        break
    }

    // SNAPSHOT MANAGEMENT

    switch (act) {
      case 'sync_response': //NOTE: remove me
      case 'te_sync_data':
        buf.push.apply(
          buf,
          this.lib_msg.queueSyncEngine(this, msg, md)
        )
        break
    }

    // PUBLIC DATA MANAGEMENT

    switch (act) {
      case 'te_update_order_mem':
        buf.push.apply(
          buf,
          this.lib_msg.queuePubOrder(this, md)
        )
        break
      case 'te_trade_mem':
        buf.push.apply(
          buf,
          this.lib_msg.queuePubTrade(this, md)
        )
        break
      case 'te_update_ticker_mem':
        buf.push([0, msg, `int.ticker.t${md.pair}`])
        break
      case 'te_update_status_mem':
        buf.push([0, msg, `int.status.${md.type}`])
        break
      case 'te_update_ticker_funding_mem':
      case 'update_fticker': //NOTE :remove me
        buf.push([0, msg, `int.ticker.f${md.currency}`])
        break

        // UTILS
      case 'trigger_fx_update':
        buf.push(
          [0, { a: 'trigger_fx_update', o: md }, 'int.global']
        )
        break
    }

    // PRIV DATA MANAGEMENT

    switch (act) {
      case 'te_update_order_mem':
      case 'te_update_position_mem':
      case 'te_update_balance_mem':
      case 'te_update_user_conf_mem':
      case 'update_user_conf':
      case 'user_snap': //NOTE: remove me
      case 'snap_user':
      case 'auth_token':
      case 'notify':
        const prix = this.getRingId(md.user_id, 'priv')

        buf.push([
          this.getRingId(prix, 'bus'),
          msg,
          `int.privr${prix}`
        ])
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

    msg.a = msg.a
    msg.o = msg.o
    msg.t = msg.t ? msg.t * 1000 : Date.now()

    const act = msg.a
    const md = msg.o || {}

    this.handleRedisStreamHook0(act, msg, md, buf)
    this.handleRedisStreamHook1(act, msg, md, buf)

    for (let i = 0; i < buf.length; i++) {
      const b = buf[i]
      this.qout_0.push(
        ['core.msg', 'publish', `${qipfx}.${b[2]}`, b[1], b[0]]
      )
    }
  }

  oJobHandle (job) {
    if (job[0] === 'core.msg') {
      const ret = {}

      ret[`gw${job[4]}`] = [[job[1], job[2], JSON.stringify(job[3])]]

      return ret
    }
    return null
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
      },
      next => {
        this.qout_0.opts.oWorkPing = 1
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
