'use strict'

const async = require('async')
const libUtilSymbol = require('@bitfinex/lib-js-util-symbol')
const specLib = require('./utils/spec.parse')
const CoreBase = require('./base.core')

const pair_join = libUtilSymbol.pair_join
const pair_split = libUtilSymbol.pair_split

class CoreVar extends CoreBase {
  constructor (conf, ctx) {
    super(conf, ctx)

    console.log('WORKER', this.wtype)
  }

  init () {
    super.init()

    this.lib_spec = specLib
  }

  oJobHandle (job) {
    let part = null

    switch (job[0]) {
      case 'core.ext':
        part = {
          gw0: [[job[1], job[2], JSON.stringify(job[3])]]
        }
        break
      case 'core.ext.save':
        part = {
          gw0: [job.slice(1)]
        }
        break
    }

    return part
  }

  handleRedisStream (channel, msg, src) {
    const chp = channel.split('.')

    if (chp[2] === 'global') {
      this.handleIntGlobal(msg)
    } else {
      this.handleBase(msg)
    }
  }

  handleIntGlobal (msg) {
    const mem = this.mem

    mem.gconf = msg.o
  }

  handleBase (msg) {
    switch (msg.a) {
      case 'te_update_ticker_mem':
        this.handleTicker(msg.o, 't')
        break

      case 'te_update_status_mem':
        this.handleStatus(msg.o)
        break

      case 'update_fticker':
        this.handleTicker(msg.o, 'f')
        break
    }
  }

  handleTicker (msg, type) {
    const tick = this.lib_spec.ticker(msg)

    super.handleTicker(tick)

    const qipfx = this.conf.gw_ipfx

    this.qout_0.push([
      'core.ext', 'publish', `${qipfx}.ticker.${tick.symbol}`,
      tick
    ])

    const mem = this.mem

    if (tick.symbol[0] !== 't' || !mem.gconf || !mem.gconf.ccys_fiat) {
      return
    }

    const fiatCcys = mem.gconf.ccys_fiat
    const pccys = pair_split(tick.pair)

    if (fiatCcys.indexOf(pccys[0]) > -1 && fiatCcys.indexOf(pccys[1]) > -1) {
      this.qout_0.push([
        'core.ext', 'publish', `${qipfx}.fx`,
        tick
      ])
    }
  }

  handleStatus (msg) {
    const qipfx = this.conf.gw_ipfx
    const mkey = msg.key || 'unk'
    const phash = `${qipfx}.status.${msg.type}.${mkey}`

    this.qout_0.push([
      'core.ext', 'publish', phash,
      msg.data
    ])
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        const qipfx = this.conf.gw_ipfx
        this.redis_gw0.cli_sub.subscribe(`${qipfx}.int.global`)
        this.redis_gw0.cli_sub.psubscribe(`${qipfx}.int.ticker.*`)
        this.redis_gw0.cli_sub.psubscribe(`${qipfx}.int.status.*`)
        next()
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      next => {
        const qipfx = this.conf.gw_ipfx
        this.redis_gw0.cli_sub.unsubscribe(`${qipfx}.int.global`)
        this.redis_gw0.cli_sub.punsubscribe(`${qipfx}.int.ticker.*`)
        this.redis_gw0.cli_sub.punsubscribe(`${qipfx}.int.status.*`)
        next()
      }
    ], cb)
  }
}

module.exports = CoreVar
