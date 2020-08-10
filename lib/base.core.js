'use strict'

const path = require('path')
const _ = require('lodash')
const async = require('async')
const WrkBase = require('bfx-wrk-base')

class CoreBase extends WrkBase {
  constructor (conf, ctx) {
    super(conf, ctx)

    _.extend(
      this.conf,
      require(path.join(this.ctx.root, '/config/core.json'))
    )
  }

  init () {
    super.init()

    const facs = [
      ['fac', 'bfx-facs-util-shard', '0', '0', {}, -4],
      ['fac', 'bfx-facs-qout', '0', '0', {}, -3],
      ['fac', 'bfx-facs-scheduler', '0', '0', {}]
    ]

    for (let i = 0; i < this.conf.shards.bus; i++) {
      facs.push(['fac', 'bfx-facs-redis', `gw${i}`, `gw${i}`, {}])
    }

    this.setInitFacs(facs)
  }

  refreshConf (type, cb) {
    if (!this.redis_gw0) {
      return cb()
    }

    const qipfx = this.conf.gw_ipfx

    this.redis_gw0.cli_rw.get(
      `${qipfx}:${type}:conf`,
      (err, res) => {
        if (err) {
          console.error(err)
          return cb(err)
        }

        if (!res) {
          return cb()
        }

        try {
          res = JSON.parse(res)
        } catch (e) {
          return console.error(e)
        }

        _.extend(this.conf, res)
        cb()
      }
    )
  }

  getRingId (v, t) {
    return this.utilShard_0.getRingIx(v, this.conf.shards[t])
  }

  getStrRingId (v, t) {
    return this.utilShard_0.getStrRingIx(v, this.conf.shards[t])
  }

  setMsgTime (msg) {
    const now = Date.now()

    if (msg.o) {
      msg.o._tm = msg.t ? +msg.t * 1000 : now
      msg.o.t_gm = now
    }
  }

  handleIntGlobal (msg) {
    this.setMsgTime(msg)

    let handled = false

    switch (msg.a) {
      case 'conf':
        this.handleIntGlobalConf(msg.o)
        handled = true
        break
    }

    return handled
  }

  handleIntGlobalConf (conf) {
    _.extend(this.conf, _.pick(conf,
      [
        'ccys', 'ccys_fiat', 'ccys_margin', 'ccys_crypto', 'ccys_futures', 'ccys_paper',
        'pairs', 'pairs_exchange', 'pairs_margin', 'pairs_futures'
      ]
    ))
  }

  handleTicker (msg) {
    const mem = this.mem

    if (!mem.tickers) {
      mem.tickers = {}
    }

    this.mem.tickers[msg.symbol] = msg
  }

  _start (cb) {
    const aseries = []

    aseries.push(next => {
      super._start(next)
    })

    aseries.push(next => {
      this.loadStatus()
      next()
    })

    aseries.push(next => {
      if (!this.handleRedisStream) {
        return next()
      }

      for (let i = 0; i < this.conf.shards.bus; i++) {
        this[`redis_gw${i}`].on('message', this.handleRedisStream.bind(this))
      }

      next()
    })

    async.series(aseries, cb)
  }

  _stop (cb) {
    const aseries = []

    aseries.push(next => {
      if (!this.handleRedisStream) {
        return next()
      }

      for (let i = 0; i < this.conf.shards.bus; i++) {
        this[`redis_gw${i}`].removeListener('message', this.handleRedisStream.bind(this))
      }

      next()
    })

    aseries.push(next => {
      super._stop(next)
    })

    async.series(aseries, cb)
  }
}

module.exports = CoreBase
