'sue strict'

const fs = require('fs')
const _ = require('lodash')
const async = require('async')
const libUtilSymbol = require('@bitfinex/lib-js-util-symbol')
const libUtilNumber = require('@bitfinex/lib-js-util-number')
const RBTree = require('bintrees').RBTree
const specLib = require('./utils/spec.parse')
const CoreBase = require('./base.core')

const nBN = libUtilNumber.nBN
const pair_join = libUtilSymbol.pair_join
const pair_ccy1 = libUtilSymbol.pair_ccy1
const pair_ccy2 = libUtilSymbol.pair_ccy2

class CoreSym extends CoreBase {
  constructor (conf, ctx) {
    super(conf, ctx)

    this.symType = ctx.symbol[0]
    this.symbol = ctx.symbol
    this.pair = this.symbol.slice(1)
    this.prefix += '-' + this.symbol

    console.log('WORKER', this.wtype, this.symType, this.symbol)
  }

  init () {
    super.init()

    this.lib_spec = specLib

    this.mem.symStreamSeq = 0
    this.mem.symStreamSix = 0
    this.mem.symBookSeq = 0
    this.mem.symBookSix = 0

    this.setInitFacs([
      ['fac', 'bfx-facs-lru', '0', 'fx', { max: 25000, maxAge: 60000 * 120 }],
      ['fac', 'bfx-facs-lru', '0', 'trades', { max: 25000, maxAge: 60000 * 10 }],
      ['fac', 'bfx-facs-redis', () => {
        return `bm${this.getStrRingId(this.symbol, 'sym_book')}`
      }, 'bm', {}]
    ])

    this.entries = new Map()

    this.book = [
      new RBTree(function (a, b) {
        let d = b[0] - a[0]
        if (d !== 0) {
          return d
        }
        return a[1] - b[1]
      }),
      new RBTree(function (a, b) {
        let d = a[0] - b[0]
        if (d !== 0) {
          return d
        }
        return a[1] - b[1]
      })
    ]
  }

  getFx (ccy1, ccy2) {
    let fx = null

    if (ccy1 === ccy2) {
      fx = '1.0'
    } else {
      const tickers = this.mem.tickers || {}

      _.each([
        [`t${pair_join(ccy1, ccy2)}`, 1],
        [`t${pair_join(ccy2, ccy1)}`, -1]
      ], test => {
        const [th, dir] = test
        const tick = tickers[th]
        if (!tick) {
          return
        }

        const price = nBN(tick.bid).plus(nBN(tick.ask)).div(2)
        fx = dir > 0 ? nBN(price) : nBN(1.0).div(price)

        return false
      })

      if (fx) {
        fx = fx.toString()
      }
    }

    return fx
  }

  prepareConfHook0 () {
    this.conf.bookFreq = this.conf.bookFreqTrading
  }

  prepareConfHook1 () {
    if (!this.conf.bookFreq || this.conf.bookFreq < 50) {
      this.conf.bookFreq = 50
    }
  }

  refreshConf (cb) {
    super.refreshConf('sym', (err) => {
      if (err) {
        console.error(err)
      }

      this.prepareConfHook0()
      this.prepareConfHook1()
      if (cb) {
        cb()
      }
    })
  }

  handleRedisStream (channel, msg, src) {
    const chp = channel.split('.')

    if (chp[2] === 'global') {
      this.handleIntGlobal(msg)
    } else if (chp[1] === 'fx') {
      this.handleFx(msg)
    } else {
      this.handleCore(msg)
    }
  }

  handleIntGlobal (msg) {
    const st = this.symType

    switch (msg.a) {
      case 'conf':
        this.mem.gconf = msg.o
        break
      case 'trigger_fx_update':
        if (st === 't') {
          this.refreshEntriesFx('order', msg.o)
        }
        break
      case 'te_sync_start':
        if (st === 't') {
          this.handleIntGlobalSync()
        }
        break
    }
  }

  handleIntGlobalSync () {
    this.entries.clear()
    _.each(this.book, book => {
      book.clear()
    })
    console.log('SYM_TE_SYNC', this.symbol)
  }

  getEntryRate_t (e) {
    return e.price
  }

  handleCoreHook0 (msg) {}

  handleCore (msg) {
    if (msg.o) {
      if (msg.t) msg.o._tm = msg.t
    }

    switch (msg.a) {
      case 'te_update_order_mem':
        this.handleEntry('order', msg.o)
        break

      case 'te_trade_mem':
        this.handleTrade('trade', msg.o)
        break
    }

    this.handleCoreHook0(msg)
  }

  refreshEntriesFx (type, opts = {}) {
    const maxDepth = opts.book_depth || 0.2

    let ticks = [this.book[0].min(), this.book[1].min()]
    ticks = _.map(ticks, t => t ? t[0] : null)

    this.lru_fx.clear()

    this.entries.forEach((entry) => {
      if (entry.active < 1 || !entry._fx_req) {
        return
      }

      const tick = ticks[entry._bsix]

      const depth = tick ? Math.abs(1 - (tick / +entry.price)) : 0

      if (depth > maxDepth) {
        return
      }

      const book = this.book[entry._bsix]

      if (entry._book) {
        book.remove([+entry.price, entry.id])
      }

      this.clearEntryFx(type, entry)
      this.updateEntryFx(type, entry)

      if (entry._book) {
        book.insert([+entry.price, entry.id, entry])
      }

      this.sendEntryChange(entry, 1)
    })
  }

  clearEntryFx (type, entry) {
    this.lib_spec[`${type}_fx_clear`](entry)
  }

  updateEntryFx (type, entry) {
    entry.pair_fx = this.pair

    this.lib_spec[`${type}_fx_setup`](entry)

    let fx = '0'
    entry._fx_req = 0

    if (entry.symbol_real === entry.symbol_v && entry.symbol_v === entry.symbol) { // NOTE: probably this could be optimized in entry.symbol_real === entry.symbol
      fx = '1'
    } else {
      entry._fx_req = 1

      const fx_pccys = [
        [pair_ccy2(entry.symbol_real.slice(1)), pair_ccy2(entry.symbol_v.slice(1))],
        [pair_ccy2(entry.symbol_v.slice(1)), pair_ccy2(entry.symbol.slice(1))]
      ]

      const chash = _.map(fx_pccys, p => p.join(':')).join(',')
      fx = this.lru_fx.get(chash)

      if (!fx) {
        fx = _.reduce(fx_pccys, (acc, pccys) => {
          if (!+acc) {
            return acc
          }

          const _fx = this.getFx(
            pccys[0], pccys[1]
          )

          if (!+_fx) {
            return '0'
          }
          return nBN(acc).times(_fx).toString()
        }, '1')

        if (+fx) {
          fx = nBN(fx).dp(8).toString()
        } else {
          console.error('Fx NOT FOUND', entry.symbol, entry.symbol_real)
          fx = '1'
        }

        this.lru_fx.set(chash, fx)
      }
    }

    // console.log('FX', entry.symbol, entry.symbol_real, fx)
    entry._fx = fx
    this.lib_spec[`${type}_fx_apply`](entry)
  }

  isEntryActive_order (entry) {
    return entry.active === 1
  }

  handleEntry (type, msg) {
    const st = this.symType

    const rateMtd = `getEntryRate_${st}`
    if (!this[rateMtd]) {
      console.error(`ERR_SYM_RATE_METHOD_NOTFOUND: ${st}`)
      return
    }

    let eid = +msg.id || null
    if (!eid) {
      return
    }

    msg.id = eid

    const _entry = this.entries.get(eid)

    const handleFx = st === 't' && type === 'order'

    if (_entry && _entry._book) {
      const rate = +this[rateMtd](_entry)
      const book = this.book[_entry._bsix]
      book.remove([rate, _entry.id])
    }

    if (_entry && handleFx) {
      this.clearEntryFx(type, _entry)
    }

    const entry = this.lib_spec[type](msg)
    if (!entry) {
      return
    }

    if (handleFx) {
      this.updateEntryFx(type, entry)
    }

    let isActive = false

    const chk0Hnd = `isEntryActive_${type}`
    if (this[chk0Hnd]) {
      isActive = this[chk0Hnd](entry)
    }

    if (isActive) {
      this.entries.set(entry.id, entry)
    }

    if (entry._book && isActive) {
      const book = this.book[entry._bsix]
      const rate = +this[rateMtd](entry)
      book.insert([rate, entry.id, entry])
    }

    if (entry.active < 1) {
      this.entries.delete(entry.id)
    }

    this.sendEntryChange(entry, isActive)
  }

  sendEntryChange (entry, isActive) {
    const st = this.symType
    const symbol = this.symbol
    const qipfx = this.conf.gw_ipfx

    entry.t = Date.now()

    this.pendingFlush = true

    this.qout_0.push([
      'core.sym-stream', 'publish', `${qipfx}.sym.${symbol}`,
      this.symMsg(['entry', entry])
    ])

    if (entry._book) {
      this.qout_0.push([
        'core.sym-book', 'publish', `${qipfx}.booka.${symbol}`,
        this.symMsg(['bu', this.prepEntryRaw(st, entry, isActive)])
      ])
    }
  }

  handleTrade (type, msg) {
    let trade = this.lib_spec.trade(msg)

    trade.vid = `${this.status.tid}-${trade.symbol.substr(1)}`

    this.lru_trades.set(`trade-${trade.id}`, trade)

    ++this.status.tid
    this.saveStatus()

    const ctrade = _.clone(trade)

    const qipfx = this.conf.gw_ipfx
    this.qout_0.push([
      'core.sym', 'publish', `${qipfx}.trades.${this.symbol}`,
      JSON.stringify(ctrade)
    ])

    this.qout_0.push([
      'core.sym-stream', 'publish', `${qipfx}.sym.${this.symbol}`,
      this.symMsg(['trade', ctrade])
    ])

    this.sendTradePriv('core_trade', ctrade)

    if (trade.symbol[0] === 'f') {
      this.handleTradeRcn('trade_rcn', msg)
    }
  }

  handleTradeRcn (type, msg) {
    let trade = this.lib_spec.trade_rcn(msg)

    let ctrade = this.lru_trades.get(`trade-${trade.id}`)
    if (!ctrade) {
      return
    }

    _.extend(ctrade, trade)
    ctrade._rec = 1

    const qipfx = this.conf.gw_ipfx

    this.qout_0.push([
      'core.sym', 'publish', `${qipfx}.trades_rcn.${this.symbol}`,
      JSON.stringify(ctrade)
    ])

    this.qout_0.push([
      'core.sym-stream', 'publish', `${qipfx}.sym.${this.symbol}`,
      this.symMsg(['trade_rcn', trade])
    ])

    this.sendTradePriv('core_trade_rcn', ctrade)
  }

  sendTradePriv (action, trade) {
    _.each(['b', 's'], pfx => {
      if (!trade[`${pfx}uid`]) {
        return
      }

      let ctrade = _.clone(trade)

      ctrade.user_id = trade[`${pfx}uid`]
      ctrade.poid = trade[`${pfx}oid`]
      ctrade.poid_c = trade[`${pfx}oid_c`]

      ctrade.u_maker = null
      if (ctrade.muid) {
        ctrade.u_maker = ctrade.moid === ctrade.poid ? 1 : -1
      }

      let mul = (pfx === 's' ? -1 : 1) * (ctrade.symbol[0] === 'f' ? -1 : 1)
      ctrade.amount = (Math.abs(+ctrade.amount) * mul).toString()

      const st = this.symType
      if (st === 't') {
        ctrade.popx = trade[`${pfx}opx`]
        ctrade.potype = trade[`${pfx}otype`]
        ctrade.pometa = trade[`${pfx}ometa`]

        if (action === 'core_trade_rcn') {
          ctrade.pfee = trade[`${pfx}fee`]
          ctrade.pccy = trade[`${pfx}ccy`]
        }
      }

      this.qout_0.push([
        'core.sym', 'publish',
        `${this.conf.gw_ipfx}.int.privr${this.getRingId(ctrade.user_id, 'priv')}`,
        JSON.stringify({ a: action, o: ctrade })
      ])
    })
  }

  handleFx (msg) {
    super.handleTicker(msg)
  }

  auditSnap () {
    this.mem.symStreamSeq = 0
    this.mem.symStreamSix++

    const qipfx = this.conf.gw_ipfx
    this.qout_0.push([
      'core.sym-stream', 'publish', `${qipfx}.sym.${this.symbol}`,
      this.symMsg(['snap_start'])
    ])

    const chunks = _.chunk(Array.from(this.entries.values()), 500)

    _.each(chunks).forEach(entries => {
      this.qout_0.push([
        'core.sym-stream', 'publish', `${qipfx}.sym.${this.symbol}`,
        this.symMsg(['snap', entries])
      ])
    })

    this.qout_0.push([
      'core.sym-stream', 'publish', `${qipfx}.sym.${this.symbol}`,
      this.symMsg(['snap_end'])
    ])
  }

  getEntryRawData_t (o) {
    return [null, o.price, 0]
  }

  prepEntryRaw (st, o, isActive) {
    let [akey, price, type] = this[`getEntryRawData_${st}`](o)

    return [o.id, price, akey, o.amount, type, o._bsix, isActive]
  }

  braw () {
    const raw = []
    const st = this.symType

    for (let ix = 0; ix < this.book.length; ix++) {
      const book = this.book[ix]

      let [it, _o] = [book.iterator(), null]

      while ((_o = it.next()) !== null) {
        raw.push(this.prepEntryRaw(st, _o[2], 1))
      }
    }

    return raw
  }

  dump () {
    fs.writeFile(`${this.conf.dir_log}/entries-active-${this.symbol}.log`, JSON.stringify(
      Array.from(this.entries.values())
    ), () => {})
    fs.writeFile(`${this.conf.dir_log}/book-${this.symbol}.log`, JSON.stringify(this.braw()), () => {})
  }

  oJobHandle (job) {
    let part = null

    switch (job[0]) {
      case 'core.sym':
        part = {}
        part[`gw${this.busIx}`] = [[job[1], job[2], job[3]]]
        break
      case 'core.sym-book':
        part = {
          bm: [[job[1], job[2], job[3]]]
        }
        break
      case 'core.sym-stream':
        part = {
          bm: [[job[1], job[2], job[3]]]
        }
        break
    }

    return part
  }

  fixStatus () {
    _.each(['tid'], fld => {
      this.status[fld] = +this.status[fld] || 0
    })
  }

  symMsg (msg) {
    msg.push(this.mem.symBookSix, this.mem.symBookSeq++)
    return JSON.stringify(msg)
  }

  pubBook () {
    const now = Date.now()
    const diff = now - this._lastBrawTs

    if (diff < this.conf.bookFreq) {
      return
    }

    if (!this.pendingFlush && diff <= 5000) {
      return
    }

    this.pendingFlush = false
    this._lastBrawTs = now

    const payload = this.braw()

    this.mem.symBookSeq = 0
    this.mem.symBookSix++

    this.qout_0.push([
      'core.sym-book', 'publish', `${this.conf.gw_ipfx}.booka.${this.symbol}`,
      this.symMsg(['bsb'])
    ])

    _.each(_.chunk(payload, 250), chunk => {
      this.qout_0.push([
        'core.sym-book', 'publish', `${this.conf.gw_ipfx}.booka.${this.symbol}`,
        this.symMsg(['bs', chunk])
      ])
    })

    this.qout_0.push([
      'core.sym-book', 'publish', `${this.conf.gw_ipfx}.booka.${this.symbol}`,
      this.symMsg(['bse'])
    ])
  }

  subSymShared () {
    if (this.mem._isSubShared) {
      return
    }

    const gconf = this.mem.gconf

    if (!gconf) {
      return
    }

    this.mem._isSubShared = true

    const ccys_fiat = gconf.ccys_fiat || []
    const needSymbolV = ccys_fiat.indexOf(pair_ccy2(this.pair)) > -1
    let symbolV = null

    if (needSymbolV) {
      symbolV = `t${pair_join(pair_ccy1(this.pair), 'USD')}`
    }

    if (!symbolV || symbolV === this.symbol) {
      return
    }

    console.log('SUBSCRIBE_SYM_SHARED', symbolV)
    this.symbolV = symbolV

    const qipfx = this.conf.gw_ipfx

    for (let i = 0; i < this.conf.shards.bus; i++) {
      const gw_sub = `redis_gw${i}`
      this[gw_sub].cli_sub.subscribe(`${qipfx}.int.${symbolV}-shared`)
    }
  }

  unsubSymShared () {
    if (!this.mem._isSubShared || !this.symbolV) {
      return
    }

    const qipfx = this.conf.gw_ipfx

    for (let i = 0; i < this.conf.shards.bus; i++) {
      const gw_sub = `redis_gw${i}`
      this[gw_sub].cli_sub.unsubscribe(`${qipfx}.int.${this.symbolV}-shared`)
    }
  }

  _start (cb) {
    this.qout_0.opts.oWorkPing = 5

    this.busIx = this.getStrRingId(this.symbol, 'bus')

    this._lastBrawTs = Date.now()
    this.pendingFlush = true

    async.series([
      next => { super._start(next) },
      next => {
        this.refreshConf(next)
      },
      next => {
        this.fixStatus()
        next()
      },
      next => {
        const qipfx = this.conf.gw_ipfx
        this.redis_gw0.cli_sub.subscribe(`${qipfx}.int.global`)
        this.redis_gw0.cli_sub.subscribe(`${qipfx}.fx`)

        for (let i = 0; i < this.conf.shards.bus; i++) {
          const gw_sub = `redis_gw${i}`
          this[gw_sub].cli_sub.subscribe(`${qipfx}.int.${this.symbol}`)
          this[gw_sub].cli_sub.subscribe(`${qipfx}.int.${this.symbol}-shared`)
        }

        next()
      },
      next => {
        if (this.conf.debug) {
          this.interval_0.add('dump', () => {
            this.dump()
          }, 30000)
        }

        this.interval_0.add('refresh-conf', () => {
          this.refreshConf()
        }, 60000)

        this.interval_0.add('sub-shared', () => {
          this.subSymShared()
        }, 2500)

        this.interval_0.add('book-raw', () => {
          this.pubBook()
        }, 100)

        if (this.symbol === 'tBTCUSD') {
          this.interval_0.add('ping', () => {
            this.qout_0.push([
              'core.sym', 'publish', `${this.conf.gw_ipfx}.info`,
              JSON.stringify({
                a: 'ping',
                wtype: this.wtype
              })
            ])
          }, 5000)
        }

        next()
      },
      next => {
        this.scheduler_0.add('trig.book-snap', () => {
          this.auditSnap()
        }, '0 0 0 * * *')
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
        this.redis_gw0.cli_sub.unsubscribe(`${qipfx}.fx`)

        for (let i = 0; i < this.conf.shards.bus; i++) {
          const gw_sub = `redis_gw${i}`
          this[gw_sub].cli_sub.unsubscribe(`${qipfx}.int.${this.symbol}`)
          this[gw_sub].cli_sub.unsubscribe(`${qipfx}.int.${this.symbol}-shared`)
        }

        this.unsubSymShared()

        next()
      }
    ], cb)
  }
}

module.exports = CoreSym
