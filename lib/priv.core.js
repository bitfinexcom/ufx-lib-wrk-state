'use strict'

const _ = require('lodash')
const fs = require('fs')
const async = require('async')
const specLib = require('ufx-lib-hive-out-spec')
const CoreBase = require('./base.core')
const locComLib = require('./utils/common.js')

class CorePriv extends CoreBase {
  constructor (conf, ctx) {
    super(conf, ctx)

    this.ring = ctx.ring
    this.prefix += '-' + this.ring

    console.log('WORKER', this.wtype, this.ring)
  }

  init () {
    super.init()

    this.lib_spec = specLib

    _.extend(this.mem, {
      users: new Map()
    })

    this.conf.liveFields = [
      'id', 'conf', 'status',
      'wallets', 'positions', 'orders'
    ]
  }

  handleRedisStream (channel, msg, src) {
    const chp = channel.split('.')

    if (chp[2] === 'global') {
      this.handleIntGlobal(msg)
    } else if (chp[1] === 'priv_snap_req') {
      this.handlePriv({ a: 'snap_user', o: msg })
    } else {
      this.handlePriv(msg)
    }
  }

  handleIntGlobal (msg) {
    super.handleIntGlobal(msg)

    switch (msg.a) {
      case 'te_sync_start':
        this.handleTeStartSync()
        break
    }
  }

  handlePriv (msg) {
    this.setMsgTime(msg)

    switch (msg.a) {
      case 'te_update_balance_mem':
        this.handleUser('balance', msg.o, {
          seq_g: msg.seq
        })
        break

      case 'te_update_position_mem':
        this.handleUser('position', msg.o, {
          seq_g: msg.seq
        })
        break

      case 'te_update_order_mem':
        this.handleUser('order', msg.o, {
          seq_g: msg.seq
        })
        break

      case 'te_trade_priv_mem':
        this.handleUser('trade', msg.o, {
          need_parse: true,
          seq_g: msg.seq
        })
        break

      case 'core_trade':
        this.handleUser('trade', msg.o, {
          seq_g: msg.seq
        })
        break

      case 'core_trade_rcn':
        this.handleUser('trade_rcn', msg.o, {
          seq_g: msg.seq
        })
        break

      case 'te_update_user_conf_mem':
      case 'update_user_conf':
        // console.log(msg.o)
        this.handleUser('conf', msg.o, {
          seq_g: msg.seq
        })
        break

      case 'notify':
      case 'auth_token':
        this.handleUser(msg.a, msg.o, {
          seq_g: msg.seq
        })
        break

      case 'snap_user':
        this.handleUserSnap(msg.o)
        break
    }
  }

  handleTeStartSync () {
    this.mem.users.forEach(user => {
      user.positions = {}
      user.wallets = {}
      user.orders = {}
    })

    console.log('PRIV_TE_SYNC', this.ring)
  }

  handleUserPartReset (t, proc = null) {
    this.mem.users.forEach(user => {
      if (proc) {
        _.each(user[t], (v, k) => {
          if (proc(k, v)) {
            delete user[t][k]
          }
        })
      } else {
        user[t] = {}
      }
    })
  }

  getUserInitExt () {
    return {}
  }

  getUser (uid) {
    let user = this.mem.users.get(uid) || null
    if (user) {
      return user
    }

    user = {
      id: uid,
      conf: {},
      status: {},
      wallets: {},
      positions: {},
      orders: {},
      seq: 0,
      seq_g: 0
    }

    _.extend(user, this.getUserInitExt())

    this.mem.users.set(uid, user)

    return user
  }

  handleUser (type, msg, opts = {}) {
    const uid = +msg.user_id || null

    if (!uid) {
      return
    }

    const user = this.getUser(uid)

    let subject = null

    user.seq_g = opts.seq_g || user.seq_g

    switch (type) {
      case 'conf':
        _.extend(user.conf, msg)
        subject = [JSON.stringify([
          user.id, 'conf', user.conf,
          ++user.seq, user.seq_g
        ])]
        break
      case 'auth_token':
        subject = [JSON.stringify([
          user.id, 'auth_token', msg,
          user.seq, user.seq_g
        ])]
        break
      case 'notify':
        subject = [JSON.stringify([
          user.id, 'notify', msg,
          user.seq, user.seq_g
        ]), {
          useSpecChannel: !!msg.important
        }]
        break
      case 'order':
        subject = this.handleEntryBase(user, type, msg, opts)
        break
      case 'balance':
        subject = this.handleBalance(user, type, msg, opts)
        break
      case 'position':
        subject = this.handlePosition(user, type, msg, opts)
        break
      case 'trade':
        subject = this.handleTrade(user, type, msg, opts)
        break
      case 'trade_rcn':
        subject = this.handleTradeRcn(user, type, msg, opts)
        break
    }

    if (!subject) {
      const hndMtd = `handleUserExt_${type}`
      if (!this[hndMtd]) {
        return
      }

      subject = this[hndMtd](user, msg, opts)
    }

    if (subject) {
      this.qout_0.push([
        'core.priv',
        { a: 'pub_user', user_id: user.id, data: subject }
      ])
    }
  }

  handleUserSnap (opts) {
    const uid = +opts.user_id
    if (!uid) {
      return
    }

    this.sendUserSnap(uid, {
      snapType: opts.snapType
    })
  }

  sendUserSnap (uid, opts) {
    const user = this.getUser(uid)

    const snapType = opts.snapType || 'full'
    const udata = snapType === 'live'
      ? _.pick(user, this.conf.liveFields) : user

    this.qout_0.push(
      [
        'core.priv', {
          a: 'pub_snap',
          user_id: user.id,
          data: JSON.stringify([
            user.id, 'user', udata,
            udata.seq, udata.seq_g
          ])
        },
        this.getRingId(user.id, 'bus')
      ]
    )
  }

  getEntryGroupTrading (msg) {
    return `t${msg.pair}`
  }

  getEntryGroup_order (msg) {
    return this.getEntryGroupTrading(msg)
  }

  handleEntryBase (user, type, msg, opts) {
    const parser = this.lib_spec[type]
    if (!parser) {
      return
    }

    const groupMtd = `getEntryGroup_${type}`
    if (!this[groupMtd]) {
      return
    }

    let eid = +msg.id || null
    if (!eid) {
      return
    }

    msg.id = eid

    let group = this[groupMtd](msg)

    const pType = `${type}s`

    if (!user[pType][group]) {
      user[pType][group] = {}
    }

    const live = user[pType][group]
    const isExisting = !!live[eid]

    const entry = parser(msg)

    if (!entry) {
      return
    }

    const chk0Mtd = `skipEntry_${type}`
    if (this[chk0Mtd] && this[chk0Mtd](entry.active)) {
      return
    }

    entry._new = false

    if (entry.active === 1) {
      entry._new = !isExisting
      live[entry.id] = entry
    } else {
      if (isExisting) {
        delete live[entry.id]

        if (locComLib.isObjetEmpty(live)) {
          delete user[pType][group]
        }
      }
    }

    return [JSON.stringify([
      user.id, type, entry,
      ++user.seq, user.seq_g
    ])]
  }

  handleTrade (user, type, msg, opts) {
    let trade = msg

    if (opts.need_parse) {
      trade = this.lib_spec.trade(msg, true)
    }

    return [JSON.stringify([
      user.id, 'trade', trade,
      ++user.seq, user.seq_g
    ])]
  }

  handleTradeRcn (user, type, msg, opts) {
    return [JSON.stringify([
      user.id, 'trade_rcn', msg,
      ++user.seq, user.seq_g
    ])]
  }

  handleBalance (user, type, msg, opts) {
    const parser = this.lib_spec.balance
    const wlt = parser(msg)

    const wk = `${wlt.wallettype}-${wlt.currency}`

    user.wallets[wk] = wlt

    if (!+wlt.balance) {
      delete user.wallets[wk]
    }

    return [JSON.stringify([
      user.id, 'wallet', wlt,
      ++user.seq, user.seq_g
    ])]
  }

  handlePosition (user, type, msg, opts) {
    const pk = `t${msg.pair}`

    const _pos = user.positions[pk]
    const isExisting = !!_pos

    const pos = this.lib_spec.pos(msg)
    user.positions[pk] = pos

    pos.user_id = user.id
    pos._new = false

    if (!pos.active) {
      delete user.positions[pk]
    } else {
      pos._new = !isExisting
    }

    return [JSON.stringify([
      user.id, 'position', pos,
      ++user.seq, user.seq_g
    ])]
  }

  getPubSnap (user, data) {
    const ret = {}

    ret[`gw${this.busIx}`] = [
      ['publish', `${this.conf.gw_ipfx}.priv.${this.ring}`, data]
    ]

    return ret
  }

  getPubUser (user, entry) {
    const rpl = {}

    const bus = rpl[`gw${this.busIx}`] = []

    const msg = entry[0]
    const opts = entry[1] || {}

    bus.push(
      ['publish', `${this.conf.gw_ipfx}.priv.${this.ring}`, msg]
    )

    if (opts.useSpecChannel) {
      bus.push(
        ['publish', `${this.conf.gw_ipfx}.priv_${type}.${this.ring}`, msg]
      )
    }

    return rpl
  }

  dump () {
    const mem = this.mem

    fs.writeFile(`${this.conf.dir_log}/priv-${this.ring}.log`, JSON.stringify(
      Array.from(mem.users.values())
    ), () => {})
  }

  oJobHandle (job) {
    let part = null

    if (job[0] === 'core.priv') {
      const jpl = job[1]
      const user = this.getUser(jpl.user_id)

      switch (jpl.a) {
        case 'pub_user':
          part = this.getPubUser(user, jpl.data)
          break
        case 'pub_snap':
          part = this.getPubSnap(user, jpl.data)
          break
      }
    }

    return part
  }

  _start (cb) {
    this.busIx = this.getRingId(+this.ring.substr(1), 'bus')

    async.series([
      next => { super._start(next) },
      next => {
        const qipfx = this.conf.gw_ipfx
        this.redis_gw0.cli_sub.subscribe(`${qipfx}.int.global`)

        for (let i = 0; i < this.conf.shards.bus; i++) {
          const gw_sub = `redis_gw${i}`
          this[gw_sub].cli_sub.subscribe(`${qipfx}.int.priv${this.ring}`)
          this[gw_sub].cli_sub.subscribe(`${qipfx}.priv_snap_req.${this.ring}`)
        }

        if (this.ring === 'r0') {
          this.interval_0.add('ping', () => {
            this.redis_gw0.cli_rw.publish(`${qipfx}.info`, JSON.stringify({ a: 'ping', wtype: this.wtype }))
          }, 5000)
        }

        if (this.conf.debug) {
          this.interval_0.add('dump', () => {
            this.dump()
          }, 30000)
        }

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

        for (let i = 0; i < this.conf.shards.bus; i++) {
          const gw_sub = `redis_gw${i}`
          this[gw_sub].cli_sub.unsubscribe(`${qipfx}.int.priv${this.ring}`)
          this[gw_sub].cli_sub.unsubscribe(`${qipfx}.priv_snap_req.${this.ring}`)
        }
        next()
      }
    ], cb)
  }
}

module.exports = CorePriv
