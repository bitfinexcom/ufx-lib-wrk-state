'use strict'

const _ = require('lodash')
const libUtilSymbol = require('@bitfinex/lib-js-util-symbol')

const pair_join = libUtilSymbol.pair_join
const pair_split = libUtilSymbol.pair_split

const queueSyncEngine = (ctx, msg, md) => {
  const buf = []

  if (md.orders) {
    let entries = md.orders

    _.each(entries, (sbook, pair) => {
      _.each(sbook, o => {
        buf.push.apply(
          buf,
          queuePubOrder(ctx, o)
        )

        const prix = ctx.getRingId(o.user_id, 'priv')

        buf.push([
          ctx.getRingId(prix, 'bus'),
          { a: 'te_update_order_mem', o: o },
          `int.privr${prix}`
        ])
      })
    })
  }

  if (md.u_confs) {
    let entries = md.u_confs

    _.each(entries, c => {
      const prix = ctx.getRingId(c.user_id, 'priv')

      buf.push(
        [
          ctx.getRingId(prix, 'bus'),
          { a: 'te_update_user_conf_mem', o: c },
          `int.privr${prix}`
        ]
      )
    })
  }

  _.each(['u_positions', 'u_wallets'], k => {
    let entries = md[k]
    if (!entries) {
      return
    }

    let en = null
    switch (k) {
      case 'u_positions':
        en = 'te_update_position_mem'
        break
      case 'u_wallets':
        en = 'te_update_balance_mem'
        break
    }

    if (!en) {
      return
    }

    _.each(entries, rows => {
      _.each(rows, e => {
        const prix = ctx.getRingId(e.user_id, 'priv')

        buf.push([
          ctx.getRingId(prix, 'bus'),
          { a: en, o: e },
          `int.privr${prix}`
        ])
      })
    })
  })

  return buf
}

const queuePubOrder = (ctx, md) => {
  if (!ctx.conf.pairs) {
    console.error('GLOBAL_CONF NOT FOUND')
    return []
  }

  const buf = []

  const pairO = md.pair
  const ccysPair = pair_split(pairO)

  const chash = `map:${pairO}`
  let pairs = ctx.lru_0.get(chash)

  if (!pairs) {
    const relay = ctx.conf.ccys_fiat

    let pairs_b = [pairO]
    if (relay.indexOf(ccysPair[1]) > -1) {
      _.each(relay, c => pairs_b.push(pair_join(ccysPair[0], c)))
    }

    pairs_b = _.uniq(pairs_b)

    pairs = []

    _.each(pairs_b, r => {
      if (ctx.conf.pairs.indexOf(r) === -1) {
        return
      }

      pairs.push(r)
    })

    ctx.lru_0.set(chash, pairs)
  }

  let ro = _.clone(md)
  const qipfx = ctx.conf.gw_ipfx

  for (let i = 0; i < pairs.length; i++) {
    const r = pairs[i]
    ro.pair_fx = r

    buf.push([
      ctx.getStrRingId(`t${r}`, 'bus'),
      JSON.stringify({ a: 'te_update_order_mem', o: ro }),
      `int.t${r}`, { encoded: true }
    ])
  }

  return buf
}

const PRIV_UPD_SKIP_REASONS = {
  1048576: true
}

const queuePrivMsg = (ctx, type, md) => {
  if (md._trg) {
    if (PRIV_UPD_SKIP_REASONS[md._trg]) {
      return []
    }
  }

  const prix = this.getRingId(md.user_id, 'priv')

  buf.push([
    this.getRingId(prix, 'bus'),
    msg,
    `int.privr${prix}`
  ])

}

const queuePubTrade = (ctx, md) => {
  return [[
    ctx.getStrRingId(`t${md.pair}`, 'bus'),
    { a: 'te_trade_mem', o: md },
    `int.t${md.pair}`
  ]]
}


module.exports = {
  queueSyncEngine: queueSyncEngine,
  queuePubOrder: queuePubOrder,
  queuePubTrade: queuePubTrade
}
