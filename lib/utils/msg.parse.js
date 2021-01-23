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
          queueOrder(ctx, o)
        )
      })
    })
  }

  return buf
}

const queueOrder = (ctx, md) => {
  if (!ctx.conf.pairs) {
    console.error('GLOBAL_CONF NOT FOUND')
    return []
  }

  const buf = []

  const md_s = JSON.stringify({ a: 'te_update_order_mem', o: md })

  const pairV = md.v_pair || md.pair
  buf.push([
    ctx.getStrRingId(`t${pairV}-shared`, 'bus'),
    md_s,
    `int.t${pairV}-shared`, { encoded: true }
  ])

  return buf
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
  queueOrder: queueOrder,
  queuePubTrade: queuePubTrade
}
