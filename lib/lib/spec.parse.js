'use strict'

const dfns = require('date-fns')

const _ = require('lodash')
const libUtilNumber = require('@bitfinex/lib-js-util-number')

const nBN = libUtilNumber.nBN

const FX_FIELDS = ['price', 'price_avg', 'price_trailing', 'price_aux_limit']
const FX_FIELDS_CLEAR = ['symbol'].concat(FX_FIELDS)

function dfmt (_d) {
  let d = null

  if (_d instanceof Date) {
    d = _d
  } else if (_.isFinite(_d)) {
    d = new Date(Math.floor(_d))
  } else {
    d = new Date(Date.parse(_d))
  }

  return dfns.formatRFC3339(d)
}

function parsePos (msg, _pos) {
  const pos = _pos ? _.extend(_pos, msg) : msg

  pos.symbol = `t${pos.pair}`
  pos.active = 1

  if (pos.status !== 'ACTIVE' || !(+pos.amount)) {
    pos.status = 'CLOSED'
    pos.active = 0
  }

  pos._tm = msg._tm

  const now = Date.now()
  pos.t = now

  return pos
}

function setupOrderFx (o) {
  o.symbol_real = o.symbol
  o.symbol = `t${(o.pair_fx || o.pair)}`

  _.each(FX_FIELDS, fld => {
    o[fld + '_real'] = o[fld]
  })
}

function applyOrderFx (o) {
  _.each(FX_FIELDS, fld => {
    const fld_real = fld + '_real'

    if (o[fld_real] === undefined || o[fld_real] === null) {
      return
    }

    if (o._fx === '1') {
      o[fld] = o[fld_real]
    } else {
      o[fld] = nBN(o[fld_real]).times(nBN(o._fx)).toString()
    }
  })
}

function clearOrderFx (o) {
  _.each(FX_FIELDS_CLEAR, fld => {
    const fld_real = fld + '_real'
    o[fld] = o[fld_real]
    delete o[fld_real]
  })
}

function parseOrder (msg, _order) {
  const order = _order ? _.extend(_order, msg) : msg

  order.id = +order.id
  order['$type'] = 'order'
  order.symbol = `t${msg.pair}`
  order.symbol_v = `t${msg.v_pair}`

  order._book = 0
  order._bsix = +order.amount_orig >= 0 ? 0 : 1
  if ((order.type === 'EXCHANGE LIMIT' || order.type === 'LIMIT') && !order.hidden) {
    order._book = 1
  }

  const now = Date.now()
  order._mts = Math.floor(now / 1000)

  order.created_at = dfmt(order.created_at)
  order.updated_at = dfmt(new Date())

  order.t = now

  return order
}

function parseTrade (msg) {
  const mul = +msg.side ? -1 : 1

  const trade = {
    id: +msg.id || +msg.trade_id || null,
    pair: msg.pair,
    symbol: `t${msg.pair}`,
    price: msg.price + '',
    amount: (+msg.amount * mul) + '',
    boid: +msg.boid,
    soid: +msg.soid,
    boid_c: +msg.boid_c,
    soid_c: +msg.soid_c,
    moid: +msg.moid,
    muid: +msg.muid,
    buid: +msg.buid,
    suid: +msg.suid,
    bopx: +msg.bopx,
    sopx: +msg.sopx,
    botype: msg.botype,
    sotype: msg.sotype,
    section: 'trading'
  }

  if (msg.created_at) {
    trade.created_at = msg.created_at
  }

  if (msg.t) {
    trade.created_at = dfmt(msg.t * 1000)
  }

  trade.t = Date.parse(trade.created_at)
  trade._tm = msg._tm

  // console.log('trade', trade)

  return trade
}

function parseTradeRcn (msg) {
  const mul = +msg.side ? -1 : 1

  const trade = {
    id: +msg.trade_id,
    pair: msg.pair,
    symbol: `t${msg.pair}`,
    price: msg.price + '',
    amount: (+msg.amount * mul) + '',
    boid: +msg.buyer_order_id,
    soid: +msg.seller_order_id,
    moid: +msg.maker_order_id,
    buid: +msg.buyer_id,
    suid: +msg.seller_id,
    muid: +msg.maker_id,
    bfee: (+msg.buyer_fee) + '',
    sfee: (+msg.seller_fee) + '',
    bccy: msg.buyer_currency,
    sccy: msg.seller_currency,
    created_at: msg.created_at || dfmt(+msg.timestamp * 1000),
    section: 'trading'
  }

  trade.t = Date.parse(trade.created_at)
  trade._tm = msg._tm

  // console.log('trade_rcn', trade, msg)

  return trade
}

function parseTicker (msg) {
  const ticker = {
    pair: msg.pair,
    symbol: 't' + msg.pair,
    bid: msg.buying + '',
    ask: msg.selling + ''
  }

  if (!msg._fx_only) {
    _.extend(ticker, {
      bid_size: msg.buying_size + '',
      ask_size: msg.selling_size + '',
      change1d: +msg.daily_change + '',
      perf1d: msg.daily_change_perc + '',
      px_last: msg.last + '',
      volume: msg.volume_p + '',
      high: msg.high + '',
      low: msg.low + '',
      _tm: msg._tm
    })
  }

  return ticker
}

module.exports = {
  dfmt: dfmt,

  order: parseOrder,
  order_fx_setup: setupOrderFx,
  order_fx_apply: applyOrderFx,
  order_fx_clear: clearOrderFx,

  pos: parsePos,
  trade: parseTrade,
  trade_rcn: parseTradeRcn,

  ticker: parseTicker
}
