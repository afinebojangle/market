COPY (
--long call
(SELECT oh.ticker as ticker
,   capmc.alpha as alpha
,   capmc.beta as beta
,   'Long Call' as trade_type 
,   oh.years_to_expiration as years_to_expiration
,   ee.error/oh.stock_price as pct_capm_error
,   ev.ma_50/oh.stock_price as pct_ma_50_volatility
,   oh.stock_price as stock_price
,   (oh.strike-oh.stock_price)/oh.stock_price as pct_strike_variance
,   oh.call_ask_price as option_price
,   oh.call_ask_implied_volitility/oh.call_ask_price as pct_implied_volatility
,   oh.delta as delta
,   oh.gamma as gamma
,   oh.theta as theta
,   oh.vega as vega
,   oh.rho as rho
,   oh.phi as phi
--,   ofp.max_forward_call_bid_price as max_forward_price
--,   ofv.forward_call_bid_volatility as forward_volatility
,   CONCAT(CASE WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price)  <= -.45 THEN '-5'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > -.45 AND ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) <= -.3 THEN '-4'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > -.3 AND ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) <= -.15 THEN '-3'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > -.15 AND ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) <= -.05 THEN '-2'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > -.05 AND ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) <= 0 THEN '-1'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > 0 AND ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) <= .05 THEN '1'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > .05 AND ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) <= .15 THEN '2'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > .15 AND ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) <= .35 THEN '3'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > .35 AND ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) <= .45 THEN '4'
        WHEN ((ofp.max_forward_call_bid_price - oh.call_ask_price) / oh.call_ask_price) > .45 THEN '5'
    END
,   CASE WHEN (forward_call_bid_volatility / oh.call_bid_price) < .1 THEN '1'
         WHEN (forward_call_bid_volatility / oh.call_bid_price) >= .1 AND (forward_call_bid_volatility / oh.call_bid_price) < .2 THEN '2'
         WHEN (forward_call_bid_volatility / oh.call_bid_price) >= .2 AND (forward_call_bid_volatility / oh.call_bid_price) < .3 THEN '3'
         WHEN (forward_call_bid_volatility / oh.call_bid_price) >= .3 AND (forward_call_bid_volatility / oh.call_bid_price) < .4 THEN '4'
         WHEN (forward_call_bid_volatility / oh.call_bid_price) >= .4 AND (forward_call_bid_volatility / oh.call_bid_price) < .5 THEN '5'
         WHEN (forward_call_bid_volatility / oh.call_bid_price) >= .5 AND (forward_call_bid_volatility / oh.call_bid_price) < .6 THEN '6'
         WHEN (forward_call_bid_volatility / oh.call_bid_price) >= .6 THEN '7'
    END) as label
FROM option_historical as oh
    JOIN capm_coefficients as capmc on oh.ticker = capmc.ticker
    JOIN equity_errors as ee on oh.ticker = ee.ticker and oh.trade_date = ee.date
    JOIN equity_volatilities as ev on oh.ticker = ev.ticker and oh.trade_date = ev.date
    JOIN option_forward_prices as ofp on oh.ticker = ofp.ticker and oh.trade_date = ofp.trade_date and oh.experiation_date = ofp.experiation_date and oh.strike = ofp.strike
    JOIN option_forward_volatilities as ofv on oh.ticker = ofv.ticker and oh.trade_date = ofv.trade_date and oh.experiation_date = ofv.experiation_date and oh.strike = ofv.strike
WHERE oh.stock_price <> 0 and oh.call_ask_price <> 0 and oh.call_ask_price <> 0 and oh.call_bid_price <> 0
    and oh.trade_date < to_date('20161201', 'YYYYMMDD'))
    )
TO '/home/rayford/storage/training_data.csv' WITH CSV HEADER DELIMITER ','
