CREATE KEYSPACE IF NOT EXISTS krakentrades WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};

CREATE KEYSPACE IF NOT EXISTS krakenstats WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE krakentrades.assetpairs (
    asset_id uuid,
    altname text,
    base text,
    aclass_base text,
    lot text,
    cost_decimals int,
    leverage_buy text,
    leverage_sell text,
    fees text,
    ordermin decimal,
    tick_size decimal,
    long_position_limit int,
    short_position_limit int
);
 
CREATE TABLE krakentrades.market_depth (
    depth_id uuid,
    price decimal,
    ask_volume decimal, 
    ask_time time,
    bid_price decimal,
    order decimal,
    bid_time time
)

CREATE TABLE krakentrades.ohlc_sticks (
    stick_id uuid,
    ask_ts time,
    open decimal,
    high decimal,
    low decimal,
    vwap decimal,
    volume decimal,
    trades int
)

CREATE TABLE krakentrades.recent_trades (
    recent_trade_id uuid,
    price decimal,
    ask_volume decimal,
    trades_ts time,
    side text,
    order text,
    misc text,
    trade_id int
)

CREATE TABLE krakentrades.bin_candles (
    candle_id uuid,
    binance_ts time,
    open_price decimal,
    high decimal,
    low decimal,
    volume decimal,
    close_time time,
    quote_volume decimal,
    quote_volume decimal,
    trades int,
    taker_base decimal,
    trade_quote decimal,
    ignore text
)

CREATE TABLE krakenstats.binance_stats(
    stat_id uuid,
    timestamp time,
    avg_price decimal,
    price_deviation decimal,
    avg_volume decimal,
    max_trades int,
    highest_volume decimal,
    max_price decimal
)