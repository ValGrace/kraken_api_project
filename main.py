from scripts.ohlc_consumer import ohlc_consumer
from scripts.trades_consumer import rt_consumer
from scripts.market_depth_consumer import depth_consumer
from scripts.asset_consumer import ap_consumer
from scripts.bin_candles_consumer import bin_consumer
from scripts.spark_consumer import create_cluster

def main():
    ohlc_consumer()
    rt_consumer()
    depth_consumer()
    ap_consumer()
    bin_consumer()
    create_cluster()

if __name__ == "__main__":
    main()