import requests
from datetime import datetime
import pandas as pd
import logging as logging

logging.basicConfig(
    filename="app.log", 
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("The program started")

# Our commodities
COMMODITIES = {
    "brent_oil": "BZ=F",
    "natural_gas": "NG=F",
    "gold": "GC=F",
    "silver": "SI=F",
    "all_comodoties_index":"GSG",
    "dollar index":"DX-Y.NYB"
}

# headers for user-agent to be able to scrape data 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def explore_commodity(ticker: str, interval: str = "1m", d_range: str = "3d"):
    """
    Fetch data from Yahoo Finance API for a given ticker.
    Returns timestamp, OHLCV data
    """
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={d_range}'
    response = requests.get(url, headers = HEADERS)
    
    if response.status_code == 200: 
        data = response.json()
        try:
            result = data['chart']['result'][0]
            timestamp = result['timestamp']
            quotes = result['indicators']['quote'][0]
            
            # Extract all OHLCV data
            opens = quotes['open']
            highs = quotes['high']
            lows = quotes['low']
            closes = quotes['close']
            volumes = quotes['volume']
            
            logging.info(f"Data loaded successfully for {ticker}")
            return timestamp, opens, highs, lows, closes, volumes
            
        except Exception as e:
            logging.exception(f"Error processing {ticker}: {e}")
            return None 
    else:
        logging.error(f"Error fetching data for {ticker}: {response.status_code}")
        return None 
        
# loop over the commodities
if __name__ == "__main__":
    all_data = []     
    for name, ticker in COMMODITIES.items():
        result = explore_commodity(ticker, "1m", "7d")
        
        if result is None:
            print(f"❌ Skipping {name} - failed to fetch")
            continue
            
        timestamp, opens, highs, lows, closes, volumes = result
        
        for ts, o, h, l, c, v in zip(timestamp, opens, highs, lows, closes, volumes):
            new_timestamp = datetime.fromtimestamp(ts)
            o = round(o, 4) if o is not None else None
            h = round(h, 4) if h is not None else None
            l = round(l, 4) if l is not None else None
            c = round(c, 4) if c is not None else None
            
            all_data.append({
                "commodity": name,
                "ticker": ticker,
                "timestamp": new_timestamp,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v
            })

    df = pd.DataFrame(all_data)
    df.to_csv("sample_prices.csv", index=False)    
    logging.info("The program finished")