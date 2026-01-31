import yfinance as yf
import pandas as pd
from datetime import datetime


def fetch_fx_data(pair="EURUSD=X", start="2019-01-01", end=None):
    """
    Fetch historical FX price data using yfinance.
    """
    if end is None:
        end = datetime.today().strftime("%Y-%m-%d")

    data = yf.download(pair, start=start, end=end, interval="1d")

    if data.empty:
        raise ValueError("No data fetched. Check symbol or date range.")

    data.reset_index(inplace=True)
    return data


if __name__ == "__main__":
    df = fetch_fx_data()
    print(df.head())

    # Save raw data
    df.to_csv("data/eurusd_daily.csv", index=False)
    print("Data saved to data/eurusd_daily.csv")
