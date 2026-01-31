import pandas as pd
import numpy as np


class TechnicalAnalysisAgent:
    def __init__(self, data_path):
        self.data = pd.read_csv(data_path)
        self._prepare_data()

    def _prepare_data(self):
        # Date handling
        self.data["Date"] = pd.to_datetime(self.data["Date"])
        self.data.sort_values("Date", inplace=True)

        # Ensure numeric prices (fix yfinance CSV issues)
        self.data["Close"] = pd.to_numeric(self.data["Close"], errors="coerce")

        # Drop rows with invalid prices
        self.data.dropna(subset=["Close"], inplace=True)

        # Technical indicators
        self.data["SMA_20"] = self.data["Close"].rolling(window=20).mean()
        self.data["SMA_50"] = self.data["Close"].rolling(window=50).mean()

        # RSI calculation
        delta = self.data["Close"].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()

        rs = gain / loss
        self.data["RSI"] = 100 - (100 / (1 + rs))

    def generate_signal(self):
        latest = self.data.iloc[-1]

        sma20 = latest["SMA_20"]
        sma50 = latest["SMA_50"]
        rsi = latest["RSI"]

        signal = "neutral"
        reasons = []

        if sma20 > sma50:
            reasons.append("Short-term trend is bullish (SMA20 > SMA50)")
        else:
            reasons.append("Short-term trend is bearish (SMA20 < SMA50)")

        if rsi > 50:
            reasons.append("Momentum is positive (RSI > 50)")
        else:
            reasons.append("Momentum is negative (RSI < 50)")

        if sma20 > sma50 and rsi > 50:
            signal = "bullish"
        elif sma20 < sma50 and rsi < 50:
            signal = "bearish"

        confidence = min(abs(rsi - 50) / 50, 1.0)

        return {
            "agent": "technical",
            "signal": signal,
            "confidence": round(confidence, 2),
            "explanation": "; ".join(reasons)
        }


if __name__ == "__main__":
    agent = TechnicalAnalysisAgent("data/eurusd_daily.csv")
    output = agent.generate_signal()

    print("Technical Agent Output:")
    for k, v in output.items():
        print(f"{k}: {v}")
