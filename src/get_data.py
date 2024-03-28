import pandas as pd
import yfinance as yf
from omegaconf import OmegaConf

# Parsing Configuration
config = OmegaConf.load("./params.yaml")
tickers = list(config.tickers)
savepath = config.volumes.ingested_folder
savename = config.volumes.ingested_dataset

data = yf.download(tickers, group_by="ticker", start="2023-01-01")

stacked = (
    data.dropna()
    .stack(level=0, future_stack=True)
    .reset_index()
    .set_index("Date")
    .sort_values(by=["Ticker", "Date"])
    .rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Adj Close": "close",
            "Volume": "volume",
        }
    )
    .drop("Close", axis=1)
)

stacked.to_csv(f"{savepath}/{savename}.csv")
