import pandas as pd
import talib
from omegaconf import OmegaConf

# Parsing Configuration
config = OmegaConf.load("./params.yaml")
loadpath = config.volumes.ingested_folder
loadname = config.volumes.ingested_dataset
savepath = config.volumes.features_folder
savename = config.volumes.features_dataset

df = pd.read_csv(f"{loadpath}/{loadname}.csv", index_col="Date")

tickers_in_dataframe = df["Ticker"].unique().tolist()

candle_names = talib.get_function_groups()["Pattern Recognition"]

features_dataframe = []
for ticker in tickers_in_dataframe:
    tmp = df[df["Ticker"] == ticker].copy()
    for candle in candle_names:
        tmp["PATTERN_" + candle] = getattr(talib, candle)(
            tmp["open"],
            tmp["high"],
            tmp["high"],
            tmp["close"],
        )
    features_dataframe.append(tmp)

features = pd.concat(features_dataframe)

features.to_csv(f"{savepath}/{savename}.csv")
