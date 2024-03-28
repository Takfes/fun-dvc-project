import json

import pandas as pd
from omegaconf import OmegaConf

# Parsing Configuration
config = OmegaConf.load("./params.yaml")
loadpath = config.volumes.features_folder
loadname = config.volumes.features_dataset
savepath = config.volumes.curated_folder
savename = config.volumes.curated_dataset
nbr_days_to_keep = config.train.nbr_days_to_keep

df = pd.read_csv(f"{loadpath}/{loadname}.csv", index_col="Date")

df_last_day = df.groupby("Ticker").tail(nbr_days_to_keep)

patterns_last_day = df_last_day.set_index("Ticker").filter(like="PATTERN")

patterns_per_ticker = {}
for i, row in patterns_last_day.iterrows():
    patterns_list = row.loc[lambda x: x != 0].index.tolist()
    patterns_list_trimmed_colnames = [
        colname.replace("PATTERN_", "").replace("CDL", "") for colname in patterns_list
    ]
    if patterns_list:
        patterns_per_ticker[i] = patterns_list_trimmed_colnames

curated = dict(
    sorted(patterns_per_ticker.items(), key=lambda x: len(x[1]), reverse=True)
)

# Save curated dictionary to curated folder
save_file = f"{savepath}/{savename}.json"
with open(save_file, "w") as f:
    json.dump(curated, f)
