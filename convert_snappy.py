import pandas as pd
import os

input_dir  = "source/yellow/"
output_dir = "source/yellow/"

for f in os.listdir(input_dir):
    if f.endswith(".parquet"):
        print("Conversion : " + f)
        df = pd.read_parquet(input_dir + f)
        df.to_parquet(
            output_dir + f,
            engine="pyarrow",
            compression="snappy",
            coerce_timestamps="ms",
            allow_truncated_timestamps=True
        )
        print("  OK")

print("Terminé !")