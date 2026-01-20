import sys
import pandas as pd

out = sys.argv[1]

rows = pd.DataFrame({'A' : [1,2], 'B': [3,4]})

print(rows.head())

rows.to_parquet(f"ouput_{out}.parquet")