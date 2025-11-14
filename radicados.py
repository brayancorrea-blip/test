import os
import pandas as pd

path = "mnt/data/radicados_filtrados (8).xlsx"
df = pd.read_excel(path)

vals = [
    "ENT20250001540959",
    "ENT20250001540215",
    "ENT20250001540679"
]

filtered = df[df["No_Radicado"].isin(vals)]

out_path = "mnt/data/radicados_filtrados.xlsx"
filtered.to_excel(out_path, index=False)

out_path
