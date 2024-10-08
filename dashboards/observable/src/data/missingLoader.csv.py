import pandas as pd
import sys

# Read the CSV
df = pd.read_csv("src/data/missing.csv")
# Write to CSV
pd.to_csv(sys.stdout)