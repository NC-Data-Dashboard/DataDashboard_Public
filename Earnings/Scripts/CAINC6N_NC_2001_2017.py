# %%
## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, November 2019 ##

# %%
# Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile

# %%
# Create Backups
''' Must create Backups manually!! '''

# %%
# Load BEA CAINC6N_NC data
response = requests.get('https://apps.bea.gov/regional/zip/CAINC6N.zip')
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")

# %%
# Check for non-data fields
df.tail(10)

# %%
# Remove non-data fields
df_clean = df[:-3]
df_clean.tail(5)

# %%
# Save as tab-delimited txt file for export to SSMS
df_clean.to_csv('./Updates/STG_BEA_CAINC6N_NC_2001_2017.txt', sep = '\t')