#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports
import urllib
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import requests
from sqlalchemy import create_engine
import pyodbc


# In[ ]:


# Load BEA CAINC5N_NC data
response = requests.get("https://apps.bea.gov/regional/zip/CAINC5N.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",")


# In[ ]:


# Check for unused fields
df.tail(10)


# In[ ]:


# Remove unused fields
df.drop(df.tail(4).index, inplace=True)


# In[ ]:


# Clean GeoFIPS
df["GeoFIPS"] = df["GeoFIPS"].replace({"": ""})


# In[ ]:


# Set GeoFIPS as Index
df.set_index(df["GeoFIPS"], inplace=True)


# In[ ]:


# Drop GeoFIPS column
df.drop("GeoFIPS", axis=1, inplace=True)


# In[ ]:

# # Create Per Capita Personal Income

# In[ ]:


print("Updating Per Capita Personal Income...")


# In[ ]:


# Create Backups
df_pc_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_pc_backup.to_csv("./Backups/STG_BEA_CA5N_Per_Capita_Personal_Income_BACKUP.txt")


# In[ ]:


# Create new dataframe for Per capita personal income
filter1 = df["LineCode"] == 30
df_per_capita = df[filter1]
df_per_capita.head()


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_per_capita.to_csv("./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt", sep="\t")


# # Create Earnings by Place of Work

# In[ ]:


print("Done. Updating Earnings by Place of Work...")


# In[ ]:


# Create Backups
df_e_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_e_backup.to_csv("./Backups/STG_BEA_CA5N_Earnings_by_Place_of_Work_BACKUP.txt")


# In[ ]:


# Create a new dataframe for Earnings by place of work
filter1 = df["LineCode"] == 35
df_earnings = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_earnings.to_csv("./Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt", sep="\t")


# # Create Population

# In[ ]:


print("Done. Updating Population...")


# In[ ]:


# Create Backups
df_pop_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Population.txt", encoding="ISO-8859-1", sep="\t"
)
df_pop_backup.to_csv("./Backups/STG_BEA_CA5N_Population_BACKUP.txt")


# In[ ]:


# Create a new dataframe for Population
filter1 = df["LineCode"] == 20
df_population = df[filter1]


# In[ ]:


# Clean Description column
df_population.loc[:, "Description"] = df_population["Description"].str.strip("2/")


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_population.to_csv("./Updates/STG_BEA_CA5N_Population.txt", sep="\t")


# # Create Personal Income

# In[ ]:


print("Done. Updating Personal Income...")


# In[ ]:


# Create Backups
df_i_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Personal_Income.txt", encoding="ISO-8859-1", sep="\t"
)
df_i_backup.to_csv("./Backups/STG_BEA_CA5N_Personal_Income_BACKUP.txt")


# In[ ]:


# Create new dataframe for Personal Income
filter1 = df["LineCode"] == 10
df_income = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_income.to_csv("./Updates/STG_BEA_CA5N_Personal_Income.txt", sep="\t")
