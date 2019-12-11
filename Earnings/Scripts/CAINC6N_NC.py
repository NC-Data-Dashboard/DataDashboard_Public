#!/usr/bin/env python
# coding: utf-8

# In[17]:


## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, December 2019 ##


# In[18]:


# Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile


# In[19]:


# Create Backups
df_backup = pd.read_csv('./Updates/STG_BEA_CAINC6N_NC.txt', 
                        encoding = 'ISO-8859-1', 
                        sep = "\t")
df_backup.to_csv('./Backups/STG_BEA_CAINC6N_NC_BACKUP.txt')


# In[20]:


# Load BEA CAINC6N_NC data
response = requests.get('https://apps.bea.gov/regional/zip/CAINC6N.zip')
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")


# In[21]:


# Check for non-data fields
df.tail(10)


# In[22]:


# Remove non-data fields
df_clean = df[:-3]
df_clean.tail(5)


# In[23]:


# Set GeoFIPS as Index
df_clean.set_index(df_clean['GeoFIPS'], inplace = True)
df_clean.head(2)


# In[24]:


# Drop GeoFIPS column 
df_clean.drop('GeoFIPS', axis = 1, inplace = True)
df_clean.head(2)


# In[25]:


# Save as tab-delimited txt file for export to SSMS
df_clean.to_csv('./Updates/STG_BEA_CAINC6N_NC.txt', sep = '\t')

