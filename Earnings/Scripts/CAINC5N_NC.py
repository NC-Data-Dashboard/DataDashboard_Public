#!/usr/bin/env python
# coding: utf-8

# In[18]:


## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, December 2019 ##


# In[19]:


# Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile


# In[20]:


# Create Backups
df_backup = pd.read_csv('./Updates/STG_BEA_CAINC5N_NC.txt', encoding = 'ISO-8859-1', sep='\t')
df_backup.to_csv('./Backups/STG_BEA_CAINC5N_NC_BACKUP.txt')


# In[21]:


# Load BEA CAINC5N_NC data
response = requests.get('https://apps.bea.gov/regional/zip/CAINC5N.zip')
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")


# In[22]:


# Check for non-data fields
df.tail(10)


# In[23]:


# Remove non-data fields
df_clean = df[:-4]
df_clean.tail(5)


# In[24]:


# Set GeoFIPS as Index
df_clean.set_index(df_clean['GeoFIPS'], inplace = True)
df_clean.head(2)


# In[25]:


# Drop GeoFIPS column
df_clean.drop('GeoFIPS', axis = 1, inplace = True)
df_clean.head(2)


# In[26]:


# Save as tab-delimited txt file for export to SSMS
df_clean.to_csv('./Updates/STG_BEA_CAINC5N_NC.txt', sep = '\t')

