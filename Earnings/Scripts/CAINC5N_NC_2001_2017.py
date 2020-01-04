#!/usr/bin/env python
# coding: utf-8

# In[1]:


## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, November 2019 ##


# In[2]:


# Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile


# In[3]:


# Create Backups
''' Must create backups manually!! '''


# In[4]:


# Load BEA CAINC5N_NC data
response = requests.get('https://apps.bea.gov/regional/zip/CAINC5N.zip')
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")


# In[5]:


# Check for non-data fields
df.tail(10)


# In[6]:


# Remove non-data fields
df_clean = df[:-4]
df_clean.tail(5)


# In[7]:


# Save as tab-delimited txt file for export to SSMS
df_clean.to_csv('./Updates/STG_BEA_CAINC5N_NC_2001_2017.txt', sep = '\t')

