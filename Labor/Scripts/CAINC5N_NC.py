#!/usr/bin/env python
# coding: utf-8

# In[12]:


# Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile


# In[ ]:


# Watermark
print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
get_ipython().run_line_magic('load_ext', 'watermark')
get_ipython().run_line_magic('watermark', '-a "Western Carolina University" -u -d -p pandas')


# In[ ]:


# Create Backups
df_backup = pd.read_csv('./Updates/STG_BEA_CAINC5N_NC.txt', encoding = 'ISO-8859-1', sep='\t')
df_backup.to_csv('./Backups/STG_BEA_CAINC5N_NC_BACKUP.txt')


# In[ ]:


# Load BEA CAINC5N_NC data
response = requests.get('https://apps.bea.gov/regional/zip/CAINC5N.zip')
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")


# In[ ]:


# Check for unused fields
df.tail(10)


# In[ ]:


# Remove unused fields
df_clean = df[:-13231]
df_clean.tail(5)


# In[ ]:


# Set GeoFIPS as Index
df_clean.set_index(df_clean['GeoFIPS'], inplace = True)
df_clean.head()


# In[ ]:


# Drop GeoFIPS column
df_clean.drop('GeoFIPS', axis = 1, inplace = True)
df_clean.head()


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_clean.to_csv('./Updates/STG_BEA_CAINC5N_NC.txt', sep = '\t')

