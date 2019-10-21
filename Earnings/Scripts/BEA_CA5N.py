#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports
import pandas as pd


# In[ ]:


# Create Backups
df_backup = pd.read_csv('./Updates/')
df_backup.to_csv('./Backups/')


# In[ ]:


# Get and read new data
df = pd.read_csv('')
df.head()


# In[ ]:


# Filter to only get values for North Carolina
filter1 = df[''] == 'NC'
df_nc = df[filter1]
df_nc.head()


# In[ ]:


# Save as tab delimited for upload to SSMS
df_nc.to_csv('./Updates/', sep = '\t')

