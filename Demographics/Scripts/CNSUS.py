#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd


# In[ ]:


# Watermark
print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
get_ipython().run_line_magic('load_ext', 'watermark')
get_ipython().run_line_magic('watermark', '-a "Western Carolina University" -u -d -v -p pandas')


# In[ ]:


# Create Backups
#df_backup = pd.read_csv('', encoding = 'ISO-8859-1', sep='\t')
#df_backup.to_csv('')


# # United States

# In[ ]:


# Load data
df_nc_1 = pd.read_csv('../Data/PEP_2018_PEPAGESEX_with_ann_us.csv')
df_nc_1.head(2)


# In[ ]:


df_nc_2 = pd.read_csv('../Data/PEP_2018_PEPSR6H_with_ann_us.csv')
df_nc_2.head(2)


# # North Carolina

# In[ ]:


df_us_1 = pd.read_csv('../Data/PEP_2018_PEPAGESEX_with_ann_nc.csv')
df_us_1.head(2)


# In[ ]:


df_us_2 = pd.read_csv('../Data/PEP_2018_PEPSR6H_with_ann_nc.csv')
df_us_2.head(2)


# # Counties

# In[ ]:


df_co_1 = pd.read_csv('../Data/PEP_2018_PEPAGESEX_with_ann_county.csv')
df_co_1.head(2)


# In[ ]:


df_co_2 = pd.read_csv('../Data/PEP_2018_PEPSR6H_with_ann_county.csv')
df_co_2.head(2)

