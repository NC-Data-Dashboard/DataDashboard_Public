#!/usr/bin/env python
# coding: utf-8

# In[ ]:


## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, October 2019 ##


# In[10]:


#Imports
import pandas as pd


# In[11]:


# Create Backups
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_MedianListingPrice_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_MedianListingPrice_AllHomes_BACKUP.txt')


# In[12]:


#Load Land data
df_mlp = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianListingPrice_AllHomes.csv',
                     encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_mlp.head()


# In[13]:


#Filter data to NC
filter1 = df_mlp['State'] == "NC"
df_mlp_nc = df_mlp[filter1]

#Check to ensure filter worked
df_mlp_nc.head(5)


# In[14]:


#View data types of dataframe
df_mlp_nc.dtypes


# In[15]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_mlp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlp_nc['MunicipalCodeFIPS'].astype(str)
df_mlp_nc.dtypes


# In[16]:


#Add leading 0's and check to ensure they were added
df_mlp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlp_nc['MunicipalCodeFIPS'].str.zfill(3)
df_mlp_nc.head(5)


# In[17]:


#Save to csv file for export in Excel
df_mlp_nc.to_csv('./Updates/STG_ZLLW_County_MedianListingPrice_AllHomes.txt', sep ='\t')

