#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Imports
import pandas as pd


# In[2]:


#Load Land data
df_mlp = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianListingPrice_AllHomes.csv',
                     encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_mlp.head()


# In[3]:


#Filter data to NC
filter1 = df_mlp['State'] == "NC"
df_mlp_nc = df_mlp[filter1]

#Check to ensure filter worked
df_mlp_nc.head(5)


# In[4]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_mlp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlp_nc['MunicipalCodeFIPS'].astype(str)
df_mlp_nc.dtypes


# In[5]:


#Add leading 0's and check to ensure they were added
df_mlp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlp_nc['MunicipalCodeFIPS'].str.zfill(3)
df_mlp_nc.head(5)


# In[6]:


#Save to csv file for export in Excel
df_mlp_nc.to_csv('./Data/STG_ZLLW_County_MedianListingPrice_AllHomes.txt', sep ='\t')

