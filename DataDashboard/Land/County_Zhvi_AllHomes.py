#!/usr/bin/env python
# coding: utf-8

# In[7]:


#Imports
import pandas as pd


# In[8]:


#Load Land data
df_zhvi = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_Zhvi_AllHomes.csv', encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_zhvi.head()


# In[9]:


#Filter data to NC
filter1 = df_zhvi['State'] == "NC"
df_zhvi_nc = df_zhvi[filter1]

#Check to ensure filter worked
df_zhvi_nc.head(5)


# In[10]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_zhvi_nc.loc[ :, 'MunicipalCodeFIPS'] = df_zhvi_nc['MunicipalCodeFIPS'].astype(str)


# In[11]:


#Add leading 0's and check to ensure they were added
df_zhvi_nc.loc[ :, 'MunicipalCodeFIPS'] = df_zhvi_nc['MunicipalCodeFIPS'].str.zfill(3)
df_zhvi_nc.head(5)


# In[12]:


#Save to csv file for export in Excel
df_zhvi_nc.to_csv('./Data/STG_ZLLW_County_Zhvi_AllHomes.txt', sep = '\t')

