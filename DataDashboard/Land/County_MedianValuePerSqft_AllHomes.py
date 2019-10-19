#!/usr/bin/env python
# coding: utf-8

# In[13]:


#Imports
import pandas as pd


# In[14]:


#Load Land data
df_mvsf = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianValuePerSqft_AllHomes.csv', encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_mvsf.head()


# In[15]:


#Filter data to NC
filter1 = df_mvsf['State'] == "NC"
df_mvsf_nc = df_mvsf[filter1]

#Check to ensure filter worked
df_mvsf_nc.head(5)


# In[16]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_mvsf_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mvsf_nc['MunicipalCodeFIPS'].astype(str)


# In[17]:


#Add leading 0's and check to ensure they were added
df_mvsf_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mvsf_nc['MunicipalCodeFIPS'].str.zfill(3)
df_mvsf_nc.head(5)


# In[18]:


#Save to csv file for export in Excel
df_mvsf_nc.to_csv('./Data/STG_ZLLW_County_MedianValuePerSqft_AllHomes.txt', sep = '\t')

