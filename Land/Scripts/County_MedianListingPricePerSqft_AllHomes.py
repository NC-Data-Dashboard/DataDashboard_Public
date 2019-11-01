#!/usr/bin/env python
# coding: utf-8

# In[11]:


#Imports
import pandas as pd


# In[12]:


# Create Backups
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_MedianListingPricePerSqft_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_MedianListingPricePerSqft_AllHomes_BACKUP.txt')


# In[13]:


#Load Land data
df_mlsf = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianListingPricePerSqft_AllHomes.csv', 
                      encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_mlsf.head()


# In[14]:


#Filter data to NC
filter1 = df_mlsf['State'] == "NC"
df_mlsf_nc = df_mlsf[filter1]

#Check to ensure filter worked
df_mlsf_nc.head(5)


# In[15]:


#View data types of dataframe
df_mlsf_nc.dtypes


# In[16]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_mlsf_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlsf_nc['MunicipalCodeFIPS'].astype(str)
df_mlsf_nc.dtypes


# In[17]:


#Add leading 0's and check to ensure they were added
df_mlsf_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlsf_nc['MunicipalCodeFIPS'].str.zfill(3)
df_mlsf_nc.head(5)


# In[18]:


#Save to csv file for export in Excel
df_mlsf_nc.to_csv('./Updates/STG_ZLLW_County_MedianListingPricePerSqft_AllHomes.txt', sep = '\t')

