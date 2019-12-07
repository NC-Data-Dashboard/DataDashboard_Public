#!/usr/bin/env python
# coding: utf-8

# In[47]:


## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, December 2019 ##


# In[48]:


#Imports
import pandas as pd


# In[49]:


# Create Backups
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_MedianListingPrice_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_MedianListingPrice_AllHomes_BACKUP.txt')


# In[50]:


#Load Land data
df_mlp = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianListingPrice_AllHomes.csv',
                     encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_mlp.head()


# In[51]:


#Filter data to NC
filter1 = df_mlp['State'] == "NC"
df_mlp_nc = df_mlp[filter1]

#Check to ensure filter worked
df_mlp_nc.head(5)


# In[52]:


#View data types of dataframe
df_mlp_nc.dtypes


# In[53]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_mlp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlp_nc['MunicipalCodeFIPS'].astype(str)
df_mlp_nc.dtypes


# In[54]:


#Add leading 0's and check to ensure they were added
df_mlp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlp_nc['MunicipalCodeFIPS'].str.zfill(3)
df_mlp_nc.head(5)


# In[55]:


# Set Index to Region Name
df_mlp_nc.set_index(df_mlp_nc['RegionName'], inplace = True)
df_mlp_nc


# In[56]:


# Drop Region Name column
df_mlp_nc.drop('RegionName', axis = 1, inplace = True)
df_mlp_nc


# In[57]:


#Save to csv file for export in Excel
df_mlp_nc.to_csv('./Updates/STG_ZLLW_County_MedianListingPrice_AllHomes.txt', sep ='\t')

