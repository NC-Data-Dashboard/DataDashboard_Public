#!/usr/bin/env python
# coding: utf-8

# In[1]:


## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, December 2019 ##


# In[2]:


#Imports
import pandas as pd


# In[3]:


# Create Backups
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_Zhvi_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_Zhvi_AllHomes_BACKUP.txt')


# In[4]:


#Load Land data
df_zhvi = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_Zhvi_AllHomes.csv', 
                      encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_zhvi.head()


# In[5]:


#Filter data to NC
filter1 = df_zhvi['State'] == "NC"
df_zhvi_nc = df_zhvi[filter1]

#Check to ensure filter worked
df_zhvi_nc.head(5)


# In[6]:


#View data types of dataframe
df_zhvi_nc.dtypes


# In[7]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_zhvi_nc.loc[ :, 'MunicipalCodeFIPS'] = df_zhvi_nc['MunicipalCodeFIPS'].astype(str)
df_zhvi_nc.dtypes


# In[8]:


#Add leading 0's and check to ensure they were added
df_zhvi_nc.loc[ :, 'MunicipalCodeFIPS'] = df_zhvi_nc['MunicipalCodeFIPS'].str.zfill(3)
df_zhvi_nc.head(5)


# In[ ]:


# Set Index to Region Name
df_zhvi_nc.set_index(df_zhvi_nc['RegionName'], inplace = True)
df_zhvi_nc.head(5)


# In[ ]:


# Drop Region Name column
df_zhvi_nc.drop('RegionName', axis = 1, inplace = True)
df_zhvi_nc.head(5)


# In[9]:


#Save to csv file for export in Excel
df_zhvi_nc.to_csv('./Updates/STG_ZLLW_County_Zhvi_AllHomes.txt', sep = '\t')

