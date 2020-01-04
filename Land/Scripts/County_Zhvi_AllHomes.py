#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Imports
import pandas as pd


# In[ ]:


# Watermark
print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
get_ipython().run_line_magic('load_ext', 'watermark')
get_ipython().run_line_magic('watermark', '-a "Western Carolina University" -u -d -p pandas')


# In[ ]:


# Create Backups
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_Zhvi_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_Zhvi_AllHomes_BACKUP.txt')


# In[ ]:


#Load Land data
df_zhvi = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_Zhvi_AllHomes.csv', 
                      encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_zhvi.head()


# In[ ]:


#Filter data to NC
filter1 = df_zhvi['State'] == "NC"
df_zhvi_nc = df_zhvi[filter1]

#Check to ensure filter worked
df_zhvi_nc.head(5)


# In[ ]:


#View data types of dataframe
df_zhvi_nc.dtypes


# In[ ]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_zhvi_nc.loc[ :, 'MunicipalCodeFIPS'] = df_zhvi_nc['MunicipalCodeFIPS'].astype(str)
df_zhvi_nc.dtypes


# In[ ]:


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


# In[ ]:


#Save to csv file for export in Excel
df_zhvi_nc.to_csv('./Updates/STG_ZLLW_County_Zhvi_AllHomes.txt', sep = '\t')

