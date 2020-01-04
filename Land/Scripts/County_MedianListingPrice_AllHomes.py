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
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_MedianListingPrice_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_MedianListingPrice_AllHomes_BACKUP.txt')


# In[ ]:


#Load Land data
df_mlp = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianListingPrice_AllHomes.csv',
                     encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_mlp.head()


# In[ ]:


#Filter data to NC
filter1 = df_mlp['State'] == "NC"
df_mlp_nc = df_mlp[filter1]

#Check to ensure filter worked
df_mlp_nc.head(5)


# In[ ]:


#View data types of dataframe
df_mlp_nc.dtypes


# In[ ]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_mlp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlp_nc['MunicipalCodeFIPS'].astype(str)
df_mlp_nc.dtypes


# In[ ]:


#Add leading 0's and check to ensure they were added
df_mlp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlp_nc['MunicipalCodeFIPS'].str.zfill(3)
df_mlp_nc.head(5)


# In[ ]:


# Set Index to Region Name
df_mlp_nc.set_index(df_mlp_nc['RegionName'], inplace = True)
df_mlp_nc


# In[ ]:


# Drop Region Name column
df_mlp_nc.drop('RegionName', axis = 1, inplace = True)
df_mlp_nc


# In[ ]:


#Save to csv file for export in Excel
df_mlp_nc.to_csv('./Updates/STG_ZLLW_County_MedianListingPrice_AllHomes.txt', sep ='\t')

