#!/usr/bin/env python
# coding: utf-8

# In[ ]:


## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, October 2019 ##


# In[6]:


# Imports  
import pandas as pd 


# In[7]:


# Create backups
df_backup = pd.read_csv('./Updates/STG_FRED_All_Transactions_House_Price_Index.txt')
df_backup.to_csv('./Backups/STG_FRED_All_Transactions_House_Price_Index_BACKUP.txt')


# In[8]:


# Getting and reading new data 
df = pd.read_excel("https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90&lat=40&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=942&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Index+2000%3D100&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2018-01-01&type=xls&startDate=1975-01-01&endDate=2018-01-01&mapWidth=999&mapHeight=1249&hideLegend=false", skiprows=1)
df.head(5)


# In[9]:


# Filter data to display only North Carolina
filter1 = df['Region Name'].str.contains(', NC')
df_nc = df[filter1]
df_nc.head()


# In[10]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv('./Updates/STG_FRED_All_Transactions_House_Price_Index.txt', sep = '\t')

