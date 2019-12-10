#!/usr/bin/env python
# coding: utf-8

# In[9]:


## Written and published by Nathan Young, Junior Data Analyst for NC Data Dashboard, December 2019 ##


# In[10]:


# Imports  
import pandas as pd 


# In[11]:


# Create backups
df_backup = pd.read_csv('./Updates/STG_FRED_Homeownership_Rate_by_County.txt')
df_backup.to_csv('./Backups/STG_FRED_Homeownership_Rate_by_County_BACKUP.txt')


# In[12]:


# Getting and reading new data 
df = pd.read_excel("https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=157125&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Rate&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2017-01-01&type=xls&startDate=2009-01-01&endDate=2017-01-01&mapWidth=999&mapHeight=521&hideLegend=false", skiprows=1)
df.head(2)


# In[13]:


# Filter data to display only North Carolina
filter1 = df['Region Name'].str.contains(', NC')
df_nc = df[filter1]
df_nc.head(2)


# In[14]:


# Set Index to Series ID
df_nc.set_index(df_nc['Series ID'], inplace = True)
df_nc.head(2)


# In[15]:


# Drop Series ID column
df_nc.drop('Series ID', axis = 1, inplace = True)
df_nc.head(2)


# In[16]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv('./Updates/STG_FRED_Homeownership_Rate_by_County.txt', sep = '\t')

