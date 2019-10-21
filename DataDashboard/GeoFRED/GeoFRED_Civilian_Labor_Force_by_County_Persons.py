#!/usr/bin/env python
# coding: utf-8

# In[15]:


# Imports  
import pandas as pd 


# In[16]:


# Create backups
df_backup = pd.read_csv('./Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt')
df_backup.to_csv('./Backups/STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP.txt')


# In[17]:


# Getting and reading new data 
df = pd.read_excel("https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=39.98&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=656&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Persons&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2018-01-01&type=xls&startDate=1990-01-01&endDate=2018-01-01&mapWidth=999&mapHeight=582&hideLegend=false", skiprows=1)
df.head()


# In[18]:


# Filter data to display only North Carolina
filter1 = df['Region Name'].str.contains(', NC')
df_nc = df[filter1]
df_nc.head()


# In[19]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv('./Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt', sep = '\t')

