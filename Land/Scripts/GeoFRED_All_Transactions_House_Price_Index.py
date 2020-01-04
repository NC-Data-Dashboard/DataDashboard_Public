#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports  
import pandas as pd


# In[ ]:


# Watermark
print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
get_ipython().run_line_magic('load_ext', 'watermark')
get_ipython().run_line_magic('watermark', '-a "Western Carolina University" -u -d -p pandas')


# In[ ]:


# Create backups
df_backup = pd.read_csv('./Updates/STG_FRED_All_Transactions_House_Price_Index.txt')
df_backup.to_csv('./Backups/STG_FRED_All_Transactions_House_Price_Index_BACKUP.txt')


# In[ ]:


# Getting and reading new data 
df = pd.read_excel("https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90&lat=40&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=942&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Index+2000%3D100&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2018-01-01&type=xls&startDate=1975-01-01&endDate=2018-01-01&mapWidth=999&mapHeight=1249&hideLegend=false", skiprows=1)
df.head(2)


# In[ ]:


# Filter data to display only North Carolina
filter1 = df['Region Name'].str.contains(', NC')
df_nc = df[filter1]
df_nc.head(2)


# In[ ]:


# Set Index to Series ID
df_nc.set_index(df_nc['Series ID'], inplace = True)
df_nc.head(2)


# In[ ]:


# Drop Series ID column
df_nc.drop('Series ID', axis = 1, inplace = True)
df_nc.head(2)


# In[ ]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv('./Updates/STG_FRED_All_Transactions_House_Price_Index.txt', sep = '\t')

