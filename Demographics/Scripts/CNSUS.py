#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd


# In[ ]:


# Watermark
#print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
#%load_ext watermark
#%watermark -a "Western Carolina University" -u -d -v -p pandas


# In[ ]:


# Create Backups
#df_backup = pd.read_csv('', encoding = 'ISO-8859-1', sep='\t')
#df_backup.to_csv('')


# # United States

# In[ ]:


#Load data
df_us = pd.read_csv('../Data/PEP_2018_PEPAGESEX_with_ann_us.csv', skiprows = 1)
print('number of rows:', df_us.shape[0])
print('number of columns:', df_us.shape[1])


# In[ ]:


#Melt data
df_us = df_us.melt(id_vars=['Geography'], var_name='Economic Measure Name', value_name='Estimated Value')
print('number of rows:', df_us.shape[0])
print('number of columns:', df_us.shape[1])


# In[ ]:


#Drop unnecessary rows
df_us = df_us.drop(df_us.index[:2])
df_us.head()


# # North Carolina

# In[ ]:


#Load data
df_nc1 = pd.read_csv('../Data/PEP_2018_PEPAGESEX_with_ann_nc.csv', skiprows=1)
print('number of rows:', df_nc1.shape[0])
print('number of columns:', df_nc1.shape[1])


# In[ ]:


#Melt data
df_nc1 = df_nc1.melt(id_vars=['Geography'], var_name='Economic Measure Name', value_name='Estimated Value')
print('number of rows:', df_nc1.shape[0])
print('number of columns:', df_nc1.shape[1])


# In[ ]:


#Filter data to display only North Carolina
filter1 = df_nc1['Geography'].str.contains('North Carolina')
df_nc1 = df_nc1[filter1]
df_nc1.head()


# In[ ]:


#Drop unnecessary rows
df_nc1 = df_nc1.drop(df_nc1.index[:2])
df_nc1.head()


# In[ ]:


#Load data
df_nc2 = pd.read_csv('../Data/PEP_2018_PEPSR6H_with_ann_nc.csv', skiprows=1)
print('number of rows:', df_nc2.shape[0])
print('number of columns:', df_nc2.shape[1])


# # Counties

# In[ ]:


#Load data
df_co1 = pd.read_csv('../Data/PEP_2018_PEPAGESEX_with_ann_county.csv', skiprows=1)
print('number of rows:', df_co1.shape[0])
print('number of columns:', df_co1.shape[1])


# In[ ]:


#Melt data
df_co1 = df_co1.melt(id_vars=['Geography'], var_name='Economic Measure Name', value_name='Estimated Value')
print('number of rows:', df_co1.shape[0])
print('number of columns:', df_co1.shape[1])


# In[ ]:


#Drop unnecessary rows
df_co1 = df_co1.drop(df_co1.index[:200])
df_co1.head()


# In[ ]:


#Load data
df_co2 = pd.read_csv('../Data/PEP_2018_PEPSR6H_with_ann_county.csv', skiprows=1)
print('number of rows:', df_co2.shape[0])
print('number of columns:', df_co2.shape[1])
df_co2

