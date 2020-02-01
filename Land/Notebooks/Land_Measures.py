#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


df = pd.read_excel('./Data/Land_MeasureDefn.xlsx')
df.head(2)


# In[3]:


df.columns


# In[4]:


df['Accessed_Date']


# In[5]:


# library for dates?  Datetime? 


# In[6]:


# Set Measure_Business_Key as index
df.set_index(df['Measure_Business_Key'], inplace = True)
df.head(2)


# In[7]:


# Drop Measure_Business_Key column
df.drop('Measure_Business_Key', axis = 1, inplace = True)
df.head(2)


# In[8]:


df.to_csv('./Updates/STG_XLSX_MeasureDefn_WRK.txt', sep = '\t')

