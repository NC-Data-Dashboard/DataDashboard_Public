#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import openpyxl
import xlsxwriter


# In[ ]:


# Watermark
print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
get_ipython().run_line_magic('load_ext', 'watermark')
get_ipython().run_line_magic('watermark', '-a "Western Carolina University" -u -d -v -p pandas,numpy,openpyxl,xlsxwriter')


# In[ ]:


df = pd.read_excel('TableauData_NC_Health_Section_WIP.xlsx', worksheet='Health')
df.head()


# In[ ]:


df1 = pd.read_excel('GeoFIPS_Codes.xlsx')
df1.head()


# In[ ]:


df1.dtypes


# In[ ]:


df_m = pd.merge(df, df1, how='left', on=['GeoArea_Name'])
df_m.head()


# In[ ]:


df_m.dtypes


# In[ ]:


df_m.rename(columns={'GeoArea_FIPS_x':'GeoArea_FIPS'}, inplace = True)
df_m.head(2)


# In[ ]:


columns = ['GeoArea_FIPS','GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name',
       'Measure_Name', 'Data_Period_Business_Key', 'Estimated_Value',
       'Unit_of_Measure_Code']

df_m = df_m[columns]
df_m.tail(2)


# In[ ]:


df_m['GeoArea_FIPS'] = df_m['GeoArea_FIPS'].fillna(0).astype(np.int64)


# In[ ]:


df_m.set_index(df_m['GeoArea_FIPS'], inplace = True)
df_m.head(2)


# In[ ]:


df_m.drop('GeoArea_FIPS', axis = 1, inplace = True)
df_m.head(2)


# In[ ]:


df_m.to_excel('TableauData_NC_Health_Section_WIP.xlsx', engine='xlsxwriter')

