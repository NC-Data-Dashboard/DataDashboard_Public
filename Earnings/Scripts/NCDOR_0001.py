#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd


# In[ ]:


# Watermark
print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
get_ipython().run_line_magic('load_ext', 'watermark')
get_ipython().run_line_magic('watermark', '-a "Western Carolina University" -u -d -p pandas')


# In[ ]:


df = pd.read_csv('https://files.nc.gov/ncdor/documents/reports/monthly_sales_10-19.xls')


# In[ ]:




