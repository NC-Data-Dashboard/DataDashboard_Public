#!/usr/bin/env python
# coding: utf-8

# In[33]:


import pandas as pd


# In[34]:


'''Must create backups manually at this time!!
Add _BACKUP to the name of the .txt file in Updates and move it to 
 Backups folder before running this script!'''


# In[35]:


df = pd.read_csv('./Data/CAINC5N_NC_2001_2017.csv')
df.head()


# In[36]:


df.to_csv('./Updates/STG_BEA_CAINC5N_NC_2001_2017.txt', sep = '\t')

