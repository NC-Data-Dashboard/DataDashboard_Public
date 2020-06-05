#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import numpy as np


# In[ ]:


#df_backup = pd.read_csv('./Updates/STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living', sep='\t')
#df_backup.to_csv('./Backups/STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living_BACKUP', sep='\t')


# In[ ]:


df_fips = pd.read_csv('./FIPS_Codes.csv')

df = pd.read_html('https://www.ncdhhs.gov/divisions/public-health/covid19/covid-19-nc-case-count#by-congregate-living')

df[3].to_csv('./Data/STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living.csv', sep='\t')

df = pd.read_csv('./Data/STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living.csv', sep='\t')

df = df.drop('Unnamed: 0', axis=1)

df = df.rename(columns={'Ongoing Outbreaks1':'Ongoing Outbreaks', 'Counties with Ongoing Outbreaks2':'Counties with Ongoing Outbreaks'})

df = df.set_index('Facility/Setting')

df = df.rename(index={'Nursing Home3':'Nursing Home', 'Residential Care Facilities4':'Residential Care Facilities', 'Correctional Facility5':'Correctional Facility'})

df.to_csv('./Data/STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living.csv', sep='\t')

df.to_csv('./Updates/STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living.txt', sep='\t')


# In[ ]:


#Connect to database
con = pyodbc.connect('Driver={SQL Server};'
                    'Server=[server];'
                    'Database=[database];'
                    'Trusted_Connection=yes;',
                    autocommit=True)
c = con.cursor()


# In[ ]:


c.execute('drop table STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living_BACKUP')

c.execute('''sp_rename 'dbo.STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living', 'STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living_BACKUP';''')

c.execute('''USE [[database]]
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [dbo].[STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living](
    [County] [varchar](40) NULL,
    [Laboratory-Confirmed Cases] [int] NULL,
    [Deaths] [int] NULL,
    [1975] [float] NULL,
    [1976] [float] NULL,
    [1977] [float] NULL,
    [1978] [float] NULL,
    [1979] [float] NULL,
    [1980] [float] NULL,
    [1981] [float] NULL,
    [1982] [float] NULL,
    [1983] [float] NULL,
    [1984] [float] NULL,
    [1985] [float] NULL,
    [1986] [float] NULL,
    [1987] [float] NULL,
    [1988] [float] NULL,
    [1989] [float] NULL,
    [1990] [float] NULL,
    [1991] [float] NULL,
    [1992] [float] NULL,
    [1993] [float] NULL,
    [1994] [float] NULL,
    [1995] [float] NULL,
    [1996] [float] NULL,
    [1997] [float] NULL,
    [1998] [float] NULL,
    [1999] [float] NULL,
    [2000] [float] NULL,
    [2001] [float] NULL,
    [2002] [float] NULL,
    [2003] [float] NULL,
    [2004] [float] NULL,
    [2005] [float] NULL,
    [2006] [float] NULL,
    [2007] [float] NULL,
    [2008] [float] NULL,
    [2009] [float] NULL,
    [2010] [float] NULL,
    [2011] [float] NULL,
    [2012] [float] NULL,
    [2013] [float] NULL,
    [2014] [float] NULL,
    [2015] [float] NULL,
    [2016] [float] NULL,
    [2017] [float] NULL,
    [2018] [float] NULL,
    [2019] [float] NULL,
    [2020] [float] NULL,
    [2021] [float] NULL,
    [2022] [float] NULL,
    [2023] [float] NULL,
    [2024] [float] NULL,
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                    r'Server=[server];'
                                    r'Database=[database];'
                                    r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df.to_sql('STG_NCDHHS_COVID_NC_Cases_by_Congregate_Living', con=engine, if_exists='replace', index=False)

