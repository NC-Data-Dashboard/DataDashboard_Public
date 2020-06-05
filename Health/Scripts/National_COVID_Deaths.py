import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import datetime as dt
import numpy as np

#create backups
df_backup = pd.read_csv('./Updates/STG_NYTI_NAT_COVID_19_Deaths.txt', sep='\t')
df_backup.to_csv('./Backups/STG_NYTI_NAT_COVID_19_Deaths_BACKUP.txt', sep='\t')

#read data
df = pd.read_csv('./Data/covid-19-data/us.csv')

#clean
df = df.rename(columns = {'date':'Data_Period_Business_Key'})
df['Data_Period_Business_Key'] = pd.to_datetime(df['Data_Period_Business_Key'])
df['Estimated_Value'] = df['deaths'].astype(float)
df = df.drop(['cases', 'deaths'], axis=1)

#add missing columns to match database
df['GeoArea_FIPS'] = '00000'
df['GeoArea_Name'] = 'United States'
df['Economic_Measure_Code'] = 'NYTI_NAT_COV04'
df['Economic_Measure_Name'] = 'COVID-19 Confirmed Deaths'
df['Measure_Name'] = ''
df['Unit_of_Measure_Code'] = 'Count'

#reset columns
columns = ['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Measure_Name', 'Data_Period_Business_Key', 'Estimated_Value', 'Unit_of_Measure_Code']
df = df[columns]
df.set_index('GeoArea_FIPS', inplace =True)

#save as txt
df.to_csv('./Updates/STG_NYTI_NAT_COVID_19_Deaths.txt', sep='\t')
df = df.reset_index()

#upload to database 
con = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=[server];"
    "Database=[database];"
    "Trusted_Connection=yes;",
    autocommit=True,
)

c = con.cursor()

#create new backup
c.execute("drop table STG_NYTI_NAT_COVID_19_Deaths_BACKUP")
c.execute("""sp_rename 'dbo.STG_NYTI_NAT_COVID_19_Deaths','STG_NYTI_NAT_COVID_19_Deaths_BACKUP';""")

params = urllib.parse.quote_plus(
	r"Driver={SQL Server};"
	r"Server=[server];"
	r"Database=[database];"
	r"Trusted_Connection=yes;"
	)

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, pool_pre_ping=True)

df.to_sql("STG_NYTI_NAT_COVID_19_Deaths", con=engine, if_exists="replace", index=False)
