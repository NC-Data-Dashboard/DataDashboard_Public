import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

#create backups
df_backup = pd.read_csv('./Updates/STG_NYTI_CNTY_COVID_19_Deaths.txt', sep='\t')
df_backup.to_csv('./Backups/STG_NYTI_CNTY_COVID_19_Deaths_BACKUP.txt', sep='\t')

#read data
df = pd.read_csv('./Data/covid-19-data/us-counties.csv')

#fitler to nc
filter1 = df['state'].str.contains('North Carolina')
df = df[filter1]

#clean
df['fips'] = df['fips'].astype(int)
df = df.rename(columns = {"fips":'GeoArea_FIPS', 'county':'GeoArea_Name', 'deaths':'Estimated_Value', 'date':'Data_Period_Business_Key'})
df = df.drop(['cases', 'state'], axis=1)

df['Data_Period_Business_Key'] = pd.to_datetime(df['Data_Period_Business_Key'])
df['Estimated_Value'] = df['Estimated_Value'].astype(float)

#add missing columns to match database
df['Economic_Measure_Code'] = 'NYTI_CNTY_COV02'
df['Economic_Measure_Name'] = 'COVID-19 Confirmed Deaths'
df['Measure_Name'] = ''
df['Unit_of_Measure_Code'] = 'Count'

#reset columns
columns = ['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Measure_Name', 'Data_Period_Business_Key', 'Estimated_Value', 'Unit_of_Measure_Code']
df = df[columns]
df.set_index('GeoArea_FIPS', inplace =True)

#save as txt
df.to_csv('./Updates/STG_NYTI_CNTY_COVID_19_Deaths.txt', sep='\t')
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
c.execute("drop table STG_NYTI_CNTY_COVID_19_Deaths_BACKUP")
c.execute("""sp_rename 'dbo.STG_NYTI_CNTY_COVID_19_Deaths','STG_NYTI_CNTY_COVID_19_Deaths_BACKUP';""")

c.execute("""USE [[database]]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_NYTI_CNTY_COVID_19_Deaths](
	[GeoArea_FIPS] [varchar](50) NULL,
	[GeoArea_Name] [varchar](50) NULL,
	[Economic_Measure_Code] [varchar](50) NULL,
	[Economic_Measure_Name] [varchar](50) NULL,
	[Measure_Name] [varchar](50) NULL,
	[Data_Period_Business_Key] [datetime] NULL,
	[Estimated_Value] [float] NULL,
	[Unit_of_Measure_Code] [varchar](50) NULL,
) ON [PRIMARY]
""")

params = urllib.parse.quote_plus(r"Driver={SQL Server};"r"Server=[server];"r"Database=[database];"r"Trusted_Connection=yes;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, pool_pre_ping=True)
df.to_sql("STG_NYTI_CNTY_COVID_19_Deaths", con=engine, if_exists="replace", index=False)
