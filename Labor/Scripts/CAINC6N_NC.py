#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import numpy as np


# In[ ]:


# Watermark
#print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
#get_ipython().run_line_magic('load_ext', 'watermark')
#get_ipython().run_line_magic('watermark', '-a "Western Carolina University" -u -d -p pandas')


# In[ ]:


# Create Backups
df_backup = pd.read_csv('../Updates/STG_BEA_CAINC6N_NC.txt', encoding = 'ISO-8859-1', sep = "\t")
df_backup.to_csv('../Backups/STG_BEA_CAINC6N_NC_BACKUP.txt')


# In[ ]:


# Load BEA CAINC6N_NC data
response = requests.get('https://apps.bea.gov/regional/zip/CAINC6N.zip')
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")


# In[ ]:


# Check for non-data fields
df.tail(10)


# In[ ]:


# Remove unused fields
df.drop(df.tail(3).index,inplace=True)
df.tail()


# In[ ]:


#Clean GeoFIPS
df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
df


# In[ ]:


# Set GeoFIPS as Index
df.set_index(df['GeoFIPS'], inplace = True)
df.head()


# In[ ]:


# Drop GeoFIPS column 
df.drop('GeoFIPS', axis = 1, inplace = True)
df.head()


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_clean.to_csv('../Updates/STG_BEA_CAINC6N_NC.txt', sep = '\t')


# # SSMS Update as Markdown Cells
# ### To run code below, change to 'Code' cells

# #Reset Index for upload to database
# df = df.reset_index()    

# #Fill NaN values for upload to database
# column_list = df.columns.values
# for i in column_list:
#     df.loc[df[i].isnull(),i]=0

# #Connect to database and create cursor
# con = pyodbc.connect('Driver={SQL Server};'
#                       'Server=STEIN\ECONDEV;'
#                       'Database=STG2;'
#                       'Trusted_Connection=yes;',
#                     autocommit=True)
# 
# c = con.cursor()

# #Drop old backup table
# c.execute('drop table STG_BEA_CAINC6N_NC_BACKUP')

# #Create new backup
# c.execute('''sp_rename 'dbo.STG_BEA_CAINC6N_NC','STG_BEA_CAINC6N_NC_BACKUP';''')

# c.execute('''USE [STG2]
# 
# SET ANSI_NULLS ON
# 
# SET QUOTED_IDENTIFIER ON
# 
# CREATE TABLE [dbo].[STG_BEA_CAINC6N_NC](
# 	[GeoFIPS] [varchar](12) NULL,
# 	[GeoName] [varchar](14) NULL,
# 	[Region] [real] NULL,
# 	[TableName] [varchar](7) NULL,
# 	[LineCode] [real] NULL,
# 	[IndustryClassification] [varchar](3) NULL,
# 	[Description] [varchar](38) NULL,
# 	[Unit] [varchar](20) NULL,
# 	[2001] [float] NULL,
# 	[2002] [float] NULL,
# 	[2003] [float] NULL,
# 	[2004] [float] NULL,
# 	[2005] [float] NULL,
# 	[2006] [float] NULL,
# 	[2007] [float] NULL,
# 	[2008] [float] NULL,
# 	[2009] [float] NULL,
# 	[2010] [float] NULL,
# 	[2011] [float] NULL,
# 	[2012] [float] NULL,
# 	[2013] [float] NULL,
# 	[2014] [float] NULL,
# 	[2015] [float] NULL,
# 	[2016] [float] NULL,
# 	[2017] [float] NULL,
# 	[2018] [float] NULL,
#     [2019] [float] NULL,
#     [2020] [float] NULL,
#     [2021] [float] NULL,
#     [2022] [float] NULL,
#     [2023] [float] NULL,
#     [2024] [float] NULL,
#     [2025] [float] NULL
# ) ON [PRIMARY]''')

# params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
#                                  r'Server=STEIN\ECONDEV;'
#                                  r'Database=STG2;'
#                                  r'Trusted_Connection=yes;')
# 
# engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
# 
# #df: pandas.dataframe; mTableName:table name in MS SQL
# #warning: discard old table if exists
# df.to_sql('STG_BEA_CAINC6N_NC', con=engine, if_exists='replace', index=False)

# c.execute("""""")

# c.execute("""/**************************************************************************/
# /***																	***/
# /***				PopulateDV2_Measure_Tables.sql						***/
# /***																	***/
# /***	The script adds entries to:										***/
# /***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
# /***				-	[DV2].[dbo].[Hub_Measure]						***/
# /***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
# /***	and updates the Load End Date for entries retiring in			***/
# /***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
# /***	All input come from:											***/
# /***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
# /***	This is the only place where HashKeys are calculated for 		***/
# /***	Measures														***/
# /***																	***/
# /**************************************************************************/
# 
# USE DV2;
# 
# 
# TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];
# 
# INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
#            ([Measure_Business_Key]
# 		   ,[Record_Source]
#            ,[Measure_HashKey]
#            ,[Measure_HashDiff]
#            ,[Measure_Authority]
#            ,[TableID]
#            ,[Table_LineCode]
#            ,[MeasureGroupName]
#            ,[MeasureName]
#            ,[MeasureCategory]
#            ,[Observation_Frequency]
#            ,[UOMCode]
#            ,[UOMName]
#            ,[DefaultScale]
#            ,[CalculationType]
#            ,[MetricHeirarchyLevel]
#            ,[ParticipatesIn]
#            ,[NAICS_IndustryCodeStr]
#            ,[BEA_Industry_ID]
#            ,[BEA_GDP_Component_ID]
#            ,[Source_Citation]
#            ,[Accessed_Date]
#            ,[Vintage]
#            ,[Revised_Data_Period]
#            ,[New_Data_Period]
#            ,[WNCD_Notes]
#            ,[Table_Note_ID]
#            ,[Table_Notes]
#            ,[Table_Line_Note_ID]
#            ,[Table_Line_Notes])
# SELECT [Measure_Business_Key]
# 		,[Record_Source]
#       ,Convert(Char(64), Hashbytes('SHA2_256',
# 	Upper(
# 		Ltrim(Rtrim([Measure_Business_Key]))
# 		)),2) as HashKey  
#       ,Convert(Char(64), Hashbytes('SHA2_256',
# 	Upper(
# 		Ltrim(Rtrim(Measure_Authority))
# 		+ '|' + Ltrim(Rtrim(TableID))
# 		+ '|' + Ltrim(Rtrim(Table_LineCode))
# 		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
# 		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
# 		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
# 		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
# 		)),2) as HashDiff
#       ,[Measure_Authority]
#       ,[TableID]
#       ,[Table_LineCode]
#       ,[MeasureGroupName]
#       ,[MeasureName]
#       ,[MeasureCategory]
#       ,[Observation_Frequency]
#       ,[UOMCode]
#       ,[UOMName]
#       ,[DefaultScale]
#       ,[CalculationType]
#       ,[MetricHeirarchyLevel]
#       ,[ParticipatesIn]
#       ,[NAICS_IndustryCodeStr]
#       ,[BEA_Industry_ID]
#       ,[BEA_GDP_Component_ID]
#       ,[Source_Citation]
#       ,[Accessed_Date]
#       ,[Vintage]
#       ,[Revised_Data_Period]
#       ,[New_Data_Period]
#       ,[WNCD_Notes]
#       ,[Table_Note_ID]
#       ,[Table_Notes]
#       ,[Table_Line_Note_ID]
#       ,[Table_Line_Notes]
#   FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];
# 
# /*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/
# 
# 
# 
# Select	M.[Measure_HashKey]
# 	into #NewKeys
# From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
# 	Left outer join [DV2].[dbo].[Hub_Measure] H
# 	on M.[Measure_HashKey] = H.[Measure_HashKey]
# 	Where H.[Record_Source] is NULL;
# 
# --	Select * from #NewKeys
# 
# /*	Register new Measure keys with the Measure Hub 	*/
# INSERT INTO [DV2].[dbo].[Hub_Measure]
#            ([Measure_HashKey]
#            ,[Measure_Business_Key]
#            ,[Load_Date]
#            ,[Record_Source]
# 		   )
#    Select	M.[Measure_HashKey]
# 			,M.[Measure_Business_Key]
# 			,CURRENT_TIMESTAMP
# 			,M.[Record_Source]
# 	FROM #NewKeys N
# 	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
# 	on N.[Measure_HashKey] = M.[Measure_HashKey]
# 	;
# 
# /*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
# DROP TABLE IF EXISTS #NewKeys1;
# 
# Select	M.[Measure_HashKey]
# 	into #NewKeys1
# From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
# 	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
# 	on M.[Measure_HashKey] = D.[Measure_HashKey]
# 	and D.[Load_Date] <= CURRENT_TIMESTAMP
# 	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
# 	where D.[Record_Source] is NULL;
# 
# --	Select * from #NewKeys1;
# 
# /*	Identify existing Measure descriptions that will be replaced by new descriptions	*/
# 
# Select	M.[Measure_HashKey]
# 		,E.[Load_Date]
# 	into #UpdtKeys
# From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
# 	inner join [DV2].[dbo].[Sat_Measure_Description] E
# 	on M.[Measure_HashKey] = E.[Measure_HashKey]
# 	and E.[Load_Date] <= CURRENT_TIMESTAMP
# 	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
# 	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];
# 
# --	Select * from #UpdtKeys;
# 
# /*	Add New Measure Descriptions to the Measure Description Satellite	*/
# 
# INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
#            ([Measure_HashKey]
#            ,[Load_Date]
#            ,[Load_End_Date]
#            ,[Record_Source]
#            ,[Measure_Business_Key]
#            ,[Measure_HashDiff]
#            ,[Measure_Authority]
#            ,[Measure_TableID]
#            ,[Measure_Table_Line_Number]
#            ,[Measure_Group_Name]
#            ,[Measure_Name]
#            ,[Measure_Category]
#            ,[Observation_Frequency]
#            ,[Unit_of_Measure_Code]
#            ,[Unit_of_Measure_Name]
#            ,[Default_Scale]
#            ,[Calulation_Type]
#            ,[Measure_Hierarchy_Level]
#            ,[Participates_In]
#            ,[NAICS_Industry_Code_Str]
#            ,[BEA_Industry_ID]
#            ,[BEA_GDP_Component_ID]
#            ,[Source_Citation]
#            ,[Accessed_Date]
#            ,[Vintage]
#            ,[Revised_Data_Period]
#            ,[New_Data_Period]
#            ,[WNCD_Notes]
#            ,[Table_Note_ID]
#            ,[Table_Notes]
#            ,[Table_Line_Note_ID]
#            ,[Table_Line_Notes])
# SELECT M.[Measure_HashKey]
# 		,CURRENT_TIMESTAMP
# 		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
#       ,[Record_Source]
#       ,[Measure_Business_Key]
#       ,[Measure_HashDiff]
#       ,[Measure_Authority]
#       ,[TableID]
#       ,[Table_LineCode]
#       ,[MeasureGroupName]
#       ,[MeasureName]
#       ,[MeasureCategory]
#       ,[Observation_Frequency]
#       ,[UOMCode]
#       ,[UOMName]
#       ,[DefaultScale]
#       ,[CalculationType]
#       ,[MetricHeirarchyLevel]
#       ,[ParticipatesIn]
#       ,[NAICS_IndustryCodeStr]
#       ,[BEA_Industry_ID]
#       ,[BEA_GDP_Component_ID]
#       ,[Source_Citation]
#       ,[Accessed_Date]
#       ,[Vintage]
#       ,[Revised_Data_Period]
#       ,[New_Data_Period]
#       ,[WNCD_Notes]
#       ,[Table_Note_ID]
#       ,[Table_Notes]
#       ,[Table_Line_Note_ID]
#       ,[Table_Line_Notes]
#   FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
#   inner join #NewKeys1 N
#   on M.[Measure_HashKey] = N.[Measure_HashKey];
#   
#   /*	Insert Replacement entries for those being retired	*/
# 
# Declare @Load_Date		Datetime2(7);
# Set @Load_Date = CURRENT_TIMESTAMP;
# 
# INSERT INTO [dbo].[Sat_Measure_Description]
#            ([Measure_HashKey]
#            ,[Load_Date]
#            ,[Load_End_Date]
#            ,[Record_Source]
#            ,[Measure_Business_Key]
#            ,[Measure_HashDiff]
#            ,[Measure_Authority]
#            ,[Measure_TableID]
#            ,[Measure_Table_Line_Number]
#            ,[Measure_Group_Name]
#            ,[Measure_Name]
#            ,[Measure_Category]
#            ,[Observation_Frequency]
#            ,[Unit_of_Measure_Code]
#            ,[Unit_of_Measure_Name]
#            ,[Default_Scale]
#            ,[Calulation_Type]
#            ,[Measure_Hierarchy_Level]
#            ,[Participates_In]
#            ,[NAICS_Industry_Code_Str]
#            ,[BEA_Industry_ID]
#            ,[BEA_GDP_Component_ID]
#            ,[Source_Citation]
#            ,[Accessed_Date]
#            ,[Vintage]
#            ,[Revised_Data_Period]
#            ,[New_Data_Period]
#            ,[WNCD_Notes]
#            ,[Table_Note_ID]
#            ,[Table_Notes]
#            ,[Table_Line_Note_ID]
#            ,[Table_Line_Notes])
# SELECT M.[Measure_HashKey]
# 		,@Load_Date
# 		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
#       ,[Record_Source]
#       ,[Measure_Business_Key]
#       ,[Measure_HashDiff]
#       ,[Measure_Authority]
#       ,[TableID]
#       ,[Table_LineCode]
#       ,[MeasureGroupName]
#       ,[MeasureName]
#       ,[MeasureCategory]
#       ,[Observation_Frequency]
#       ,[UOMCode]
#       ,[UOMName]
#       ,[DefaultScale]
#       ,[CalculationType]
#       ,[MetricHeirarchyLevel]
#       ,[ParticipatesIn]
#       ,[NAICS_IndustryCodeStr]
#       ,[BEA_Industry_ID]
#       ,[BEA_GDP_Component_ID]
#       ,[Source_Citation]
#       ,[Accessed_Date]
#       ,[Vintage]
#       ,[Revised_Data_Period]
#       ,[New_Data_Period]
#       ,[WNCD_Notes]
#       ,[Table_Note_ID]
#       ,[Table_Notes]
#       ,[Table_Line_Note_ID]
#       ,[Table_Line_Notes]
#   FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
#   inner join #UpdtKeys N
#   on M.[Measure_HashKey] = N.[Measure_HashKey];
# 
# /*	Retire the entries in the Measure Description satellite that have been replaced.	*/
# /*	End-Date entries being retired.													 	*/
# 
# UPDATE [DV2].[dbo].[Sat_Measure_Description] 
#    SET [Load_End_Date] =  @Load_Date
#    From #UpdtKeys K
# 	,[DV2].[dbo].[Sat_Measure_Description] T
#  WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
#  and T.[Load_Date] = k.[Load_Date];""")

# c.execute("""/**************************************************************************/
# /***																	***/
# /***				PopulateDV2_Measure_Tables.sql						***/
# /***																	***/
# /***	The script adds entries to:										***/
# /***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
# /***				-	[DV2].[dbo].[Hub_Measure]						***/
# /***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
# /***	and updates the Load End Date for entries retiring in			***/
# /***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
# /***	All input come from:											***/
# /***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
# /***	This is the only place where HashKeys are calculated for 		***/
# /***	Measures														***/
# /***																	***/
# /**************************************************************************/
# 
# USE DV2;
# 
# 
# TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];
# 
# INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
#            ([Measure_Business_Key]
# 		   ,[Record_Source]
#            ,[Measure_HashKey]
#            ,[Measure_HashDiff]
#            ,[Measure_Authority]
#            ,[TableID]
#            ,[Table_LineCode]
#            ,[MeasureGroupName]
#            ,[MeasureName]
#            ,[MeasureCategory]
#            ,[Observation_Frequency]
#            ,[UOMCode]
#            ,[UOMName]
#            ,[DefaultScale]
#            ,[CalculationType]
#            ,[MetricHeirarchyLevel]
#            ,[ParticipatesIn]
#            ,[NAICS_IndustryCodeStr]
#            ,[BEA_Industry_ID]
#            ,[BEA_GDP_Component_ID]
#            ,[Source_Citation]
#            ,[Accessed_Date]
#            ,[Vintage]
#            ,[Revised_Data_Period]
#            ,[New_Data_Period]
#            ,[WNCD_Notes]
#            ,[Table_Note_ID]
#            ,[Table_Notes]
#            ,[Table_Line_Note_ID]
#            ,[Table_Line_Notes])
# SELECT [Measure_Business_Key]
# 		,[Record_Source]
#       ,Convert(Char(64), Hashbytes('SHA2_256',
# 	Upper(
# 		Ltrim(Rtrim([Measure_Business_Key]))
# 		)),2) as HashKey  
#       ,Convert(Char(64), Hashbytes('SHA2_256',
# 	Upper(
# 		Ltrim(Rtrim(Measure_Authority))
# 		+ '|' + Ltrim(Rtrim(TableID))
# 		+ '|' + Ltrim(Rtrim(Table_LineCode))
# 		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
# 		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
# 		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
# 		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
# 		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
# 		)),2) as HashDiff
#       ,[Measure_Authority]
#       ,[TableID]
#       ,[Table_LineCode]
#       ,[MeasureGroupName]
#       ,[MeasureName]
#       ,[MeasureCategory]
#       ,[Observation_Frequency]
#       ,[UOMCode]
#       ,[UOMName]
#       ,[DefaultScale]
#       ,[CalculationType]
#       ,[MetricHeirarchyLevel]
#       ,[ParticipatesIn]
#       ,[NAICS_IndustryCodeStr]
#       ,[BEA_Industry_ID]
#       ,[BEA_GDP_Component_ID]
#       ,[Source_Citation]
#       ,[Accessed_Date]
#       ,[Vintage]
#       ,[Revised_Data_Period]
#       ,[New_Data_Period]
#       ,[WNCD_Notes]
#       ,[Table_Note_ID]
#       ,[Table_Notes]
#       ,[Table_Line_Note_ID]
#       ,[Table_Line_Notes]
#   FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];
# 
# /*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/
# 
# 
# 
# Select	M.[Measure_HashKey]
# 	into #NewKeys
# From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
# 	Left outer join [DV2].[dbo].[Hub_Measure] H
# 	on M.[Measure_HashKey] = H.[Measure_HashKey]
# 	Where H.[Record_Source] is NULL;
# 
# --	Select * from #NewKeys
# 
# /*	Register new Measure keys with the Measure Hub 	*/
# INSERT INTO [DV2].[dbo].[Hub_Measure]
#            ([Measure_HashKey]
#            ,[Measure_Business_Key]
#            ,[Load_Date]
#            ,[Record_Source]
# 		   )
#    Select	M.[Measure_HashKey]
# 			,M.[Measure_Business_Key]
# 			,CURRENT_TIMESTAMP
# 			,M.[Record_Source]
# 	FROM #NewKeys N
# 	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
# 	on N.[Measure_HashKey] = M.[Measure_HashKey]
# 	;
# 
# /*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
# DROP TABLE IF EXISTS #NewKeys1;
# 
# Select	M.[Measure_HashKey]
# 	into #NewKeys1
# From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
# 	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
# 	on M.[Measure_HashKey] = D.[Measure_HashKey]
# 	and D.[Load_Date] <= CURRENT_TIMESTAMP
# 	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
# 	where D.[Record_Source] is NULL;
# 
# --	Select * from #NewKeys1;
# 
# /*	Identify existing Measure descriptions that will be replaced by new descriptions	*/
# 
# Select	M.[Measure_HashKey]
# 		,E.[Load_Date]
# 	into #UpdtKeys
# From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
# 	inner join [DV2].[dbo].[Sat_Measure_Description] E
# 	on M.[Measure_HashKey] = E.[Measure_HashKey]
# 	and E.[Load_Date] <= CURRENT_TIMESTAMP
# 	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
# 	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];
# 
# --	Select * from #UpdtKeys;
# 
# /*	Add New Measure Descriptions to the Measure Description Satellite	*/
# 
# INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
#            ([Measure_HashKey]
#            ,[Load_Date]
#            ,[Load_End_Date]
#            ,[Record_Source]
#            ,[Measure_Business_Key]
#            ,[Measure_HashDiff]
#            ,[Measure_Authority]
#            ,[Measure_TableID]
#            ,[Measure_Table_Line_Number]
#            ,[Measure_Group_Name]
#            ,[Measure_Name]
#            ,[Measure_Category]
#            ,[Observation_Frequency]
#            ,[Unit_of_Measure_Code]
#            ,[Unit_of_Measure_Name]
#            ,[Default_Scale]
#            ,[Calulation_Type]
#            ,[Measure_Hierarchy_Level]
#            ,[Participates_In]
#            ,[NAICS_Industry_Code_Str]
#            ,[BEA_Industry_ID]
#            ,[BEA_GDP_Component_ID]
#            ,[Source_Citation]
#            ,[Accessed_Date]
#            ,[Vintage]
#            ,[Revised_Data_Period]
#            ,[New_Data_Period]
#            ,[WNCD_Notes]
#            ,[Table_Note_ID]
#            ,[Table_Notes]
#            ,[Table_Line_Note_ID]
#            ,[Table_Line_Notes])
# SELECT M.[Measure_HashKey]
# 		,CURRENT_TIMESTAMP
# 		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
#       ,[Record_Source]
#       ,[Measure_Business_Key]
#       ,[Measure_HashDiff]
#       ,[Measure_Authority]
#       ,[TableID]
#       ,[Table_LineCode]
#       ,[MeasureGroupName]
#       ,[MeasureName]
#       ,[MeasureCategory]
#       ,[Observation_Frequency]
#       ,[UOMCode]
#       ,[UOMName]
#       ,[DefaultScale]
#       ,[CalculationType]
#       ,[MetricHeirarchyLevel]
#       ,[ParticipatesIn]
#       ,[NAICS_IndustryCodeStr]
#       ,[BEA_Industry_ID]
#       ,[BEA_GDP_Component_ID]
#       ,[Source_Citation]
#       ,[Accessed_Date]
#       ,[Vintage]
#       ,[Revised_Data_Period]
#       ,[New_Data_Period]
#       ,[WNCD_Notes]
#       ,[Table_Note_ID]
#       ,[Table_Notes]
#       ,[Table_Line_Note_ID]
#       ,[Table_Line_Notes]
#   FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
#   inner join #NewKeys1 N
#   on M.[Measure_HashKey] = N.[Measure_HashKey];
#   
#   /*	Insert Replacement entries for those being retired	*/
# 
# Declare @Load_Date		Datetime2(7);
# Set @Load_Date = CURRENT_TIMESTAMP;
# 
# INSERT INTO [dbo].[Sat_Measure_Description]
#            ([Measure_HashKey]
#            ,[Load_Date]
#            ,[Load_End_Date]
#            ,[Record_Source]
#            ,[Measure_Business_Key]
#            ,[Measure_HashDiff]
#            ,[Measure_Authority]
#            ,[Measure_TableID]
#            ,[Measure_Table_Line_Number]
#            ,[Measure_Group_Name]
#            ,[Measure_Name]
#            ,[Measure_Category]
#            ,[Observation_Frequency]
#            ,[Unit_of_Measure_Code]
#            ,[Unit_of_Measure_Name]
#            ,[Default_Scale]
#            ,[Calulation_Type]
#            ,[Measure_Hierarchy_Level]
#            ,[Participates_In]
#            ,[NAICS_Industry_Code_Str]
#            ,[BEA_Industry_ID]
#            ,[BEA_GDP_Component_ID]
#            ,[Source_Citation]
#            ,[Accessed_Date]
#            ,[Vintage]
#            ,[Revised_Data_Period]
#            ,[New_Data_Period]
#            ,[WNCD_Notes]
#            ,[Table_Note_ID]
#            ,[Table_Notes]
#            ,[Table_Line_Note_ID]
#            ,[Table_Line_Notes])
# SELECT M.[Measure_HashKey]
# 		,@Load_Date
# 		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
#       ,[Record_Source]
#       ,[Measure_Business_Key]
#       ,[Measure_HashDiff]
#       ,[Measure_Authority]
#       ,[TableID]
#       ,[Table_LineCode]
#       ,[MeasureGroupName]
#       ,[MeasureName]
#       ,[MeasureCategory]
#       ,[Observation_Frequency]
#       ,[UOMCode]
#       ,[UOMName]
#       ,[DefaultScale]
#       ,[CalculationType]
#       ,[MetricHeirarchyLevel]
#       ,[ParticipatesIn]
#       ,[NAICS_IndustryCodeStr]
#       ,[BEA_Industry_ID]
#       ,[BEA_GDP_Component_ID]
#       ,[Source_Citation]
#       ,[Accessed_Date]
#       ,[Vintage]
#       ,[Revised_Data_Period]
#       ,[New_Data_Period]
#       ,[WNCD_Notes]
#       ,[Table_Note_ID]
#       ,[Table_Notes]
#       ,[Table_Line_Note_ID]
#       ,[Table_Line_Notes]
#   FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
#   inner join #UpdtKeys N
#   on M.[Measure_HashKey] = N.[Measure_HashKey];
# 
# /*	Retire the entries in the Measure Description satellite that have been replaced.	*/
# /*	End-Date entries being retired.													 	*/
# 
# UPDATE [DV2].[dbo].[Sat_Measure_Description] 
#    SET [Load_End_Date] =  @Load_Date
#    From #UpdtKeys K
# 	,[DV2].[dbo].[Sat_Measure_Description] T
#  WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
#  and T.[Load_Date] = k.[Load_Date];""")
