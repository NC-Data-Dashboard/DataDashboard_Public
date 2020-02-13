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


# Load BEA CAINC6N_NC data
response = requests.get('https://apps.bea.gov/regional/zip/CAINC6N.zip')
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")


# In[ ]:


# Check for unused fields
df.tail(10)


# In[ ]:


# Remove unused fields
df.drop(df.tail(4).index,inplace=True)


# In[ ]:


#Clean GeoFIPS
df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})


# In[ ]:


# Set GeoFIPS as Index
df.set_index(df['GeoFIPS'], inplace = True)


# In[ ]:


# Drop GeoFIPS column
df.drop('GeoFIPS', axis = 1, inplace = True)


# In[ ]:


#Connect to database and create cursor
con = pyodbc.connect('Driver={SQL Server};'
                      'Server=STEIN\ECONDEV;'
                      'Database=STG2;'
                      'Trusted_Connection=yes;',
                    autocommit=True)

c = con.cursor()


# # Create Compensation of Employees

# In[ ]:


print('Updating Compensation of Employees...')


# In[ ]:


# Create Backups
df_comp_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Compensation_of_Employees.txt', encoding = 'ISO-8859-1', sep='\t')
df_comp_backup.to_csv('./Backups/STG_BEA_CA6N_Compensation_of_Employees_BACKUP.txt')


# In[ ]:


# Create new dataframe for Per capita Information
filter1 = df['LineCode'] == 1
df_compensation = df[filter1]
df_compensation.head()


# In[ ]:


# Clean Description column
df_compensation.loc[:,'Description'] = df_compensation['Description'].str.strip('1/')


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_compensation.to_csv('./Updates/STG_BEA_CA6N_Compensation_of_Employees.txt', sep = '\t')


# In[ ]:


# Reset the index
df_compensation = df_compensation.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_compensation.columns.values
for i in column_list:
    df_compensation.loc[df_compensation[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Compensation_of_Employees_BACKUP')


# In[ ]:


# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Compensation_of_Employees','STG_BEA_CA6N_Compensation_of_Employees_BACKUP';''')


# In[ ]:


# Create Per Capita table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Compensation_of_Employees](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_compensation.to_sql('STG_BEA_CA6N_Compensation_of_Employees', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Compensation_of_Employees';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0001';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Create Wages and Salaries

# In[ ]:


print('Done. Updating Wages and Salaries...')


# In[ ]:


# Create Backups
df_w_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Wages_and_Salaries.txt', encoding = 'ISO-8859-1', sep='\t')
df_w_backup.to_csv('./Backups/STG_BEA_CA6N_Wages_and_Salaries_BACKUP.txt')


# In[ ]:


# Create a new dataframe for Earnings by place of work
filter1 = df['LineCode'] == 5
df_wages = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_wages.to_csv('./Updates/STG_BEA_CA6N_Wages_and_Salaries.txt', sep = '\t')


# In[ ]:


# Reset the index
df_wages = df_wages.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_wages.columns.values
for i in column_list:
    df_wages.loc[df_wages[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Wages_and_Salaries_BACKUP')


# In[ ]:


# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Wages_and_Salaries','STG_BEA_CA6N_Wages_and_Salaries_BACKUP';''')


# In[ ]:


# Create Earnings table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Wages_and_Salaries](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_wages.to_sql('STG_BEA_CA6N_Wages_and_Salaries', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Wages_and_Salaries';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0005';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Create Health Care and Social Assistance

# In[ ]:


print('Done. Updating Health Care and Social Assistance...')


# In[ ]:


# Create Backups
df_h_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Health_Care_and_Social_Assistance.txt', encoding = 'ISO-8859-1', sep='\t')
df_h_backup.to_csv('./Backups/STG_BEA_CA6N_Health_Care_and_Social_Assistance_BACKUP.txt')


# In[ ]:


# Create a new dataframe for Health_Care_and_Social_Assistance
filter1 = df['LineCode'] == 1600
df_health = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_health.to_csv('./Updates/STG_BEA_CA6N_Health_Care_and_Social_Assistance.txt', sep = '\t')


# In[ ]:


# Reset the index
df_health = df_health.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_health.columns.values
for i in column_list:
    df_health.loc[df_health[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Health_Care_and_Social_Assistance_BACKUP')


# In[ ]:


# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Health_Care_and_Social_Assistance','STG_BEA_CA6N_Health_Care_and_Social_Assistance_BACKUP';''')


# In[ ]:


# Create Health_Care_and_Social_Assistance table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Health_Care_and_Social_Assistance](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_health.to_sql('STG_BEA_CA6N_Health_Care_and_Social_Assistance', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Health_Care_and_Social_Assistance';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1600';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Create Information

# In[ ]:


print('Done. Updating Information..')


# In[ ]:


# Create Backups
df_i_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Information.txt', encoding = 'ISO-8859-1', sep='\t')
df_i_backup.to_csv('./Backups/STG_BEA_CA6N_Information_BACKUP.txt')


# In[ ]:


# Create new dataframe for Information
filter1 = df['LineCode'] == 900
df_info = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_info.to_csv('./Updates/STG_BEA_CA6N_Information.txt', sep = '\t')


# In[ ]:


# Reset the index
df_info = df_info.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_info.columns.values
for i in column_list:
    df_info.loc[df_info[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Information_BACKUP')


# In[ ]:


# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Information','STG_BEA_CA6N_Information_BACKUP';''')


# In[ ]:


# Create Information Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Information](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_info.to_sql('STG_BEA_CA6N_Information', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Information';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0900';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Create Management of Companies and Enterprises

# In[ ]:


print('Done. Updating Management of Companies and Enterprises..')

# Create Backups
df_mang_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Management_of_Companies_and_Enterprises.txt', encoding = 'ISO-8859-1', sep='\t')
df_mang_backup.to_csv('./Backups/STG_BEA_CA6N_Management_of_Companies_and_Enterprises_BACKUP.txt')

# Create new dataframe for Information
filter1 = df['LineCode'] == 1300
df_management = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_management.to_csv('./Updates/STG_BEA_CA6N_Management_of_Companies_and_Enterprises.txt', sep = '\t')

# Reset the index
df_management = df_management.reset_index()

# Fill NaN values for upload to database
column_list = df_management.columns.values
for i in column_list:
    df_management.loc[df_management[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Management_of_Companies_and_Enterprises_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Management_of_Companies_and_Enterprises','STG_BEA_CA6N_Management_of_Companies_and_Enterprises_BACKUP';''')

# Create Information Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Management_of_Companies_and_Enterprises](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=TfITANIUM-BOOK;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_management.to_sql('STG_BEA_CA6N_Management_of_Companies_and_Enterprises', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Management_of_Companies_and_Enterprises';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1300';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Manufacturing

# In[ ]:


print('Done. Updating Manufacturing..')

# Create Backups
df_manu_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Manufacturing.txt', encoding = 'ISO-8859-1', sep='\t')
df_manu_backup.to_csv('./Backups/STG_BEA_CA6N_Manufacturing_BACKUP.txt')

# Create new dataframe for Manufacturing
filter1 = df['LineCode'] == 500
df_manufacturing = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_manufacturing.to_csv('./Updates/STG_BEA_CA6N_Manufacturing.txt', sep = '\t')

# Reset the indexf
df_manufacturing = df_manufacturing.reset_index()

# Fill NaN values for upload to database
column_list = df_manufacturing.columns.values
for i in column_list:
    df_manufacturing.loc[df_manufacturing[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Manufacturing_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Manufacturing','STG_BEA_CA6N_Manufacturing_BACKUP';''')

# Create Manufacturing Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Manufacturing](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_manufacturing.to_sql('STG_BEA_CA6N_Manufacturing', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Manufacturing';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0500';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Mining, Quarrying, and Oil and Gas Production

# In[ ]:


print('Done. Updating Mining, Quarrying, and Oil and Gas Production..')

# Create Backups
df_min_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt', encoding = 'ISO-8859-1', sep='\t')
df_min_backup.to_csv('./Backups/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP.txt')

# Create new dataframe for Mining_Quarrying_and_Oil_and_Gas_Extraction
filter1 = df['LineCode'] == 200
df_mining = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_mining.to_csv('./Updates/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt', sep = '\t')

# Reset the index
df_mining = df_mining.reset_index()

# Fill NaN values for upload to database
column_list = df_mining.columns.values
for i in column_list:
    df_mining.loc[df_mining[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction','STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP';''')

# Create Mining_Quarrying_and_Oil_and_Gas_Extraction Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_mining.to_sql('STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0200';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Other Services

# In[ ]:


print('Done. Updating Other Services..')

# Create Backups
df_ser_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Other_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_ser_backup.to_csv('./Backups/STG_BEA_CA6N_Other_Services_BACKUP.txt')

# Create new dataframe for Other_Services
filter1 = df['LineCode'] == 1900
df_services = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_services.to_csv('./Updates/STG_BEA_CA6N_Other_Services.txt', sep = '\t')

# Reset the index
df_services = df_services.reset_index()

# Fill NaN values for upload to database
column_list = df_services.columns.values
for i in column_list:
    df_services.loc[df_services[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Other_Services_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Other_Services','STG_BEA_CA6N_Other_Services_BACKUP';''')

# Create Other_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Other_Services](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_services.to_sql('STG_BEA_CA6N_Other_Services', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2; 

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Other_Services';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1900';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Professional, Scientific, and Technical Services

# In[ ]:


print('Done. Updating Professional Scientific and Technical Services..')

# Create Backups
df_pst_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_pst_backup.to_csv('./Backups/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services_BACKUP.txt')

# Create new dataframe for Professional_Scientific_and_Technical_Services
filter1 = df['LineCode'] == 1200
df_professional = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_professional.to_csv('./Updates/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services.txt', sep = '\t')

# Reset the index
df_professional = df_professional.reset_index()

# Fill NaN values for upload to database
column_list = df_professional.columns.values
for i in column_list:
    df_professional.loc[df_professional[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Professional_Scientific_and_Technical_Services_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Professional_Scientific_and_Technical_Services','STG_BEA_CA6N_Professional_Scientific_and_Technical_Services_BACKUP';''')

# Create Professional_Scientific_and_Technical_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Professional_Scientific_and_Technical_Services](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_professional.to_sql('STG_BEA_CA6N_Professional_Scientific_and_Technical_Services', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Professional_Scientific_and_Technical_Services';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1200';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Real Estate and Rental Housing

# In[ ]:


print('Done. Updating Real Estate and Rental Housing..')

# Create Backups
df_hou_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing.txt', encoding = 'ISO-8859-1', sep='\t')
df_hou_backup.to_csv('./Backups/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing_BACKUP.txt')

# Create new dataframe for Real_Estate_and_Rental_and_Leasing
filter1 = df['LineCode'] == 1100
df_realestate = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_realestate.to_csv('./Updates/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing.txt', sep = '\t')

# Reset the index
df_realestate = df_realestate.reset_index()

# Fill NaN values for upload to database
column_list = df_realestate.columns.values
for i in column_list:
    df_realestate.loc[df_realestate[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing','STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing_BACKUP';''')

# Create Real_Estate_and_Rental_and_Leasing Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_realestate.to_sql('STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1100';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Retail Trade

# In[ ]:


print('Done. Updating Retail Trade..')

# Create Backups
df_r_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Retail_Trade.txt', encoding = 'ISO-8859-1', sep='\t')
df_r_backup.to_csv('./Backups/STG_BEA_CA6N_Retail_Trade_BACKUP.txt')

# Create new dataframe for Retail_Trade
filter1 = df['LineCode'] == 700
df_retail = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_retail.to_csv('./Updates/STG_BEA_CA6N_Retail_Trade.txt', sep = '\t')

# Reset the index
df_retail = df_retail.reset_index()

# Fill NaN values for upload to database
column_list = df_retail.columns.values
for i in column_list:
    df_retail.loc[df_retail[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Retail_Trade_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Retail_Trade','STG_BEA_CA6N_Retail_Trade_BACKUP';''')

# Create Retail_Trade Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Retail_Trade](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_retail.to_sql('STG_BEA_CA6N_Retail_Trade', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Retail_Trade';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0700';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:





# # Transportation and Warehousing

# In[ ]:


print('Done. Updating Transportation and Warehousing..')

# Create Backups
df_t_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Transportation_and_Warehousing.txt', encoding = 'ISO-8859-1', sep='\t')
df_t_backup.to_csv('./Backups/STG_BEA_CA6N_Transportation_and_Warehousing_BACKUP.txt')

# Create new dataframe for Transportation_and_Warehousing
filter1 = df['LineCode'] == 800
df_transportation = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_transportation.to_csv('./Updates/STG_BEA_CA6N_Transportation_and_Warehousing.txt', sep = '\t')

# Reset the index
df_transportation = df_transportation.reset_index()

# Fill NaN values for upload to database
column_list = df_transportation.columns.values
for i in column_list:
    df_transportation.loc[df_transportation[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Transportation_and_Warehousing_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Transportation_and_Warehousing','STG_BEA_CA6N_Transportation_and_Warehousing_BACKUP';''')

# Create Transportation_and_Warehousing Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Transportation_and_Warehousing](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_transportation.to_sql('STG_BEA_CA6N_Transportation_and_Warehousing', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Transportation_and_Warehousing';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0800';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Utilities

# In[ ]:


print('Done. Updating Utilities..')

# Create Backups
df_u_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Utilities.txt', encoding = 'ISO-8859-1', sep='\t')
df_u_backup.to_csv('./Backups/STG_BEA_CA6N_Utilities_BACKUP.txt')

# Create new dataframe for Utilities
filter1 = df['LineCode'] == 300
df_utilities = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_utilities.to_csv('./Updates/STG_BEA_CA6N_Utilities.txt', sep = '\t')

# Reset the index
df_utilities = df_utilities.reset_index()

# Fill NaN values for upload to database
column_list = df_utilities.columns.values
for i in column_list:
    df_utilities.loc[df_utilities[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Utilities_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Utilities','STG_BEA_CA6N_Utilities_BACKUP';''')

# Create Utilities Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Utilities](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_utilities.to_sql('STG_BEA_CA6N_Utilities', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Utilities';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0300';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Wholesale Trade

# In[ ]:


print('Done. Updating Wholesale Trade..')

# Create Backups
df_wt_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Wholesale_Trade.txt', encoding = 'ISO-8859-1', sep='\t')
df_wt_backup.to_csv('./Backups/STG_BEA_CA6N_Wholesale_Trade_BACKUP.txt')

# Create new dataframe for Wholesale_Trade
filter1 = df['LineCode'] == 600
df_wholesale = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_wholesale.to_csv('./Updates/STG_BEA_CA6N_Wholesale_Trade.txt', sep = '\t')

# Reset the index
df_wholesale = df_wholesale.reset_index()

# Fill NaN values for upload to database
column_list = df_wholesale.columns.values
for i in column_list:
    df_wholesale.loc[df_wholesale[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Wholesale_Trade_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Wholesale_Trade','STG_BEA_CA6N_Wholesale_Trade_BACKUP';''')

# Create Wholesale_Trade Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Wholesale_Trade](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_wholesale.to_sql('STG_BEA_CA6N_Wholesale_Trade', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Wholesale_Trade';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0600';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Employer Contributions for Employee Pension and Insurance Funds

# In[ ]:


print('Done. Updating Employer Contributions for Employee Pension and Insurance Funds..')

# Create Backups
df_p_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds.txt', encoding = 'ISO-8859-1', sep='\t')
df_p_backup.to_csv('./Backups/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds_BACKUP.txt')

# Create new dataframe for Employer_Contributions_for_Employee_Pension_and_Insurance_Funds
filter1 = df['LineCode'] == 7
df_pension = df[filter1]

# Clean Description column
df_pension.loc[:,'Description'] = df_pension['Description'].str.strip('2/')

# Save as tab-delimited txt file for export to SSMS
df_pension.to_csv('./Updates/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds.txt', sep = '\t')

# Reset the index
df_pension = df_pension.reset_index()

# Fill NaN values for upload to database
column_list = df_pension.columns.values
for i in column_list:
    df_pension.loc[df_pension[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds','STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds_BACKUP';''')

# Create Employer_Contributions_for_Employee_Pension_and_Insurance_Funds Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_pension.to_sql('STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0007';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Employer Contributions for Government Social Insurance

# In[ ]:


print('Done. Updating Employer Contributions for Government Social Insurance..')

# Create Backups
df_si_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance.txt', encoding = 'ISO-8859-1', sep='\t')
df_si_backup.to_csv('./Backups/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance_BACKUP.txt')

# Create new dataframe for Employer_Contributions_for_Government_Social_Insurance
filter1 = df['LineCode'] == 8
df_social = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_social.to_csv('./Updates/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance.txt', sep = '\t')

# Reset the index
df_social = df_social.reset_index()

# Fill NaN values for upload to database
column_list = df_social.columns.values
for i in column_list:
    df_social.loc[df_social[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance','STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance_BACKUP';''')

# Create Employer_Contributions_for_Government_Social_Insurance Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_social.to_sql('STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0008';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Government and Government Enterprises

# In[ ]:


print('Done. Updating Government and Government Enterprises..')

# Create Backups
df_g_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Government_and_Government_Enterprises.txt', encoding = 'ISO-8859-1', sep='\t')
df_g_backup.to_csv('./Backups/STG_BEA_CA6N_Government_and_Government_Enterprises_BACKUP.txt')

# Create new dataframe for Government_and_Government_Enterprises
filter1 = df['LineCode'] == 2000
df_gov = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_gov.to_csv('./Updates/STG_BEA_CA6N_Government_and_Government_Enterprises.txt', sep = '\t')

# Reset the index
df_gov = df_gov.reset_index()

# Fill NaN values for upload to database
column_list = df_gov.columns.values
for i in column_list:
    df_gov.loc[df_gov[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Government_and_Government_Enterprises_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Government_and_Government_Enterprises','STG_BEA_CA6N_Government_and_Government_Enterprises_BACKUP';''')

# Create Government_and_Government_Enterprises Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Government_and_Government_Enterprises](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_gov.to_sql('STG_BEA_CA6N_Government_and_Government_Enterprises', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2; 

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Government_and_Government_Enterprises';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_2000';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Private Nonfarm Compensation

# In[ ]:


print('Done. Updating Private Nonfarm Compensation..')

# Create Backups
df_pnc_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Private_Nonfarm_Compensation.txt', encoding = 'ISO-8859-1', sep='\t')
df_pnc_backup.to_csv('./Backups/STG_BEA_CA6N_Private_Nonfarm_Compensation_BACKUP.txt')

# Create new dataframe for Private_Nonfarm_Compensation
filter1 = df['LineCode'] == 90
df_private = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_private.to_csv('./Updates/STG_BEA_CA6N_Private_Nonfarm_Compensation.txt', sep = '\t')

# Reset the index
df_private = df_private.reset_index()

# Fill NaN values for upload to database
column_list = df_private.columns.values
for i in column_list:
    df_private.loc[df_private[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Private_Nonfarm_Compensation_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Private_Nonfarm_Compensation','STG_BEA_CA6N_Private_Nonfarm_Compensation_BACKUP';''')

# Create Private_Nonfarm_Compensation Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Private_Nonfarm_Compensation](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_private.to_sql('STG_BEA_CA6N_Private_Nonfarm_Compensation', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;
 

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Private_Nonfarm_Compensation';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0090';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Farm Compensation

# In[ ]:


print('Done. Updating Farm Compensation..')

# Create Backups
df_fc_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Farm_Compensation.txt', encoding = 'ISO-8859-1', sep='\t')
df_fc_backup.to_csv('./Backups/STG_BEA_CA6N_Farm_Compensation_BACKUP.txt')

# Create new dataframe for Farm_Compensation
filter1 = df['LineCode'] == 81
df_farm = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_farm.to_csv('./Updates/STG_BEA_CA6N_Farm_Compensation.txt', sep = '\t')

# Reset the index
df_farm = df_farm.reset_index()

# Fill NaN values for upload to database
column_list = df_farm.columns.values
for i in column_list:
    df_farm.loc[df_farm[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Farm_Compensation_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Farm_Compensation','STG_BEA_CA6N_Farm_Compensation_BACKUP';''')

# Create Farm_Compensation Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Farm_Compensation](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_farm.to_sql('STG_BEA_CA6N_Farm_Compensation', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;
 

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Farm_Compensation';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0081';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Nonfarm Compensation

# In[ ]:


print('Done. Updating Nonfarm Compensation..')

# Create Backups
df_nf_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Nonfarm_Compensation.txt', encoding = 'ISO-8859-1', sep='\t')
df_nf_backup.to_csv('./Backups/STG_BEA_CA6N_Nonfarm_Compensation_BACKUP.txt')

# Create new dataframe for Nonfarm_Compensation
filter1 = df['LineCode'] == 82
df_nonfarm = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_nonfarm.to_csv('./Updates/STG_BEA_CA6N_Nonfarm_Compensation.txt', sep = '\t')

# Reset the index
df_nonfarm = df_nonfarm.reset_index()

# Fill NaN values for upload to database
column_list = df_nonfarm.columns.values
for i in column_list:
    df_nonfarm.loc[df_nonfarm[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Nonfarm_Compensation_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Nonfarm_Compensation','STG_BEA_CA6N_Nonfarm_Compensation_BACKUP';''')

# Create Nonfarm_Compensation Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Nonfarm_Compensation](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_nonfarm.to_sql('STG_BEA_CA6N_Nonfarm_Compensation', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;
 

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Nonfarm_Compensation';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0082';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Supplements to Wages and Salaries

# In[ ]:


print('Done. Updating Supplements to Wages and Salaries..')

# Create Backups
df_supp_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries.txt', encoding = 'ISO-8859-1', sep='\t')
df_supp_backup.to_csv('./Backups/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries_BACKUP.txt')

# Create new dataframe for Supplements_to_Wages_and_Salaries
filter1 = df['LineCode'] == 6
df_supplement = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_supplement.to_csv('./Updates/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries.txt', sep = '\t')

# Reset the index
df_supplement = df_supplement.reset_index()

# Fill NaN values for upload to database
column_list = df_supplement.columns.values
for i in column_list:
    df_supplement.loc[df_supplement[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Supplements_to_Wages_and_Salaries_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Supplements_to_Wages_and_Salaries','STG_BEA_CA6N_Supplements_to_Wages_and_Salaries_BACKUP';''')

# Create Supplements_to_Wages_and_Salaries Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Supplements_to_Wages_and_Salaries](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_supplement.to_sql('STG_BEA_CA6N_Supplements_to_Wages_and_Salaries', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;
 

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Supplements_to_Wages_and_Salaries';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0006';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Average Compensation Per Job

# In[ ]:


print('Done. Updating Average Compensation Per Job..')

# Create Backups
df_ac_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt', encoding = 'ISO-8859-1', sep='\t')
df_ac_backup.to_csv('./Backups/STG_BEA_CA6N_Average_Compensation_Per_Job_BACKUP.txt')

# Create new dataframe for Average_Compensation_Per_Job
filter1 = df['LineCode'] == 9
df_comp = df[filter1]

# Clean Description column
df_comp.loc[:,'Description'] = df_comp['Description'].str.strip('3/')

# Save as tab-delimited txt file for export to SSMS
df_comp.to_csv('./Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt', sep = '\t')

# Reset the index
df_comp = df_comp.reset_index()

# Fill NaN values for upload to database
column_list = df_comp.columns.values
for i in column_list:
    df_comp.loc[df_comp[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Average_Compensation_Per_Job_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Average_Compensation_Per_Job','STG_BEA_CA6N_Average_Compensation_Per_Job_BACKUP';''')

# Create Average_Compensation_Per_Job Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Average_Compensation_Per_Job](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_comp.to_sql('STG_BEA_CA6N_Average_Compensation_Per_Job', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Average_Compensation_Per_Job';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0009';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Accommodation and Food Services

# In[ ]:


print('Done. Updating Accommodation and Food Services..')

# Create Backups
df_acc_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Accommodation_and_Food_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_acc_backup.to_csv('./Backups/STG_BEA_CA6N_Accommodation_and_Food_Services_BACKUP.txt')

# Create new dataframe for Accommodation_and_Food_Services
filter1 = df['LineCode'] == 1800
df_food = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_food.to_csv('./Updates/STG_BEA_CA6N_Accommodation_and_Food_Services.txt', sep = '\t')

# Reset the index
df_food = df_food.reset_index()

# Fill NaN values for upload to database
column_list = df_food.columns.values
for i in column_list:
    df_food.loc[df_food[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Accommodation_and_Food_Services_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Accommodation_and_Food_Services','STG_BEA_CA6N_Accommodation_and_Food_Services_BACKUP';''')

# Create Accommodation_and_Food_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Accommodation_and_Food_Services](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_food.to_sql('STG_BEA_CA6N_Accommodation_and_Food_Services', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Accommodation_and_Food_Services';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1800';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Administrative Support

# In[ ]:


print('Done. Updating Administrative Support..')

# Create Backups
df_as_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_as_backup.to_csv('./Backups/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP.txt')

# Create new dataframe for Administrative_and_Support_and_Waste_Management_and_Remediation_Services
filter1 = df['LineCode'] == 1400
df_admin = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_admin.to_csv('./Updates/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', sep = '\t')

# Reset the index
df_admin = df_admin.reset_index()

# Fill NaN values for upload to database
column_list = df_admin.columns.values
for i in column_list:
    df_admin.loc[df_admin[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services','STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP';''')

# Create Administrative_and_Support_and_Waste_Management_and_Remediation_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_admin.to_sql('STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1400';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Arts, Entertainment, and Recreation

# In[ ]:


print('Done. Updating Arts, Entertainment, and Recreation..')

# Create Backups
df_aer_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Arts_Entertainment_and_Recreation.txt', encoding = 'ISO-8859-1', sep='\t')
df_aer_backup.to_csv('./Backups/STG_BEA_CA6N_Arts_Entertainment_and_Recreation_BACKUP.txt')

# Create new dataframe for Arts_Entertainment_and_Recreation
filter1 = df['LineCode'] == 1700
df_arts = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_arts.to_csv('./Updates/STG_BEA_CA6N_Arts_Entertainment_and_Recreation.txt', sep = '\t')

# Reset the index
df_arts = df_arts.reset_index()

# Fill NaN values for upload to database
column_list = df_arts.columns.values
for i in column_list:
    df_arts.loc[df_arts[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Arts_Entertainment_and_Recreation_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Arts_Entertainment_and_Recreation','STG_BEA_CA6N_Arts_Entertainment_and_Recreation_BACKUP';''')

# Create Arts_Entertainment_and_Recreation Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Arts_Entertainment_and_Recreation](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_arts.to_sql('STG_BEA_CA6N_Arts_Entertainment_and_Recreation', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Arts_Entertainment_and_Recreation';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1700';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Construction

# In[ ]:


print('Done. Updating Construction..')

# Create Backups
df_con_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Construction.txt', encoding = 'ISO-8859-1', sep='\t')
df_con_backup.to_csv('./Backups/STG_BEA_CA6N_Construction_BACKUP.txt')

# Create new dataframe for Construction
filter1 = df['LineCode'] == 400
df_construction = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_construction.to_csv('./Updates/STG_BEA_CA6N_Construction.txt', sep = '\t')

# Reset the index
df_construction = df_construction.reset_index()

# Fill NaN values for upload to database
column_list = df_construction.columns.values
for i in column_list:
    df_construction.loc[df_construction[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Construction_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Construction','STG_BEA_CA6N_Construction_BACKUP';''')

# Create Construction Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Construction](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_construction.to_sql('STG_BEA_CA6N_Construction', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Construction';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0400';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Educational Services

# In[ ]:


print('Done. Updating Educational Services..')

# Create Backups
df_es_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Educational_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_es_backup.to_csv('./Backups/STG_BEA_CA6N_Educational_Services_BACKUP.txt')

# Create new dataframe for Educational_Services
filter1 = df['LineCode'] == 1500
df_eduserv = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_eduserv.to_csv('./Updates/STG_BEA_CA6N_Educational_Services.txt', sep = '\t')

# Reset the index
df_eduserv = df_eduserv.reset_index()

# Fill NaN values for upload to database
column_list = df_eduserv.columns.values
for i in column_list:
    df_eduserv.loc[df_eduserv[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Educational_Services_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Educational_Services','STG_BEA_CA6N_Educational_Services_BACKUP';''')

# Create Educational_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Educational_Services](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_eduserv.to_sql('STG_BEA_CA6N_Educational_Services', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;


TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Educational_Services';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1500';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Finance and Insurance

# In[ ]:


print('Done. Updating Finance and Insurance..')

# Create Backups
df_fi_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Finance_and_Insurance.txt', encoding = 'ISO-8859-1', sep='\t')
df_fi_backup.to_csv('./Backups/STG_BEA_CA6N_Finance_and_Insurance_BACKUP.txt')

# Create new dataframe for Finance_and_Insurance
filter1 = df['LineCode'] == 1000
df_finance = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_finance.to_csv('./Updates/STG_BEA_CA6N_Finance_and_Insurance.txt', sep = '\t')

# Reset the index
df_finance = df_finance.reset_index()

# Fill NaN values for upload to database
column_list = df_finance.columns.values
for i in column_list:
    df_finance.loc[df_finance[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Finance_and_Insurance_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Finance_and_Insurance','STG_BEA_CA6N_Finance_and_Insurance_BACKUP';''')

# Create Finance_and_Insurance Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Finance_and_Insurance](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_finance.to_sql('STG_BEA_CA6N_Finance_and_Insurance', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Finance_and_Insurance';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_1000';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")


# # Forestry, Fishing, and Related Activities

# In[ ]:


print('Done. Updating Forestry, Fishing, and Related Activities..')

# Create Backups
df_ffr_backup = pd.read_csv('./Updates/STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities.txt', encoding = 'ISO-8859-1', sep='\t')
df_ffr_backup.to_csv('./Backups/STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities_BACKUP.txt')

# Create new dataframe for Forestry_Fishing_and_Related_Activities
filter1 = df['LineCode'] == 100
df_forestry = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_forestry.to_csv('./Updates/STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities.txt', sep = '\t')

# Reset the index
df_forestry = df_forestry.reset_index()

# Fill NaN values for upload to database
column_list = df_forestry.columns.values
for i in column_list:
    df_forestry.loc[df_forestry[i].isnull(),i]=0

# Drop old backup table
c.execute('drop table STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities_BACKUP')

# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities','STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities_BACKUP';''')

# Create Forestry_Fishing_and_Related_Activities Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_forestry.to_sql('STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/

USE STG2;

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key varchar(50)      -- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'BEA'		-- Code for the source of the data
	set @TableName = 'STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @Measure_Business_Key = 'BEA_CA6N_0100';
	set @ColNm = 'YR_ONE'
	set @StartRow = 9;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [STG2].[sys].[all_columns] c
		inner join STG2.sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;


--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = left(@ColNm,4) --+ Right(@ColNm,2);

		SET @SQL = 'INSERT INTO STG2.[dbo].[STG_XLSX_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[DataPeriodID]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]						
		   ) 
		
		SELECT [GeoFIPS],'''+@GEOID_Type+''' ,''BEA_GDPS''+right(''0000''+Ltrim(Rtrim([ComponentID])),4)+''_''+right(''00000''+Ltrim(Rtrim([IndustryID])),5),'''	-- **** THIS LINE NEEDS ADJUSTMENT DEPENDING ON INPUT FILE!!!
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when isnumeric(['+@ColNm+']) = 1 Then cast(['+@ColNm+'] as decimal(18,4))
	else NULL
		end ObservedValue
		,case
			when isnumeric(['+@ColNm+']) = 0 Then ['+@ColNm+']
			else NULL
		end ObservationQualifier'	
		  + ' FROM STG2.[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[STG2].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;


TRUNCATE TABLE [STG2].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [STG2].[dbo].[STG_SAT_MeasureDefn_WRK]
           ([Measure_Business_Key]
		   ,[Record_Source]
           ,[Measure_HashKey]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[TableID]
           ,[Table_LineCode]
           ,[MeasureGroupName]
           ,[MeasureName]
           ,[MeasureCategory]
           ,[Observation_Frequency]
           ,[UOMCode]
           ,[UOMName]
           ,[DefaultScale]
           ,[CalculationType]
           ,[MetricHeirarchyLevel]
           ,[ParticipatesIn]
           ,[NAICS_IndustryCodeStr]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT [Measure_Business_Key]
		,[Record_Source]
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim([Measure_Business_Key]))
		)),2) as HashKey  
      ,Convert(Char(64), Hashbytes('SHA2_256',
	Upper(
		Ltrim(Rtrim(Measure_Authority))
		+ '|' + Ltrim(Rtrim(TableID))
		+ '|' + Ltrim(Rtrim(Table_LineCode))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureGroupName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MeasureCategory,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Observation_Frequency,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMCode,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(UOMName,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(DefaultScale,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(CalculationType,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(MetricHeirarchyLevel,0)))
		+ '|' + Ltrim(Rtrim(COALESCE(ParticipatesIn,'...'))
		+ '|' + Ltrim(Rtrim(COALESCE(NAICS_IndustryCodeStr,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_Industry_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(BEA_GDP_Component_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Source_Citation,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Accessed_Date,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Vintage,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Revised_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(New_Data_Period,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(WNCD_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Notes,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Note_ID,'...')))
		+ '|' + Ltrim(Rtrim(COALESCE(Table_Line_Notes,'...'))))
		)),2) as HashDiff
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Hub_Measure] H
	on M.[Measure_HashKey] = H.[Measure_HashKey]
	Where H.[Record_Source] is NULL;

--	Select * from #NewKeys

/*	Register new Measure keys with the Measure Hub 	*/
INSERT INTO [DV2].[dbo].[Hub_Measure]
           ([Measure_HashKey]
           ,[Measure_Business_Key]
           ,[Load_Date]
           ,[Record_Source]
		   )
   Select	M.[Measure_HashKey]
			,M.[Measure_Business_Key]
			,CURRENT_TIMESTAMP
			,M.[Record_Source]
	FROM #NewKeys N
	inner join [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	Left outer join [DV2].[dbo].[Sat_Measure_Description] D
	on M.[Measure_HashKey] = D.[Measure_HashKey]
	and D.[Load_Date] <= CURRENT_TIMESTAMP
	AND D.[Load_End_Date] > CURRENT_TIMESTAMP
	where D.[Record_Source] is NULL;

--	Select * from #NewKeys1;

/*	Identify existing Measure descriptions that will be replaced by new descriptions	*/

Select	M.[Measure_HashKey]
		,E.[Load_Date]
	into #UpdtKeys
From	[STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
	inner join [DV2].[dbo].[Sat_Measure_Description] E
	on M.[Measure_HashKey] = E.[Measure_HashKey]
	and E.[Load_Date] <= CURRENT_TIMESTAMP
	AND E.[Load_End_Date] > CURRENT_TIMESTAMP
	and M.[Measure_HashDiff] <> E.[Measure_HashDiff];

--	Select * from #UpdtKeys;

/*	Add New Measure Descriptions to the Measure Description Satellite	*/

INSERT INTO [DV2].[dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,CURRENT_TIMESTAMP
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #NewKeys1 N
  on M.[Measure_HashKey] = N.[Measure_HashKey];
  
  /*	Insert Replacement entries for those being retired	*/

Declare @Load_Date		Datetime2(7);
Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [dbo].[Sat_Measure_Description]
           ([Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Measure_Business_Key]
           ,[Measure_HashDiff]
           ,[Measure_Authority]
           ,[Measure_TableID]
           ,[Measure_Table_Line_Number]
           ,[Measure_Group_Name]
           ,[Measure_Name]
           ,[Measure_Category]
           ,[Observation_Frequency]
           ,[Unit_of_Measure_Code]
           ,[Unit_of_Measure_Name]
           ,[Default_Scale]
           ,[Calulation_Type]
           ,[Measure_Hierarchy_Level]
           ,[Participates_In]
           ,[NAICS_Industry_Code_Str]
           ,[BEA_Industry_ID]
           ,[BEA_GDP_Component_ID]
           ,[Source_Citation]
           ,[Accessed_Date]
           ,[Vintage]
           ,[Revised_Data_Period]
           ,[New_Data_Period]
           ,[WNCD_Notes]
           ,[Table_Note_ID]
           ,[Table_Notes]
           ,[Table_Line_Note_ID]
           ,[Table_Line_Notes])
SELECT M.[Measure_HashKey]
		,@Load_Date
		,cast('9999-12-31 23:59:59.9999999' as Datetime2(7)) LoadEndDate
      ,[Record_Source]
      ,[Measure_Business_Key]
      ,[Measure_HashDiff]
      ,[Measure_Authority]
      ,[TableID]
      ,[Table_LineCode]
      ,[MeasureGroupName]
      ,[MeasureName]
      ,[MeasureCategory]
      ,[Observation_Frequency]
      ,[UOMCode]
      ,[UOMName]
      ,[DefaultScale]
      ,[CalculationType]
      ,[MetricHeirarchyLevel]
      ,[ParticipatesIn]
      ,[NAICS_IndustryCodeStr]
      ,[BEA_Industry_ID]
      ,[BEA_GDP_Component_ID]
      ,[Source_Citation]
      ,[Accessed_Date]
      ,[Vintage]
      ,[Revised_Data_Period]
      ,[New_Data_Period]
      ,[WNCD_Notes]
      ,[Table_Note_ID]
      ,[Table_Notes]
      ,[Table_Line_Note_ID]
      ,[Table_Line_Notes]
  FROM [STG2].[dbo].[STG_SAT_MeasureDefn_WRK] M
  inner join #UpdtKeys N
  on M.[Measure_HashKey] = N.[Measure_HashKey];

/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_Measure_Description] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_Measure_Description] T
 WHERE T.[Measure_HashKey] = K.[Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];""")


# In[ ]:


c.execute("""/*****		Post_Observations.sql										*****/
/*****		Post Observations Staged in XLSX Data Series Work Table		*****/
/*****		Insert new observations. Update existing observations.		*****/
/*****		Required Input: [STG2].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE STG2;


DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [STG2].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [STG2].[dbo].[STG_Sat_DataSeries_WRK]
           (
		   [GeoArea_ID]
           ,[GEOID_Type]
           ,[Measure_Business_Key]
           ,[Data_Period_Business_Key]
           ,[GeoArea_Measure_HashKey]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey]
           ,[Record_Source]
           ,[ObservedValue]
           ,[ObservationQualifier]
		   )
SELECT   S.[GeoArea_ID]
        ,S.[GEOID_Type]
        ,S.[Measure_Business_Key]
        ,S.[DataPeriodID]
        ,Convert(Char(64), Hashbytes('SHA2_256',
		  Upper(
				Ltrim(Rtrim(S.[GeoArea_ID]))
				+'|'+Ltrim(Rtrim(S.[GEOID_Type]))
				+'|'+Ltrim(Rtrim(S.[Measure_Business_Key]))
				+'|'+Ltrim(Rtrim(S.[DataPeriodID]))
		       )),2) 
       ,G.GeoArea_HashKey
       ,M.Measure_HashKey
       ,D.Data_Period_HashKey
	   ,S.[Record_Source]
       ,S.[ObservedValue]
       ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_XLSX_DataSeries_WRK] S
  inner join DV2.dbo.Hub_GeoArea G
    on S.GeoArea_ID = G.GeoArea_ID
	and S.GEOID_Type = G.GEOID_Type
  inner join DV2.dbo.Hub_Measure M
    on S.Measure_Business_Key = M.Measure_Business_Key
  Inner Join DV2.dbo.Hub_Data_Period D
    on S.DataPeriodID = D.Data_Period_Business_Key
Where S.[ObservedValue] is not null
	or s.[ObservationQualifier] is not null
;
--Select count(*) as [Loaded to STG_Sat_DataSeries_WRK]
--from [STG2].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[STG2].[dbo].[STG_Sat_DataSeries_WRK] S
inner join [ACS].[dbo].[V_Sat_GeoArea_Measure_Observation_Cur] O
on S.[GeoArea_Measure_HashKey] = O.[GeoArea_Measure_HashKey]
Where (		coalesce(S.[ObservedValue],1) <> coalesce(O.[Estimated_Value],1)
		or 
			coalesce(S.[ObservationQualifier],1) <> coalesce(O.[Estimation_Qualifier],1)
	  ); 

Select @@ROWCOUNT as [Update Keys Identified];


INSERT INTO [DV2].[dbo].[Link_GeoArea_Measurement]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Reord_Source]
           ,[GeoArea_HashKey]
           ,[Measure_HashKey]
           ,[Data_Period_HashKey])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
      ,[Record_Source]
      ,[GeoArea_HashKey]
      ,[Measure_HashKey]
      ,[Data_Period_HashKey]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Links Inserted];


INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,CURRENT_TIMESTAMP
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #NewKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Observations Inserted];

Set @Load_Date = CURRENT_TIMESTAMP;

INSERT INTO [DV2].[dbo].[Sat_GeoArea_Measure_Observation]
           ([GeoArea_Measure_HashKey]
           ,[Load_Date]
           ,[Load_End_Date]
           ,[Record_Source]
           ,[Estimated_Value]
           ,[Estimation_Qualifier])
SELECT S.[GeoArea_Measure_HashKey]
	  ,@Load_Date
	  ,cast('9999-12-31 23:59:59.9999999' as Datetime2(7))
      ,S.[Record_Source]
      ,S.[ObservedValue]
      ,S.[ObservationQualifier]
  FROM [STG2].[dbo].[STG_Sat_DataSeries_WRK] S
  inner join #UpdtKeys N
  on S.GeoArea_Measure_HashKey = N.[GeoArea_Measure_HashKey]

  Select @@ROWCOUNT as [Replacement Observations Inserted];


/*	Retire the entries in the Measure Description satellite that have been replaced.	*/
/*	End-Date entries being retired.													 	*/

UPDATE [DV2].[dbo].[Sat_GeoArea_Measure_Observation] 
   SET [Load_End_Date] =  @Load_Date
   From #UpdtKeys K
	,[DV2].[dbo].[Sat_GeoArea_Measure_Observation] T
 WHERE T.[GeoArea_Measure_HashKey] = K.[GeoArea_Measure_HashKey]
 and T.[Load_Date] = k.[Load_Date];

   Select @@ROWCOUNT as [Observations Retired];""")

