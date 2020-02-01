#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Imports
import pandas as pd
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


# Create backups
df_backup = pd.read_csv('./Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt')
df_backup.to_csv('./Backups/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP.txt')


# In[ ]:


# Getting and reading new data 
df = pd.read_excel("https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147063&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Percent&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2025-01-01&type=xls&startDate=2009-01-01&endDate=2025-01-01&mapWidth=999&mapHeight=521&hideLegend=false", skiprows=1)
df.head(2)


# In[ ]:


# Filter data to display only North Carolina
filter1 = df['Region Name'].str.contains(', NC')
df_nc = df[filter1]
df_nc.head(2)


# In[ ]:


# Set Series ID as Index
df_nc.set_index(df_nc['Series ID'], inplace = True)
df_nc.head(2)


# In[ ]:


# Drop Series ID column
df_nc.drop('Series ID', axis = 1, inplace = True)
df_nc.head(2)


# In[ ]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv('./Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt', sep = '\t')


# In[ ]:


#Reset Index for upload to database
df_nc = df_nc.reset_index()    


# In[ ]:


column_list = df_nc.columns.values
for i in column_list:
    df_nc.loc[df_nc[i].isnull(),i]=0


# In[ ]:


#Connect to database and create cursor
con = pyodbc.connect('Driver={SQL Server};'
                      'Server=STEIN\ECONDEV;'
                      'Database=STG2;'
                      'Trusted_Connection=yes;',
                    autocommit=True)

c = con.cursor()


# In[ ]:


#Drop old backup table
c.execute('drop table STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP')


# In[ ]:


#Create new backup
c.execute('''sp_rename 'dbo.STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent','STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP';''')


# In[ ]:


c.execute('''USE [STG2]

SET ANSI_NULLS ON


SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent](
	[Series ID] [varchar](14) NULL,
	[Region Name] [varchar](23) NULL,
	[Region Code] [int] NULL,
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
    [2025] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#df: pandas.dataframe; mTableName:table name in MS SQL
#warning: discard old table if exists
df_nc.to_sql('STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/
/*******	Specifically modifed for FRED ANNUAL input											******/
/*******				******/

USE STG2;
 

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key	varchar(50)	-- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'FRED'		-- Code for the source of the data
	set @TableName = 'STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	Set @Measure_Business_Key = 'FRED_S1501ACSTOTAL0_00000';		-- Data Series Business Identifier   *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @ColNm = 'YR_ONE'
	set @StartRow = 4;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
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


/*******************************	THE SELECT PART OF THE INSERT STATEMENT NEEDS SPECIAL ATTENTION		************************************/
	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = @ColNm -- + Right(@ColNm,2);  -- MAKE FOR OTHER THAN ANNUAL

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
		
		SELECT StateCodeFIPS+MunicipalCodeFIPS,'''+@GEOID_Type+''' ,'''+@Measure_Business_Key+''','''	
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when ['+@ColNm+'] > 1 Then cast(['+@ColNm+'] as decimal(18,4))
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
	
	Select top 100 * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/
/*******	Specifically modifed for FRED ANNUAL input											******/
/*******				******/

USE STG2;
 

TRUNCATE TABLE STG2.[dbo].[STG_XLSX_DataSeries_WRK];

/*****			Cursor through column names	from [sys].[all_columns]	*******/
Declare @ColNm		varchar(30)	-- holds the column name
	,@ColID			int			-- Tracks the column we are working on
	,@DataPeriodKey Nvarchar(20)
	,@StartRow		int			-- Starting point in [sys].[all_columns] table
	,@SQL			nvarchar(1000)	-- SQL string to be executed
	,@TableName		nvarchar(128)	-- Name of input table
	,@Measure_Business_Key	varchar(50)	-- The Data Series Business Identifier
	,@GEOID_Type	varchar(10)		-- Identifies the standard for the GeoArea Tidentifier
	,@Record_Source	Varchar(10)		-- Code for the source of the data

	Set	@GEOID_Type	= 'FIPS'		-- Identifies the standard for the GeoArea Tidentifier
	Set	@Record_Source	= 'FRED'		-- Code for the source of the data
	set @TableName = 'STG_FRED_All_Transactions_House_Price_Index_by_County';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	Set @Measure_Business_Key = 'FRED_ATNHPIUS_00000';		-- Data Series Business Identifier   *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @ColNm = 'YR_ONE'
	set @StartRow = 4;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
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


/*******************************	THE SELECT PART OF THE INSERT STATEMENT NEEDS SPECIAL ATTENTION		************************************/
	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = @ColNm -- + Right(@ColNm,2);  -- MAKE FOR OTHER THAN ANNUAL

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
		
		SELECT StateCodeFIPS+MunicipalCodeFIPS,'''+@GEOID_Type+''' ,'''+@Measure_Business_Key+''','''	
		  + @DataPeriodKey + ''' ,''' 
		  + @Record_Source + ''' ,' 
		  +'Case 
	when ['+@ColNm+'] > 1 Then cast(['+@ColNm+'] as decimal(18,4))
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
	
	Select top 100 * from STG2.dbo.STG_XLSX_DataSeries_WRK
	;""")


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

