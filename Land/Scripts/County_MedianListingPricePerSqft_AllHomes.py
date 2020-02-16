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


# Create Backups
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_MedianListingPricePerSqft_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_MedianListingPricePerSqft_AllHomes_BACKUP.txt')


# In[ ]:


#Load Land data
df_mlsf = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianListingPricePerSqft_AllHomes.csv', 
                      encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_mlsf.head()


# In[ ]:


#Filter data to NC
filter1 = df_mlsf['State'] == "NC"
df_mlsf_nc = df_mlsf[filter1]

#Check to ensure filter worked
df_mlsf_nc.head(5)


# In[ ]:


#View data types of dataframe
df_mlsf_nc.dtypes


# In[ ]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_mlsf_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlsf_nc['MunicipalCodeFIPS'].astype(str)
df_mlsf_nc.dtypes


# In[ ]:


#Add leading 0's and check to ensure they were added
df_mlsf_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlsf_nc['MunicipalCodeFIPS'].str.zfill(3)
df_mlsf_nc.head(5)


# In[ ]:


# Set Index to Region Name
df_mlsf_nc.set_index(df_mlsf_nc['RegionName'], inplace = True)
df_mlsf_nc


# In[ ]:


# Drop Region Name column
df_mlsf_nc.drop('RegionName', axis = 1, inplace = True)
df_mlsf_nc


# In[ ]:


#Save to csv file for export in Excel
df_mlsf_nc.to_csv('./Updates/STG_ZLLW_County_MedianListingPricePerSqf_AllHomes.txt', sep = '\t')


# In[ ]:


#Reset Index for upload to database
df_mlsf_nc = df_mlsf_nc.reset_index()    


# In[ ]:


#Fill NaN values for upload to database
df_mlsf_nc['Metro'] = df_mlsf_nc['Metro'].replace(np.nan,'', regex=True)

column_list = df_mlsf_nc.columns.values
for i in column_list:
    df_mlsf_nc.loc[df_mlsf_nc[i].isnull(),i]=0


# In[ ]:


#Connect to database and create cursor
con = pyodbc.connect('Driver={SQL Server};'
                      'Server=[servername];'
                      'Database=[databasename];'
                      'Trusted_Connection=yes;',
                    autocommit=True)

c = con.cursor()


# In[ ]:


#Drop old backup table
c.execute('''drop table dbo.STG_ZLLW_County_MedianListingPricePerSqft_AllHomes_BACKUP;''')


# In[ ]:


c.execute('''sp_rename 'dbo.STG_ZLLW_County_MedianListingPricePerSqft_AllHomes','STG_ZLLW_County_MedianListingPricePerSqft_AllHomes_BACKUP';''')


# In[ ]:


c.execute('''USE [[databasename]]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_ZLLW_County_MedianListingPricePerSqft_AllHomes](
	[RegionName] [varchar](40) NULL,
	[State] [varchar](2) NULL,
	[Metro] [varchar](40) NULL,
	[StateCodeFIPS] [varchar](2) NULL,
	[MunicipalCodeFIPS] [varchar](3) NULL,
	[SizeRank] [smallint] NULL,
	[2010-01] [float] NULL,
	[2010-02] [float] NULL,
	[2010-03] [float] NULL,
	[2010-04] [float] NULL,
	[2010-05] [float] NULL,
	[2010-06] [float] NULL,
	[2010-07] [float] NULL,
	[2010-08] [float] NULL,
	[2010-09] [float] NULL,
	[2010-10] [float] NULL,
	[2010-11] [float] NULL,
	[2010-12] [float] NULL,
	[2011-01] [float] NULL,
	[2011-02] [float] NULL,
	[2011-03] [float] NULL,
	[2011-04] [float] NULL,
	[2011-05] [float] NULL,
	[2011-06] [float] NULL,
	[2011-07] [float] NULL,
	[2011-08] [float] NULL,
	[2011-09] [float] NULL,
	[2011-10] [float] NULL,
	[2011-11] [float] NULL,
	[2011-12] [float] NULL,
	[2012-01] [float] NULL,
	[2012-02] [float] NULL,
	[2012-03] [float] NULL,
	[2012-04] [float] NULL,
	[2012-05] [float] NULL,
	[2012-06] [float] NULL,
	[2012-07] [float] NULL,
	[2012-08] [float] NULL,
	[2012-09] [float] NULL,
	[2012-10] [float] NULL,
	[2012-11] [float] NULL,
	[2012-12] [float] NULL,
	[2013-01] [float] NULL,
	[2013-02] [float] NULL,
	[2013-03] [float] NULL,
	[2013-04] [float] NULL,
	[2013-05] [float] NULL,
	[2013-06] [float] NULL,
	[2013-07] [float] NULL,
	[2013-08] [float] NULL,
	[2013-09] [float] NULL,
	[2013-10] [float] NULL,
	[2013-11] [float] NULL,
	[2013-12] [float] NULL,
	[2014-01] [float] NULL,
	[2014-02] [float] NULL,
	[2014-03] [float] NULL,
	[2014-04] [float] NULL,
	[2014-05] [float] NULL,
	[2014-06] [float] NULL,
	[2014-07] [float] NULL,
	[2014-08] [float] NULL,
	[2014-09] [float] NULL,
	[2014-10] [float] NULL,
	[2014-11] [float] NULL,
	[2014-12] [float] NULL,
	[2015-01] [float] NULL,
	[2015-02] [float] NULL,
	[2015-03] [float] NULL,
	[2015-04] [float] NULL,
	[2015-05] [float] NULL,
	[2015-06] [float] NULL,
	[2015-07] [float] NULL,
	[2015-08] [float] NULL,
	[2015-09] [float] NULL,
	[2015-10] [float] NULL,
	[2015-11] [float] NULL,
	[2015-12] [float] NULL,
	[2016-01] [float] NULL,
	[2016-02] [float] NULL,
	[2016-03] [float] NULL,
	[2016-04] [float] NULL,
	[2016-05] [float] NULL,
	[2016-06] [float] NULL,
	[2016-07] [float] NULL,
	[2016-08] [float] NULL,
	[2016-09] [float] NULL,
	[2016-10] [float] NULL,
	[2016-11] [float] NULL,
	[2016-12] [float] NULL,
	[2017-01] [float] NULL,
	[2017-02] [float] NULL,
	[2017-03] [float] NULL,
	[2017-04] [float] NULL,
	[2017-05] [float] NULL,
	[2017-06] [float] NULL,
	[2017-07] [float] NULL,
	[2017-08] [float] NULL,
	[2017-09] [float] NULL,
	[2017-10] [float] NULL,
	[2017-11] [float] NULL,
	[2017-12] [float] NULL,
	[2018-01] [float] NULL,
	[2018-02] [float] NULL,
	[2018-03] [float] NULL,
	[2018-04] [float] NULL,
	[2018-05] [float] NULL,
	[2018-06] [float] NULL,
	[2018-07] [float] NULL,
	[2018-08] [float] NULL,
	[2018-09] [float] NULL,
	[2018-10] [float] NULL,
	[2018-11] [float] NULL,
	[2018-12] [float] NULL,
	[2019-01] [float] NULL,
	[2019-02] [float] NULL,
	[2019-03] [float] NULL,
	[2019-04] [float] NULL,
	[2019-05] [float] NULL,
	[2019-06] [float] NULL,
	[2019-07] [float] NULL,
	[2019-08] [float] NULL,
	[2019-09] [float] NULL,
	[2019-10] [float] NULL,
	[2019-11] [float] NULL,
	[2019-12] [float] NULL,
    [2020-01] [float] NULL,
    [2020-02] [float] NULL,
    [2020-03] [float] NULL,
    [2020-04] [float] NULL,
    [2020-05] [float] NULL,
    [2020-06] [float] NULL,
    [2020-07] [float] NULL,
    [2020-08] [float] NULL,
    [2020-09] [float] NULL,
    [2020-10] [float] NULL,
    [2020-11] [float] NULL,
    [2020-12] [float] NULL,
    [2021-01] [float] NULL,
    [2021-02] [float] NULL,
    [2021-03] [float] NULL,
    [2021-04] [float] NULL,
    [2021-05] [float] NULL,
    [2021-06] [float] NULL,
    [2021-07] [float] NULL,
    [2021-08] [float] NULL,
    [2021-09] [float] NULL,
    [2021-10] [float] NULL,
    [2021-11] [float] NULL,
    [2021-12] [float] NULL,
    [2022-01] [float] NULL,
    [2022-02] [float] NULL,
    [2022-03] [float] NULL,
    [2022-04] [float] NULL,
    [2022-05] [float] NULL,
    [2022-06] [float] NULL,
    [2022-07] [float] NULL,
    [2022-08] [float] NULL,
    [2022-09] [float] NULL,
    [2022-10] [float] NULL,
    [2022-11] [float] NULL,
    [2022-12] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=[servername];'
                                 r'Database=[databasename];'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_mlsf_nc.to_sql('STG_ZLLW_County_MedianListingPricePerSqft_AllHomes', con=engine, if_exists='replace', index=False)


# In[ ]:


c.execute("""/*******	DYNAMIC SCRIPT FOR MOVING DATA FROM SHALLOW-AND-WIDE LAYOUT TO DEEP-AND-NARROW LAYOUT	******/
/*******	TARGET (OUTPUT) TABLE IS STATIC. SOURCE (INPUT) TABLE IS DIFFERENT EVERY TIME			******/
/*******	Specifically modifed for Zillow Monthly input											******/
/*******				******/

USE [databasename];
GO 

TRUNCATE TABLE [databasename].[dbo].[STG_XLSX_DataSeries_WRK];

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
	Set	@Record_Source	= 'ZLLW'		-- Code for the source of the data
	set @TableName = 'STG_ZLLW_County_MedianListingPricePerSqft_AllHomes';   -- SOURCE TABLE *** NEEDS TO BE UPDATED MANUALLY!!!!!
	Set @Measure_Business_Key = 'ZLLW_CNTY_MLP01';		-- Data Series Business Identifier   *** NEEDS TO BE UPDATED MANUALLY!!!!!
	set @ColNm = 'YR_ONE'
	set @StartRow = 7;				-- **** NEEDS ADJUSTMENT DEPENDING ON INPUT *****
	set @DataPeriodKey = '9999'

	Declare C Cursor Fast_Forward	/* Fast_Forward specifies Read-only, Forward-only */
		for 
		Select  C.name	
				,C.column_id
		FROM [[databasename]].[sys].[all_columns] c
		inner join [databasename].sys.all_objects t
		on c.object_id = t.object_id
		and t.name = @TableName
		and c.column_id >= @StartRow;

	Open C;

	Fetch Next From C into @ColNm, @ColID;
--	SELECT @ColNm, @ColID

	While @@Fetch_Status = 0
	Begin
		Set @DataPeriodKey = replace(@ColNm,'-','M') -- + Right(@ColNm,2);  -- MAKE FOR OTHER THAN ANNUAL

		SET @SQL = 'INSERT INTO [databasename].[dbo].[STG_XLSX_DataSeries_WRK]
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
		  + ' FROM [databasename].[dbo].' + @TableName;

--		Select @SQL as SQLStmt
		EXEC(@SQL)
--		Break
		Fetch Next From C into @ColNm, @ColID;
	End;
	Close C;
	Deallocate C;
	
--	Select cOUNT(*) from [databasename].dbo.STG_XLSX_DataSeries_WRK
--	WHERE [oBSERVEDvALUE] IS NOT NULL;

	Select top 100 * from [databasename].dbo.STG_XLSX_DataSeries_WRK
	;""")	


# In[ ]:


c.execute("""/**************************************************************************/
/***																	***/
/***				PopulateDV2_Measure_Tables.sql						***/
/***																	***/
/***	The script adds entries to:										***/
/***				-	[[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK]			***/
/***				-	[DV2].[dbo].[Hub_Measure]						***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	and updates the Load End Date for entries retiring in			***/
/***				-	[DV2].[dbo].[Sat_Measure_Description]			***/
/***	All input come from:											***/
/***				-	[[databasename]].[dbo].[STG_XLSX_MeasureDefn_WRK]			***/
/***	This is the only place where HashKeys are calculated for 		***/
/***	Measures														***/
/***																	***/
/**************************************************************************/

USE DV2;
GO

TRUNCATE TABLE [[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK];

INSERT INTO [[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK]
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
  FROM [[databasename]].[dbo].[STG_XLSX_MeasureDefn_WRK];

/*	List the Keys from the incoming data that are not currently present in the Measure Hub	*/



Select	M.[Measure_HashKey]
	into #NewKeys
From	[[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK] M
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
	inner join [[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK] M
	on N.[Measure_HashKey] = M.[Measure_HashKey]
	;

/*	List the Keys from the incoming data that are not currently present in the Measure Description Satellite	*/
DROP TABLE IF EXISTS #NewKeys1;

Select	M.[Measure_HashKey]
	into #NewKeys1
From	[[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK] M
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
From	[[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK] M
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
  FROM [[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK] M
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
  FROM [[databasename]].[dbo].[STG_SAT_MeasureDefn_WRK] M
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
/*****		Required Input: [[databasename]].[dbo].[STG_XLSX_DataSeries_WRK]		*****/


USE [databasename];
GO

DECLARE		@Load_Date		Datetime2(7)

DROP TABLE IF EXISTS #NewKeys;

DROP TABLE IF EXISTS #UpdtKeys;

TRUNCATE TABLE [[databasename]].[dbo].[STG_Sat_DataSeries_WRK];

INSERT INTO [[databasename]].[dbo].[STG_Sat_DataSeries_WRK]
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
  FROM [[databasename]].[dbo].[STG_XLSX_DataSeries_WRK] S
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
--from [[databasename]].[dbo].[STG_Sat_DataSeries_WRK];

Select @@ROWCOUNT as [Update Rows Staged]

Select S.[GeoArea_Measure_HashKey]
  into #NewKeys
From	[[databasename]].[dbo].[STG_Sat_DataSeries_WRK] S
left outer join [DV2].[dbo].[Link_GeoArea_Measurement] L
ON S.[GeoArea_Measure_HashKey] = L.[GeoArea_Measure_HashKey]
WHERE L.[Reord_Source] is null;

Select @@ROWCOUNT as [New Keys Identified];


Select  S.[GeoArea_Measure_HashKey]
		,O.Load_Date
	into #UpdtKeys
From	[[databasename]].[dbo].[STG_Sat_DataSeries_WRK] S
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
  FROM [[databasename]].[dbo].[STG_Sat_DataSeries_WRK] S
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
  FROM [[databasename]].[dbo].[STG_Sat_DataSeries_WRK] S
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
  FROM [[databasename]].[dbo].[STG_Sat_DataSeries_WRK] S
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

