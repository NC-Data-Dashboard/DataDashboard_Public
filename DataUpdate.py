#!/usr/bin/env python -W ignore

#Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile
import urllib
import numpy as np
import os
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import time
import subprocess

#Watermark
print('-------------------------\nNathan Young\nJunior Data Analyst, Project Lead Developer\nNC Data Dashboard\nCenter for the Study of Free Enterprise\nWestern Carolina University\nLast Updated: Apr 01 2020\n-------------------------\nStarting NC Data Dashboard Update...')

#Connecting to Database
con = pyodbc.connect('Driver={SQL Server};'
                    'Server=STEIN\ECONDEV;'
                    'Database=General_Vault;'
                    'Trusted_Connection=yes;',
                    autocommit=True)
c = con.cursor()

#Clearing terminal for clean operation
clear = lambda: os.system('cls')
clear()

#Program
try:
    #Get root directory per computer
    path = os.getcwd()

    #Run Program
    def runProgram(): #done
        os.chdir(path)
        print('NC Data Dashboard Update\n-------------------------\nMain Menu:\n\n1-Demographics\n2-Earnings\n3-Health\n4-Labor\n5-Land\n6-Natural Products\n\n888-Update All\n\n999-Exit\n\nNote: Ctrl+C will terminate the program at any time.\n-------------------------')
        folder = int(input('Which folder would you like to clean data for? '))
        if folder == 1:
            print('\nTaking you to Demographics...')
            os.chdir('./Demographics')
            time.sleep(3)
            clear()
            demographics_update()
        elif folder == 2:
            print('\nTaking you to Earnings...')
            os.chdir('./Earnings')
            time.sleep(3)
            clear()
            earnings_update()     
        elif folder == 3: #List health sources [Website/source](address)
            print('\nThe Health folder does not have automated updates due to the way data is secured by its respective sources.\nThis folder has to updated manually and the sources are as follows:\n\n\n\n')
            pass
        elif folder == 4:
            print('\nTaking you to Labor...')
            os.chdir('./Labor')
            time.sleep(3)
            clear()
            labor_update()
        elif folder == 5:
            print('\nTaking you to Land...')
            os.chdir('./Land')
            time.sleep(3)
            clear()
            land_update()
        elif folder == 6:
            print('\nTaking you to Natural Products...')
            os.chdir('./Natural Products')
            time.sleep(3)
            clear()
            natproducts_update()
        elif folder == 888:
            print('Updating all folders...')
            subprocess.call([r'Update.bat'])
            pass
        elif folder == 999:
            clear()
            exit()
        else:
            print('Please enter a number from the menu.')
            runProgram()
        while True:
            endProgram()

    #Leave Program
    def endProgram(): #done
        answer = int(input('-------------------------\nEnd Program\n\n1-Return to Main Menu\n2-Publish Data to Database\n\n999-Exit\n-------------------------\nWhat do you want to do? '))
        if answer == 1:
            print('Returning to main menu.')
            clear()
            print('Restarting program...')
            time.sleep(3)
            clear()
            runProgram()
            pass
        elif answer == 2:
            print('Connecting to database.')
            time.sleep(3)
            print('Connected.')
            pass
        elif answer == 999:
            exit()
        else:
            print('Please enter a number from the menu')
        while True:
            endProgram()

##### Demographics ##### -- working, needs send to Excel

    #Updating Demographics section
    def demographics_update(): #done
        print('NC Data Dashboard Update\n-------------------------\nWelcome to Demographics!\n')
        section_or_sources = int(input('Menu:\n1-Section\n2-Individual Sources\n\n999-Exit\n-------------------------\nAre you updating the folder or individual sources? '))
        if section_or_sources == 1: 
            print('Updating and publishing folder...')
            subprocess.call([r'demographics.bat'])
            pass
        elif section_or_sources == 2:
            rounds = int(input('How many files are you updating? '))
            for i in range(rounds):
                source = int(input('-------------------------\nDemographics Sources:\n\n1-GeoFred\n2-Census (Unavailable)\n\n999-Exit\n-------------------------\nWhat source are you updating? '))
                if source == 1:
                    demographicsFRED()
                elif source == 2:
                    print('This source is currently unavailable. ')
                    demographics_update()
                elif source == 999:
                    exit()
                else:
                    print('Please enter a number from the menu.')
                    demographics_update()
        elif section_or_sources == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            demographics_update()
        while True:
            endProgram()

    #Cleaning Demographics GeoFRED data
    def demographicsFRED(): #done
        clear()
        print('NC Data Dashboard Update\n-------------------------\nUpdating Demographics GeoFRED\n\nDemographics Sources:\n1-Civilian Labor Force\n2-EQFXSUBPRIME\n3-People 25 and Over Education\n4-Resident Population\n\n999-Exit\n-------------------------')
        files = {1:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=39.98&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=656&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Persons&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=582&hideLegend=false', 2:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147149&attributes=Not+Seasonally+Adjusted%2C+Quarterly%2C+Percent&aggregationFrequency=Quarterly&aggregationType=Average&transformation=lin&date=2025-01-01&type=xls&startDate=1999-01-01&endDate=2025-01-01&mapWidth=999&mapHeight=521&hideLegend=false', 3:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147063&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Percent&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=2009-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=521&hideLegend=false', 4:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.78&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1549&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Thousands+of+Persons&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1970-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=582&hideLegend=false'}
        source = int(input('What source are you updating? '))
        if source == 1:
            print('Updating Civilian Labor Force')
            filename = './Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt'
            backup_fn = './Backups/STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 2:
            print('Updating EQFXSUBPRIME')
            filename = './Updates/STG_FRED_EQFXSUBPRIME.txt'
            backup_fn = './Backups/STG_FRED_EQFXSUBPRIME_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 3:
            print('Updating People Under 25 Education Status')
            filename = './Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt'
            backup_fn = './Backups/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 4:
            print('Updating Resident Population')
            filename = './Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt'
            backup_fn = './Backups/STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            demographicsFRED()
        while True:
            print('Connecting to database to publish data...')
            time.sleep(3)
            clear()
            demographics_publish_FRED()

    #Publishing Demographics GeoFRED data
    def demographics_publish_FRED(): #working, need send to Excel
        print('NC Data Dashboard Publish\n-------------------------\nPublishing Demographics\n\nGeoFred sources:\n1-Civilian Labor Force\n2-EQFXSUPRIME\n3-People 25 and Over Education\n4-Resident Population\n\n999-Exit\n-------------------------')
        folder = int(input('What table are you publishing? '))
        if folder == 1:
            print('Publishing Civilian Labor Force')
            df = pd.read_csv('./Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt', sep='\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_Civilian_Labor_Force_by_County_Persons','STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_Civilian_Labor_Force_by_County_Persons](
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                                r'Server=STEIN\ECONDEV;'
                                                r'Database=General_Vault;'
                                                r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_Civilian_Labor_Force_by_County_Persons', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 2:
            print('Publishing EQFXSUBPRIME')
            df = pd.read_csv('./Updates/STG_FRED_EQFXSUBPRIME.txt', sep='\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_EQFXSUBPRIME_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_EQFXSUBPRIME','STG_FRED_EQFXSUBPRIME_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_EQFXSUBPRIME](
                [Series ID] [varchar](18) NULL,
                [Region Name] [varchar](23) NULL,
                [Region Code] [int] NULL,
                [1999 Q1] [float] NULL,
                [1999 Q2] [float] NULL,
                [1999 Q3] [float] NULL,
                [1999 Q4] [float] NULL,
                [2000 Q1] [float] NULL,
                [2000 Q2] [float] NULL,
                [2000 Q3] [float] NULL,
                [2000 Q4] [float] NULL,
                [2001 Q1] [float] NULL,
                [2001 Q2] [float] NULL,
                [2001 Q3] [float] NULL,
                [2001 Q4] [float] NULL,
                [2002 Q1] [float] NULL,
                [2002 Q2] [float] NULL,
                [2002 Q3] [float] NULL,
                [2002 Q4] [float] NULL,
                [2003 Q1] [float] NULL,
                [2003 Q2] [float] NULL,
                [2003 Q3] [float] NULL,
                [2003 Q4] [float] NULL,
                [2004 Q1] [float] NULL,
                [2004 Q2] [float] NULL,
                [2004 Q3] [float] NULL,
                [2004 Q4] [float] NULL,
                [2005 Q1] [float] NULL,
                [2005 Q2] [float] NULL,
                [2005 Q3] [float] NULL,
                [2005 Q4] [float] NULL,
                [2006 Q1] [float] NULL,
                [2006 Q2] [float] NULL,
                [2006 Q3] [float] NULL,
                [2006 Q4] [float] NULL,
                [2007 Q1] [float] NULL,
                [2007 Q2] [float] NULL,
                [2007 Q3] [float] NULL,
                [2007 Q4] [float] NULL,
                [2008 Q1] [float] NULL,
                [2008 Q2] [float] NULL,
                [2008 Q3] [float] NULL,
                [2008 Q4] [float] NULL,
                [2009 Q1] [float] NULL,
                [2009 Q2] [float] NULL,
                [2009 Q3] [float] NULL,
                [2009 Q4] [float] NULL,
                [2010 Q1] [float] NULL,
                [2010 Q2] [float] NULL,
                [2010 Q3] [float] NULL,
                [2010 Q4] [float] NULL,
                [2011 Q1] [float] NULL,
                [2011 Q2] [float] NULL,
                [2011 Q3] [float] NULL,
                [2011 Q4] [float] NULL,
                [2012 Q1] [float] NULL,
                [2012 Q2] [float] NULL,
                [2012 Q3] [float] NULL,
                [2012 Q4] [float] NULL,
                [2013 Q1] [float] NULL,
                [2013 Q2] [float] NULL,
                [2013 Q3] [float] NULL,
                [2013 Q4] [float] NULL,
                [2014 Q1] [float] NULL,
                [2014 Q2] [float] NULL,
                [2014 Q3] [float] NULL,
                [2014 Q4] [float] NULL,
                [2015 Q1] [float] NULL,
                [2015 Q2] [float] NULL,
                [2015 Q3] [float] NULL,
                [2015 Q4] [float] NULL,
                [2016 Q1] [float] NULL,
                [2016 Q2] [float] NULL,
                [2016 Q3] [float] NULL,
                [2016 Q4] [float] NULL,
                [2017 Q1] [float] NULL,
                [2017 Q2] [float] NULL,
                [2017 Q3] [float] NULL,
                [2017 Q4] [float] NULL,
                [2018 Q1] [float] NULL,
                [2018 Q2] [float] NULL,
                [2018 Q3] [float] NULL,
                [2018 Q4] [float] NULL,
                [2019 Q1] [float] NULL,
                [2019 Q2] [float] NULL,
                [2019 Q3] [float] NULL,
                [2019 Q4] [float] NULL,
                [2020 Q1] [float] NULL,
                [2020 Q2] [float] NULL,
                [2020 Q3] [float] NULL,
                [2020 Q4] [float] NULL,
                [2021 Q1] [float] NULL,
                [2021 Q2] [float] NULL,
                [2021 Q3] [float] NULL,
                [2021 Q4] [float] NULL,
                [2022 Q1] [float] NULL,
                [2022 Q2] [float] NULL,
                [2022 Q3] [float] NULL,
                [2022 Q4] [float] NULL,
                [2023 Q1] [float] NULL,
                [2023 Q2] [float] NULL,
                [2023 Q3] [float] NULL,
                [2023 Q4] [float] NULL,
                [2024 Q1] [float] NULL,
                [2024 Q2] [float] NULL,
                [2024 Q3] [float] NULL,
                [2024 Q4] [float] NULL,
                [2025 Q1] [float] NULL,
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                                r'Server=STEIN\ECONDEV;'
                                                r'Database=General_Vault;'
                                                r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, pool_pre_ping=True)
            df.to_sql('STG_FRED_EQFXSUBPRIME', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 3:
            print('Publishing People 25 and Over Education')
            df = pd.read_csv('./Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt', sep='\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent','STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP';''')
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                                r'Server=STEIN\ECONDEV;'
                                                r'Database=General_Vault;'
                                                r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 4:
            print('Publishing Resident Population')
            df = pd.read_csv('./Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt', sep='\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_Resident_Population_by_County_Thousands_of_Persons','STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_Resident_Population_by_County_Thousands_of_Persons](
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                                r'Server=STEIN\ECONDEV;'
                                                r'Database=General_Vault;'
                                                r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_Resident_Population_by_County_Thousands_of_Persons', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            pass
        while True:
            endProgram()

##### Earnings ##### -- working, needs SQL

    #Updating Earnings section
    def earnings_update(): #done
        print('NC Data Dashboard Update\n-------------------------\nWelcome to Earnings!\n')
        section_or_sources = int(input('Menu:\n1-Section\n2-Individual Sources\n\n999-Exit\n-------------------------\nAre you updating the folder or individual sources? '))
        if section_or_sources == 1:
            print('Updating and publishing folder...')
            subprocess.call([r'earnings.bat'])
            pass
        elif section_or_sources == 2:
            rounds = int(input('How many files are you updating? '))
            for i in range(rounds):
                source = int(input('-------------------------\nEarnings Sources:\n\n1-BEA\n2-NCDOR\n\n999-Exit\n-------------------------\nWhat source are you updating? '))
                if source == 1:
                    earningsCAINC5N()
                elif source == 2:
                    earningsNCDOR() 
                else:
                    print('Please enter a number from the menu.')
                    earnings_update()
        elif section_or_sources == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            earnings_update()
        while True:
            endProgram()

    # Cleaning Earnings BEA CAINC5N Data
    def earningsCAINC5N(): #done
        print('Updating CAINC5N Earnings Version.')
        response = requests.get('https://apps.bea.gov/regional/zip/CAINC5N.zip')
        zip = ZipFile(BytesIO(response.content))
        files = zip.namelist()
        with zip.open(files[34]) as csvfile:
            df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")
            df.drop(df.tail(4).index,inplace=True)
            df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
            df.set_index(df['GeoFIPS'], inplace = True)
            df.drop('GeoFIPS', axis = 1, inplace = True)   
            linecodes = {10:'./Updates/STG_BEA_CA5N_PersonalIncome.txt', 20:'./Updates/STG_BEA_CA5N_Population.txt', 30:'./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt', 35:'./Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt', 50:'./Updates/STG_BEA_CA5N_Wages_and_Salaries.txt', 60:'./Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt', 70:'./Updates/STG_BEA_CA5N_Proprietors_Income.txt', 81:'./Updates/STG_BEA_CA5N_Farm_Earnings.txt', 82:'./Updates/STG_BEA_CA5N_Nonfarm_Earnings.txt', 90:'./Updates/STG_BEA_CA5N_Private_NonFarm_Earnings.txt', 100:'./Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt', 200:'./Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Technical_Services.txt', 300:'./Updates/STG_BEA_CA5N_Utilities.txt', 400:'./Updates/STG_BEA_CA5N_Construction.txt', 500:'./Updates/STG_BEA_CA5N_Manufacturing.txt', 600:'./Updates/STG_BEA_CA5N_Wholesale_Trade.txt', 700:'./Updates/STG_BEA_CA5N_Retail_Trade.txt', 800:'./Updates/STG_BEA_CA5N_Transporatation_and_Warehousing.txt', 900:'./Updates/STG_BEA_CA5N_Information.txt', 1000:'./Updates/STG_BEA_CA5N_Finance_and_Insurance.txt', 1100:'./Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt', 1200:'./Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt', 1300:'./Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt', 1400:'./Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', 1500:'./Updates/STG_BEA_CA5N_Educational_Services.txt', 1600:'./Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt', 1700:'./Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt', 1800:'./Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt', 1900:'./Updates/STG_BEA_CA5N_Other_Services.txt', 2000:'./Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt', 2001:'./Updates/STG_BEA_CA5N_Federal_Civilian.txt', 2002:'./Updates/STG_BEA_CA5N_Military.txt', 2010:'./Updates/STG_BEA_CA5N_State_and_Local.txt', 2011:'./Updates/STG_BEA_CA5N_State_Government.txt', 2012:'./Updates/STG_BEA_CA5N_Local_Government.txt'}
            for key, value in linecodes.items():
                filter1 = df['LineCode'] == key
                df_filtered = df[filter1]
                df_filtered.to_csv(value, sep = '\t')
                print('\nUpdate Complete!')
            pass
        while True:
            print('Connecting to database to publish data...')
            time.sleep(3)
            clear()
            earnings_publish_CAINC5N()

    #Cleaning Earnings NCDOR data
    def earningsNCDOR(): #building, will only take latest month of data, need to append latest month to entire csv to txt
        print('Updating NCDOR\n-------------------------\nBEA Sources:\n1-MSALESUSETAX_0001\n2-MSALESUSETAX_0002\n\n999-Exit\n-------------------------\n')
        source = int(input('Which file would you like to update? '))
        if source == 1:
            print('Updating MSALESUSETAX_0001')
            month = input('Please enter link to new month tax data: ')
            df = pd.read_excel(month, skiprows=9)
            df = df.drop(df.index[0])
            df = df[:-8]
            df = df.loc[:,~df.columns.str.contains('Unnamed')]
            df2 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
            df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
            df = df.rename(columns = {'County.1':'County', 'Collections*':'Collections'})
            df2 = df2.rename(columns = {'County.1':'County', 'Collections*.1':'Collections'})
            df_append = df.append(df2, ignore_index=True)
            df_append = df_append.dropna(how='all')
            df_append = df_append.fillna('0')
            df_append['Collections'] = df_append['Collections'].astype(float)
            df_append = df_append[:-5]
            #pull previous file and append new data to old file as update file.
            df_append.to_csv('./Updates/STG_BEA_MSALESUSETAX_0001.txt', sep='\t')
            pass
        elif source == 2:
            print('Updating MSALESUSETAX_0002')
            month = input('Please enter link to new month tax data: ')
            df = pd.read_excel(month, skiprows =  9)
            df = df.drop(df.index[0])
            df = df[:-8]
            df = df.loc[:,~df.columns.str.contains('Unnamed')]
            df2 = pd.DataFrame(df, columns = ['County.1', 'and Purchases*.1'])
            df = df.drop(columns=['County.1', 'Collections*', 'Collections*.1', 'and Purchases*.1'])
            df = df.rename(columns = {'County.1':'County', 'and Purchases*':'Sales'})
            df2 = df2.rename(columns = {'County.1':'County', 'and Purchases*.1':'Sales'})
            df_append = df.append(df2, ignore_index=True)
            df_append = df_append.dropna(how='all')
            df_append = df_append.fillna('0')
            df_append['Sales'] = df_append['Sales'].astype(float)
            df_append = df_append[:-5]
            df_append.to_csv('./Updates/STG_BEA_MSALESUSETAX_0002.txt', sep='\t')
            print('\nUpdate Complete!')
            pass
        elif source == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            earningsNCDOR()
        while True:
            print('Connecting to database to publish data...')
            time.sleep(3)
            clear()
            earnings_publish_NCDOR()
    
    #Publishing Earnings BEA data
    def earnings_publish_CAINC5N(): #working, need send to Excel
        print('NC Data Dashboard Publish\n-------------------------\nPublishing Earnings\n\nCAINC5N sources:\n1-Wages and Salaries\n2-Health Care and Social Assistance\n3-Information\n4-Management of Companies and Enterprises\n5-Manufactoring\n6-Mining, Quarrying, etc.\n7-Other Services\n8-Pro, Sci, Tech Services\n9-Real Estate and Rental Housing\n10-Retail Trade\n11-Transportation and Warehousing\n12-Utilities\n13-Wholesale Trade\n14-Proprietors Income\n15-Government and Government Enterprises\n16-Private Nonfarm Compensation\n17-Farm Compensation\n18-Nonfarm Compensation\n19-Supplements to Wages and Salaries\n20-Federal, Civilian\n21-Accommodation and Food Services\n22-Administrative Support\n23-Arts, Entertainment, and Recreation\n24-Construction\n25-Educational Services\n26-Finance and Insurance\n27-Forestry, Fishing, and Related Activities\n28-Military\n29-State and Local\n30-State Government\n31-Local Government\n\n999-Exit\n-------------------------')
        folder = int(input('What table are you publishing? '))
        if folder == 1: #Wages and Salaries
            print('Publishing Wages and Salaries')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Wages_and_Salaries.txt', sep = '\t')
            df = df.reset_index() 
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Wages_and_Salaries_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Wages_and_Salaries','STG_BEA_CA5N_Wages_and_Salaries_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Wages_and_Salaries](
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
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Wages_and_Salaries', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 2: #Health Care and Social Assistance
            print('Updating Health Care and Social Assistance')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt', sep = '\t')
            df = df.reset_index() 
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Health_Care_and_Social_Assistance_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Health_Care_and_Social_Assistance','STG_BEA_CA5N_Health_Care_and_Social_Assistance_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Health_Care_and_Social_Assistance](
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
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Health_Care_and_Social_Assistance', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 3: #Information
            print('Updating Information')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Information.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Information_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Information','STG_BEA_CA5N_Information_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Information](
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
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Information', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 4: #Mgt of Companies and Enterprises
            print('Publishing Mgt of Companies and Enterprises')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Management_of_Companies_and_Enterprises_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Management_of_Companies_and_Enterprises','STG_BEA_CA5N_Management_of_Companies_and_Enterprises_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Management_of_Companies_and_Enterprises](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Management_of_Companies_and_Enterprises', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 5: #Manufactoring
            print('Publishing Manufactoring')
            df= pd.read_csv('./Updates/STG_BEA_CA5N_Manufacturing.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Manufacturing_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Manufacturing','STG_BEA_CA5N_Manufacturing_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Manufacturing](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')

            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Manufacturing', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 6: #Mining
            print('Publishing Mining, Quarrying, etc.')
            df= pd.read_csv('./Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction','STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 7: #Other Services
            print('Publishing Other Services')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Other_Services.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Other_Services_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Other_Services','STG_BEA_CA5N_Other_Services_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Other_Services](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Other_Services', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 8: #Pro, Sci, Tech Services
            print('Publishing Pro, Sci, Tech Services')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Professional_Scientific_and_Technical_Services_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Professional_Scientific_and_Technical_Services','STG_BEA_CA5N_Professional_Scientific_and_Technical_Services_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Professional_Scientific_and_Technical_Services](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Professional_Scientific_and_Technical_Services', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 9: #Real Estate and Rental Housing
            print('Publishing Real Estate')
            df = df.read_csv('./Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing','STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 10: #Retail Trade
            print('Publishing Retail Trade')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Retail_Trade.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Retail_Trade_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Retail_Trade','STG_BEA_CA5N_Retail_Trade_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Retail_Trade](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Retail_Trade', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 11: #Transportation and Warehousing
            print('Publishing Transportation')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Transportation_and_Warehousing.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Transportation_and_Warehousing_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Transportation_and_Warehousing','STG_BEA_CA5N_Transportation_and_Warehousing_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Transportation_and_Warehousing](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Transportation_and_Warehousing', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 12: #Utilities
            print('Publishing Utilites')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Utilities.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Utilities_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Utilities','STG_BEA_CA5N_Utilities_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Utilities](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Utilities', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 13: #Wholesale Trade
            print('Publishing Wholesale Trade')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Wholesale_Trade.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Wholesale_Trade_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Wholesale_Trade','STG_BEA_CA5N_Wholesale_Trade_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Wholesale_Trade](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Wholesale_Trade', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 14: #Proprietors Income
            print('Publishing Proprietors Income')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Proprietors_Income.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Proprietors_Income_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Proprietors_Income','STG_BEA_CA5N_Proprietors_Income_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Proprietors_Income](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Proprietors_Income', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 15: #Government and Government Enterprises
            print('Publishing Government and Government Enterprises')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Government_and_Government_Enterprises_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Government_and_Government_Enterprises','STG_BEA_CA5N_Government_and_Government_Enterprises_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Government_and_Government_Enterprises](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Government_and_Government_Enterprises', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 16: #Private Nonfarm Compensation
            print('Publish Private Nonfarm Compensation')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Private_Nonfarm_Compensation.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Private_Nonfarm_Compensation_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Private_Nonfarm_Compensation','STG_BEA_CA5N_Private_Nonfarm_Compensation_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Private_Nonfarm_Compensation](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Private_Nonfarm_Compensation', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 17: #Farm Compensation
            print('Publishing Farm Compensation')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Farm_Compensation.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Farm_Compensation_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Farm_Compensation','STG_BEA_CA5N_Farm_Compensation_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Farm_Compensation](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Farm_Compensation', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 18: #Nonfarm Compensation
            print('Publish Nonfarm Compensation')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Nonfarm_Compensation.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Nonfarm_Compensation_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Nonfarm_Compensation','STG_BEA_CA5N_Nonfarm_Compensation_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Nonfarm_Compensation](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Nonfarm_Compensation', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 19: #Supplements to Wages and Salaries
            print('Publish Supplements to Wages and Salaries')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Supplements_to_Wages_and_Salaries_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Supplements_to_Wages_and_Salaries','STG_BEA_CA5N_Supplements_to_Wages_and_Salaries_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Supplements_to_Wages_and_Salaries](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Supplements_to_Wages_and_Salaries', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 20: #Federal, Civilian
            print('Publish Federal, Civilian')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Federal_Civilian_Government.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Federal_Civilian_Government_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Federal_Civilian_Government','STG_BEA_CA5N_Federal_Civilian_Government_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Federal_Civilian_Government](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Federal_Civilian_Government', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 21: #Accommodation and Food Services
            print('Publish Accommodation and Food Services')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Accommodation_and_Food_Services_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Accommodation_and_Food_Services','STG_BEA_CA5N_Accommodation_and_Food_Services_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Accommodation_and_Food_Services](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Accommodation_and_Food_Services', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 22: #Administrative Support
            print('Publish Administrative Support')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services','STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 23: #Arts, Entertainment, and Recreation
            print('Publish Arts, Entertainment, and Recreation')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Arts_Entertainment_and_Recreation_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Arts_Entertainment_and_Recreation','STG_BEA_CA5N_Arts_Entertainment_and_Recreation_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Arts_Entertainment_and_Recreation](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Arts_Entertainment_and_Recreation', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 24: #Construction
            print('Publish Construction')
            df = pd.read__csv('./Updates/STG_BEA_CA5N_Construction.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Construction_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Construction','STG_BEA_CA5N_Construction_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Construction](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Construction', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 25: #Educational Services
            print('Publish Educational Services')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Educational_Services.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Educational_Services_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Educational_Services','STG_BEA_CA5N_Educational_Services_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Educational_Services](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Educational_Services', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 26: #Finance and Insurance
            print('Publish Finance and Insurance')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Finance_and_Insurance.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Finance_and_Insurance_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Finance_and_Insurance','STG_BEA_CA5N_Finance_and_Insurance_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Finance_and_Insurance](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Finance_and_Insurance', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 27: #Forestry, Fishing, and Related Activities
            print('Publish Forestry, Fishing, and Related Activities')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities','STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 28: #Military
            print('Publish Military')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Military_Government.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Military_Government_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Military_Government','STG_BEA_CA5N_Military_Government_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Military_Government](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Military_Government', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 29: #State and Local
            print('Publish State and Local')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_State_Local_Government.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_State_Local_Government_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_State_Local_Government','STG_BEA_CA5N_State_Local_Government_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_State_Local_Government](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_State_Local_Government', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 30: #State Government
            print('Publish State Government')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_State_Government.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_State_Government_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_State_Government','STG_BEA_CA5N_State_Government_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_State_Government](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_State_Government', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 31: #Local Government
            print('Publish Local Government')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Local_Government.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Local_Government_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Local_Government','STG_BEA_CA5N_Local_Government_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Local_Government](
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
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Local_Government', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 999:
            print('Have a nice day!')
            exit()
        while True:
            endProgram()

    #Publishing Earnings NCDOR data
    def earnings_publish_NCDOR(): #building, need SQL
        while True:
            endProgram()

##### Labor ##### -- working, needs send to Excel

    #Updating Labor section
    def labor_update(): #done
        print('NC Data Dashboard Update\n-------------------------\nWelcome to Labor!\n')
        section_or_sources = int(input('Menu:\n1-Section\n2-Individual Sources\n\n999-Exit\n-------------------------\nAre you updating the folder or individual sources? '))
        if section_or_sources == 1:
            print('Updating and publishing folder...')
            subprocess.call([r'labor.bat'])
            pass
        elif section_or_sources == 2:
            rounds = int(input('How many files are you updating? '))
            for i in range(rounds):
                source = int(input('-------------------------\nLabor Sources:\n\n1-GeoFred\n2-BEA\n\n999-Exit\n-------------------------\nWhat source are you updating? '))
                if source == 1:
                    laborFRED()
                elif source == 2:
                    laborBEA()
                else:
                    print('Please enter a number from the menu.')
                    labor_update()
        elif section_or_sources == 999:
            exit()
        else:
            print('Please enter a number from the menu.')
            labor_update()
        while True:
            endProgram()

    #Cleaning Labor GeoFRED data
    def laborFRED(): #done
        clear()
        print('NC Data Dashboard Update\n-------------------------\nUpdating Labor GeoFRED\n\nLabor Sources:\n1-Civilian Labor Force\n2-People 25 and Over Education\n3-Resident Population\n4-Unemployment Rate\n\n999-Exit\n-------------------------')
        files = {1:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=39.98&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=656&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Persons&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=582&hideLegend=false', 2:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147149&attributes=Not+Seasonally+Adjusted%2C+Quarterly%2C+Percent&aggregationFrequency=Quarterly&aggregationType=Average&transformation=lin&date=2025-01-01&type=xls&startDate=1999-01-01&endDate=2025-01-01&mapWidth=999&mapHeight=521&hideLegend=false', 3:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147063&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Percent&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=2009-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=521&hideLegend=false', 4:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.78&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1549&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Thousands+of+Persons&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1970-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=582&hideLegend=false'}
        source = int(input('What source are you updating? '))
        if source == 1:
            print('Updating Civilian Labor Source')
            filename = './Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt'
            backup_fn = './Backups/STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 2:
            print('Updating People Under 25 Education Status')
            filename = './Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt'
            backup_fn = './Backups/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 3:
            print('Updating Resident Population')
            filename = './Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt'
            backup_fn = './Backups/STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 4:
            print('Updating Unemployment Rate')
            filename = './Updates/STG_FRED_Unemployment_Rate_by_County_Percent.txt'
            backup_fn = './Backups/STG_FRED_Unemployment_Rate_by_County_Percent_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 999:
            exit()
        else:
            print('Please enter a number from the menu.')
            laborFRED()
        while True:
            print('Connecting to database to publish data...')
            time.sleep(3)
            clear()
            labor_publish_FRED()

    #Cleaning Labor BEA data
    def laborBEA(): #done 
        #create new file for every dictionary entry
        files = {5:'https://apps.bea.gov/regional/zip/CAINC5N.zip', 6:'https://apps.bea.gov/regional/zip/CAINC6N.zip'}
        print('Updating Labor BEA\n-------------------------\nLabor BEA Sources:\n1-CAINC5N\n2-CAINC6N\n\n999-Exit\n-------------------------\n')
        key = int(input('What source are you updating? '))
        if key == 1:
            print('Updating CAINC5N Labor Version')
            for key, value in files.items():
                response = requests.get(value)
                zip = ZipFile(BytesIO(response.content))
                files = zip.namelist()
                with zip.open(files[34]) as csvfile:
                    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")
                    df.drop(df.tail(4).index,inplace=True)
                    df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
                    df.set_index(df['GeoFIPS'], inplace = True)
                    df.drop('GeoFIPS', axis = 1, inplace = True)           
                    linecodes = {10:'./Updates/STG_BEA__CA5N_PersonalIncome.txt', 20:'./Updates/STG_BEA_CA5N_Population.txt', 30:'./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt', 35:'./Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt'}
                    for key, value in linecodes.items():
                        filter1 = df['LineCode'] == key
                        df_filtered = df[filter1]
                        df_filtered.to_csv(value, sep = '\t')
                        print('\nUpdate Complete!')
        elif key == 2:  
            print('Updating CAINC6N Labor Version')  
            for key, value in files.items():
                response = requests.get('https://apps.bea.gov/regional/zip/CAINC6N.zip')
                zip = ZipFile(BytesIO(response.content))
                files = zip.namelist()
                with zip.open(files[34]) as csvfile:
                    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")
                    df.drop(df.tail(4).index,inplace=True)
                    df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
                    df.set_index(df['GeoFIPS'], inplace = True)
                    df.drop('GeoFIPS', axis = 1, inplace = True)
                    linecodes = {1:'./Updates/STG_BEA_CA6N_Compensation_of_Employees.txt', 5:'./Updates/STG_BEA_CA6N_Wages_and_Salaries.txt', 6:'./Updates/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries.txt', 7:'./Updates/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds.txt', 8:'./Updates/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance.txt', 9:'./Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt', 81:'./Updates/STG_BEA_CA6N_Farm_Compensation.txt', 82:'./Updates/STG_BEA_CA6N_NonFarm_Compensation.txt', 90:'./Updates/STG_BEA_CA6N_Private_Nonfarm_Compensation.txt', 100:'./Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt', 200:'./Updates/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt', 300:'./Updates/STG_BEA_CA6N_Utilities.txt', 400:'./Updates/STG_BEA_CA6N_Construction.txt', 500:'./Updates/STG_BEA_CA6N_Manufacturing.txt', 600:'./Updates/STG_BEA_CA6N_Wholesale_Trade.txt', 700:'./Updates/STG_BEA_CA6N_Retail_Trade.txt', 800:'./Updates/STG_BEA_CA6N_Transportation_and_Warehousing.txt', 900:'./Updates/STG_BEA_CA6N_Information.txt', 1000:'./Updates/STG_BEA_CA6N_Finance_and_Insurance.txt', 1100:'./Updates/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing.txt', 1200:'./Updates/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services.txt', 1300:'./Updates/STG_BEA_CA6N_Management_of_Companies_and_Enterprises.txt', 1400:'./Updates/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', 1500:'./Updates/STG_BEA_CA6N_Educational_Services.txt', 1600:'./Updates/STG_BEA_CA6N_Health_Care_and_Social_Assistance.txt', 1700:'./Updates/STG_BEA_CA6N_Arts_Entertainment_and_Recreation.txt', 1800:'./Updates/STG_BEA_CA6N_Accommodation_and_Food_Services.txt', 1900:'./Updates/STG_BEA_CA6N_Other_Services.txt', 2000:'./Updates/STG_BEA_CA6N_Government_and_Government_Enterprises.txt'}
                    for key, value in linecodes.items():
                        filter1 = df['LineCode'] == key
                        df_filtered = df[filter1]
                        df_filtered.to_csv(value, sep='\t')
                        print('\nUpdate Complete!')
                        pass
        elif key == 999:
            exit()
        else:
            print('Please enter a number from the menu.')
            laborBEA()
        while True:
            print('Connecting to database to publish data...')
            time.sleep(3)
            clear()
            labor_publish_CAINC5N()

    #Publishing Labor GeoFRED data
    def labor_publish_FRED(): #working, need send to Excel
        print('NC Data Dashboard Publish\n-------------------------\nPublishing Labor\n\nGeoFred sources:\n1-Civilian Labor Force\n2-People 25 and Over Education\n3-Resident Population\n4-Unemployment Rate\n\n999-Exit\n-------------------------')
        folder = int(input('What table are you publishing? '))
        if folder == 1:
            print('Publishing Civilian Labor Force')
            df = pd.read_csv('./Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_Civilian_Labor_Force_by_County_Persons','STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_Civilian_Labor_Force_by_County_Persons](
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                    r'Server=STEIN\ECONDEV;'
                                    r'Database=General_Vault;'
                                    r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_Civilian_Labor_Force_by_County_Persons', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 2:
            print('Publishing People 25 and Over Education')
            df = pd.read_csv('./Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent','STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP';''')
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                    r'Server=STEIN\ECONDEV;'
                                    r'Database=General_Vault;'
                                    r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 3:
            print('Publishing Resident Population')
            df = pd.read_csv('./Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_Resident_Population_by_County_Thousands_of_Persons','STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_Resident_Population_by_County_Thousands_of_Persons](
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                    r'Server=STEIN\ECONDEV;'
                                    r'Database=General_Vault;'
                                    r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_Resident_Population_by_County_Thousands_of_Persons', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 4:
            print('Publishing Unemployment Rate')
            df = pd.read_csv('./Updates/STG_FRED_Unemployment_Rate_by_County_Percent.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_Unemployment_Rate_by_County_Percent_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_Unemployment_Rate_by_County_Percent','STG_FRED_Unemployment_Rate_by_County_Percent_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_Unemployment_Rate_by_County_Percent](
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                    r'Server=STEIN\ECONDEV;'
                                    r'Database=General_Vault;'
                                    r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_Unemployment_Rate_by_County_Percent', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 999:
            exit()
        else:
            print('Please enter a number from the menu.')
            labor_publish_FRED()
        while True:
            endProgram()

    #Publishing Labor BEA data
    def labor_publish_CAINC5N(): #working, need send to Excel
        print('NC Data Dashboard Publish\n-------------------------\nPublishing Labor\n\nCAINC sources:\n1-Per Capita Personal Income\n2-Earnings by Place of Work\n3-Population\n4-Personal Income\n\n999-Exit\n-------------------------')
        folder = int(input('What table are you publishing? '))
        if folder == 1: #Per Capita Personal Income
            print('Publishing Per Capita Personal Income')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Per_Capita_Personal_Income_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Per_Capita_Personal_Income','STG_BEA_CA5N_Per_Capita_Personal_Income_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Per_Capita_Personal_Income](
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
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Per_Capita_Personal_Income', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 2: #Earnings by Place of Work
            print('Publishing Earnings by Place of Work')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Earnings_by_Place_of_Work_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Earnings_by_Place_of_Work','STG_BEA_CA5N_Earnings_by_Place_of_Work_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Earnings_by_Place_of_Work](
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
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Earnings_by_Place_of_Work', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 3: #Population
            print('Publishing Population')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Population.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Population_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Population','STG_BEA_CA5N_Population_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Population](
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
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Population', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 4: #Personal Income
            print('Publishing Personal Income')
            df = pd.read_csv('./Updates/STG_BEA_CA5N_Personal_Income.txt', sep = '\t')
            df = df.reset_index()
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_BEA_CA5N_Personal_Income_BACKUP')
            c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Personal_Income','STG_BEA_CA5N_Personal_Income_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_BEA_CA5N_Personal_Income](
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
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_BEA_CA5N_Personal_Income', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 999:
            exit()
        else:
            print('Please enter a number from the menu.')
            labor_publish_CAINC5N()
        while True:
            endProgram()  

##### Land ##### -- working, needs send to Excel

    #Updating Land section
    def land_update(): #done
        print('NC Data Dashboard Update\n-------------------------\nWelcome to Land!\n')
        section_or_sources = int(input('Menu:\n1-Section\n2-Individual Sources\n\n999-Exit\n-------------------------\nAre you updating the folder or individual sources? '))
        if section_or_sources == 1:
            print('Updating and publishing folder...')
            subprocess.call([r'land.bat'])
            pass
        elif section_or_sources == 2:
            clear()
            rounds = int(input('NC Data Dashboard Update\n-------------------------\nWelcome to Land!\n\nHow many files are you updating? '))
            for i in range(rounds):
                source = int(input('-------------------------\nLand Sources:\n\n1-Zillow\n2-GeoFred\n\n999-Exit\n-------------------------\nWhat source are you updating? '))
                if source == 1:
                    landZLLW()
                elif source == 2:
                    landFRED()
                elif source == 999:
                    exit()
                else:
                    print('Please enter a number from the menu.')
                    land_update()
        elif section_or_sources == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            land_update()
        while True:
            endProgram()

    #Cleaning Land GeoFRED data
    def landFRED(): #done
        clear()
        print('NC Data Dashboard Update\n-------------------------\nUpdating Land GeoFRED\n\nLand Sources:\n1-All Transactions House Price Index\n2-Homeownership Rate\n3-New Private Housing\n\n999-Exit\n-------------------------')
        files = {1:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90&lat=40&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=942&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Index+2000%3D100&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1975-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=1249&hideLegend=false', 2:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=157125&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Rate&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=2009-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=521&hideLegend=false', 3:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=155206&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Units&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=521&hideLegend=false'}
        source = int(input('What source are you updating? '))
        if source == 1:
            print('Updating All Transactions House Price Index')
            filename = './Updates/STG_FRED_All_Transactions_House_Price_Index.txt'
            backup_fn = './Backups/STG_FRED_All_Transactions_House_Price_Index_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 2:
            print('Updating Homeownership Rate')
            filename = './Updates/STG_FRED_Homeownership_Rate_by_County.txt'
            backup_fn = './Backups/STG_FRED_Homeownership_Rate_by_County_BACKUP.txt'
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 3:
            print('Updating New Private Housing')
            filename = './Updates/STG_FRED_New_Private_Housing_Structures.txt'
            backup_fn = './Backups/STG_FRED_New_Private_Housing_Structures_BACKUP.txt'
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_filter = df['Region Name'].str.contains(', NC')
                df = df[region_filter]
                df.set_index(df['Series ID'], inplace = True)
                df.drop('Series ID', axis = 1, inplace = True)
                df.to_csv(filename, sep = '\t')
                print('\nUpdate Complete!')
                pass
        elif source == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            landFRED()
        while True:
            print('Connecting to database to publish data...')
            time.sleep(3)
            clear()
            land_publish_FRED()

    #Cleaning Land Zillow data
    def landZLLW(): #done
        clear()
        print('NC Data Dashboard Update\n-------------------------\nUpdating Zillow\n\nZillow Sources:\n1-Median Sale Price\n2-Median Value Per Sqft\n3-Zhvi\n\n999-Exit\n-------------------------')
        source = int(input('What source are you updating? '))
        if source == 1:
            print('Updating Median Sale Price')
            filename = './Updates/STG_ZLLW_County_MedianSalePrice_AllHomes.txt'
            backup_fn = './Backups/STG_ZLLW_County_MedianSalePrice_AllHomes_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            df_fips = pd.read_csv('./FIPS_Codes.csv')
            df = pd.read_csv('http://files.zillowstatic.com/research/public/County/Sale_Prices_County.csv', encoding='ISO-8859-1')
            df = df.drop(columns = ['RegionID'], axis = 1)
            state_filter = df['StateName'] == "North Carolina"
            df = df[state_filter]
            df = df.sort_values('RegionName', ascending = True)
            df_join = df.set_index('RegionName').join(df_fips.set_index('RegionName'))
            df_join.loc[ :, 'MunicipalCodeFIPS'] = df_join['MunicipalCodeFIPS'].astype(str)
            df_join.loc[ :, 'MunicipalCodeFIPS'] = df_join['MunicipalCodeFIPS'].str.zfill(3)
            columns = ['State','Metro','StateCodeFIPS','MunicipalCodeFIPS','SizeRank','2008-03','2008-04','2008-05','2008-06','2008-07','2008-08','2008-09','2008-10','2008-11','2008-12','2009-01','2009-02','2009-03','2009-04','2009-05','2009-06','2009-07','2009-08','2009-09','2009-10','2009-11','2009-12','2010-01','2010-02','2010-03','2010-04','2010-05','2010-06','2010-07','2010-08','2010-09','2010-10','2010-11','2010-12','2011-01','2011-02','2011-03','2011-04','2011-05','2011-06','2011-07','2011-08','2011-09','2011-10','2011-11','2011-12','2012-01','2012-02','2012-03','2012-04','2012-05','2012-06','2012-07','2012-08','2012-09','2012-10','2012-11','2012-12','2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12','2016-01','2016-02','2016-03','2016-04','2016-05','2016-06','2016-07','2016-08','2016-09','2016-10','2016-11','2016-12','2017-01','2017-02','2017-03','2017-04','2017-05','2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01','2018-02','2018-03','2018-04','2018-05','2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01', '2020-02']
            df_join = df_join[columns]
            df_join.to_csv(filename, sep ='\t')
            print('\nUpdate Complete!')
            pass
        elif source == 2:
            print('Updating Median Value Per Sqft')
            filename = './Updates/STG_ZLLW_County_MedianValuePerSqft_AllHomes.txt'
            backup_fn = './Backups/STG_ZLLW_County_MedianValuePerSqft_AllHomes_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            df = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianValuePerSqft_AllHomes.csv', encoding='ISO-8859-1')
            state_filter = df['State'] == 'NC'
            df = df[state_filter]
            df.loc[:, 'MunicipalCodeFIPS'] = df['MunicipalCodeFIPS'].astype(str)
            df.loc[:, 'MunicipalCodeFIPS'] = df['MunicipalCodeFIPS'].str.zfill(3)
            df.set_index(df['RegionName'], inplace=True)
            df.drop('RegionName', axis=1, inplace=True)
            df.to_csv(filename, sep='\t')
            print('\nUpdate Complete!')
            pass
        elif source == 3:
            print('Updating Zhvi')
            filename = './Updates/STG_ZLLW_County_Zhvi_AllHomes.txt'
            backup_fn = './Backups/STG_ZLLW_County_Zhvi_AllHomes_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            df = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_Zhvi_AllHomes.csv', encoding='ISO-8859-1')
            state_filter = df['State'] == 'NC'
            df = df[state_filter]
            df.loc[:, 'MunicipalCodeFIPS'] = df['MunicipalCodeFIPS'].astype(str)
            df.loc[:, 'MunicipalCodeFIPS'] = df['MunicipalCodeFIPS'].str.zfill(3)
            df.set_index(df['RegionName'], inplace=True)
            df.drop('RegionName', axis=1, inplace=True)
            df.to_csv(filename, sep='\t')
            print('\nUpdate Complete!')
            pass
        elif source == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            landZLLW()
        while True:
            print('Connecting to database to publish data...')
            time.sleep(3)
            clear()
            land_publish_zllw()

    #Publishing Land FRED data
    def land_publish_FRED(): #working, need send to Excel
        print('NC Data Dashboard Publish\n-------------------------\nPublishing Land\n\nGeoFred sources:\n1-All Transactions House Price Index\n2-Homeownership Rate\n3-New Private Housing\n\n999-Exit\n-------------------------')
        folder = int(input('What table are you publishing? '))
        if folder == 1: #All Transactions House Price Index
            print('Publishing All Transactions House Price Index')
            df = pd.read_csv('./Updates/STG_FRED_All_Transactions_House_Price_Index.txt', sep = '\t')
            df = df.reset_index() 
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_All_Transactions_House_Price_Index_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_All_Transactions_House_Price_Index','STG_FRED_All_Transactions_House_Price_Index_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_All_Transactions_House_Price_Index](
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_All_Transactions_House_Price_Index', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 2: #Homeownership Rate
            print('Publishing Homeownership Rate')
            df = pd.read_csv('./Updates/STG_FRED_Homeownership_Rate_by_County.txt', sep = '\t')
            df = df.reset_index() 
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_Homeownership_Rate_by_County_BACKUP')
            c.execute('''sp_rename 'dbo.STG_FRED_Homeownership_Rate_by_County','STG_FRED_Homeownership_Rate_by_County_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_Homeownership_Rate_by_County](
                [Series ID] [varchar](14) NULL,
                [Region Name] [varchar](23) NULL,
                [Region Code] [int] NULL,
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
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_Homeownership_Rate_by_County', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 3: #New Private Housing
            print('Publishing New Private Housing')
            df = pd.read_csv('./Updates/STG_FRED_New_Private_Housing_Structures.txt', sep = '\t')
            df = df.reset_index() 
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_FRED_New_Private_Housing_Structures_BACKUP') 
            c.execute('''sp_rename 'dbo.STG_FRED_New_Private_Housing_Structures','STG_FRED_New_Private_Housing_Structures_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_FRED_New_Private_Housing_Structures](
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
                [2025] [float] NULL,
                [2026] [float] NULL,
                [2027] [float] NULL,
                [2028] [float] NULL,
                [2029] [float] NULL,
                [2030] [float] NULL
            ) ON [PRIMARY]''')
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=General_Vault;'
                                 r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_FRED_New_Private_Housing_Structures', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Please enter a number from the menu.')
            land_publish_FRED()
        while True:
            endProgram()

    #Publishing Land ZLLW data
    def land_publish_zllw(): #working, need send to Excel
        print('NC Data Dashboard Publish\n-------------------------\nPublishing Land\n\nZillow sources:\n1-Median Sale Price\n2-Median Value Per Sqft\n3-Zhvi\n\n999-Exit\n-------------------------')
        folder = int(input('What table are you publishing? '))
        if folder == 1:
            print('Publishing Median Sale Price')
            df = pd.read_csv('./Updates/STG_ZLLW_County_MedianSalePrice_AllHomes.txt', sep='\t')
            df = df.reset_index()
            df['Metro'] = df['Metro'].replace(np.nan,'', regex=True)
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_ZLLW_County_MedianSalePrice_AllHomes_BACKUP')
            c.execute('''sp_rename 'dbo.STG_ZLLW_County_MedianSalePrice_AllHomes','STG_ZLLW_County_MedianSalePrice_AllHomes_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_ZLLW_County_MedianSalePrice_AllHomes](
                [RegionName] [varchar](19) NULL,
                [RegionID] [smallint] NULL,
                [State] [varchar](2) NULL,
                [Metro] [varchar](38) NULL,
                [StateCodeFIPS] [varchar](2) NULL,
                [MunicipalCodeFIPS] [varchar](3) NULL,
                [SizeRank] [smallint] NULL,
                [1996-04] [float] NULL,
                [1996-05] [float] NULL,
                [1996-06] [float] NULL,
                [1996-07] [float] NULL,
                [1996-08] [float] NULL,
                [1996-09] [float] NULL,
                [1996-10] [float] NULL,
                [1996-11] [float] NULL,
                [1996-12] [float] NULL,
                [1997-01] [float] NULL,
                [1997-02] [float] NULL,
                [1997-03] [float] NULL,
                [1997-04] [float] NULL,
                [1997-05] [float] NULL,
                [1997-06] [float] NULL,
                [1997-07] [float] NULL,
                [1997-08] [float] NULL,
                [1997-09] [float] NULL,
                [1997-10] [float] NULL,
                [1997-11] [float] NULL,
                [1997-12] [float] NULL,
                [1998-01] [float] NULL,
                [1998-02] [float] NULL,
                [1998-03] [float] NULL,
                [1998-04] [float] NULL,
                [1998-05] [float] NULL,
                [1998-06] [float] NULL,
                [1998-07] [float] NULL,
                [1998-08] [float] NULL,
                [1998-09] [float] NULL,
                [1998-10] [float] NULL,
                [1998-11] [float] NULL,
                [1998-12] [float] NULL,
                [1999-01] [float] NULL,
                [1999-02] [float] NULL,
                [1999-03] [float] NULL,
                [1999-04] [float] NULL,
                [1999-05] [float] NULL,
                [1999-06] [float] NULL,
                [1999-07] [float] NULL,
                [1999-08] [float] NULL,
                [1999-09] [float] NULL,
                [1999-10] [float] NULL,
                [1999-11] [float] NULL,
                [1999-12] [float] NULL,
                [2000-01] [float] NULL,
                [2000-02] [float] NULL,
                [2000-03] [float] NULL,
                [2000-04] [float] NULL,
                [2000-05] [float] NULL,
                [2000-06] [float] NULL,
                [2000-07] [float] NULL,
                [2000-08] [float] NULL,
                [2000-09] [float] NULL,
                [2000-10] [float] NULL,
                [2000-11] [float] NULL,
                [2000-12] [float] NULL,
                [2001-01] [float] NULL,
                [2001-02] [float] NULL,
                [2001-03] [float] NULL,
                [2001-04] [float] NULL,
                [2001-05] [float] NULL,
                [2001-06] [float] NULL,
                [2001-07] [float] NULL,
                [2001-08] [float] NULL,
                [2001-09] [float] NULL,
                [2001-10] [float] NULL,
                [2001-11] [float] NULL,
                [2001-12] [float] NULL,
                [2002-01] [float] NULL,
                [2002-02] [float] NULL,
                [2002-03] [float] NULL,
                [2002-04] [float] NULL,
                [2002-05] [float] NULL,
                [2002-06] [float] NULL,
                [2002-07] [float] NULL,
                [2002-08] [float] NULL,
                [2002-09] [float] NULL,
                [2002-10] [float] NULL,
                [2002-11] [float] NULL,
                [2002-12] [float] NULL,
                [2003-01] [float] NULL,
                [2003-02] [float] NULL,
                [2003-03] [float] NULL,
                [2003-04] [float] NULL,
                [2003-05] [float] NULL,
                [2003-06] [float] NULL,
                [2003-07] [float] NULL,
                [2003-08] [float] NULL,
                [2003-09] [float] NULL,
                [2003-10] [float] NULL,
                [2003-11] [float] NULL,
                [2003-12] [float] NULL,
                [2004-01] [float] NULL,
                [2004-02] [float] NULL,
                [2004-03] [float] NULL,
                [2004-04] [float] NULL,
                [2004-05] [float] NULL,
                [2004-06] [float] NULL,
                [2004-07] [float] NULL,
                [2004-08] [float] NULL,
                [2004-09] [float] NULL,
                [2004-10] [float] NULL,
                [2004-11] [float] NULL,
                [2004-12] [float] NULL,
                [2005-01] [float] NULL,
                [2005-02] [float] NULL,
                [2005-03] [float] NULL,
                [2005-04] [float] NULL,
                [2005-05] [float] NULL,
                [2005-06] [float] NULL,
                [2005-07] [float] NULL,
                [2005-08] [float] NULL,
                [2005-09] [float] NULL,
                [2005-10] [float] NULL,
                [2005-11] [float] NULL,
                [2005-12] [float] NULL,
                [2006-01] [float] NULL,
                [2006-02] [float] NULL,
                [2006-03] [float] NULL,
                [2006-04] [float] NULL,
                [2006-05] [float] NULL,
                [2006-06] [float] NULL,
                [2006-07] [float] NULL,
                [2006-08] [float] NULL,
                [2006-09] [float] NULL,
                [2006-10] [float] NULL,
                [2006-11] [float] NULL,
                [2006-12] [float] NULL,
                [2007-01] [float] NULL,
                [2007-02] [float] NULL,
                [2007-03] [float] NULL,
                [2007-04] [float] NULL,
                [2007-05] [float] NULL,
                [2007-06] [float] NULL,
                [2007-07] [float] NULL,
                [2007-08] [float] NULL,
                [2007-09] [float] NULL,
                [2007-10] [float] NULL,
                [2007-11] [float] NULL,
                [2007-12] [float] NULL,
                [2008-01] [float] NULL,
                [2008-02] [float] NULL,
                [2008-03] [float] NULL,
                [2008-04] [float] NULL,
                [2008-05] [float] NULL,
                [2008-06] [float] NULL,
                [2008-07] [float] NULL,
                [2008-08] [float] NULL,
                [2008-09] [float] NULL,
                [2008-10] [float] NULL,
                [2008-11] [float] NULL,
                [2008-12] [float] NULL,
                [2009-01] [float] NULL,
                [2009-02] [float] NULL,
                [2009-03] [float] NULL,
                [2009-04] [float] NULL,
                [2009-05] [float] NULL,
                [2009-06] [float] NULL,
                [2009-07] [float] NULL,
                [2009-08] [float] NULL,
                [2009-09] [float] NULL,
                [2009-10] [float] NULL,
                [2009-11] [float] NULL,
                [2009-12] [float] NULL,
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
            params = urllib.parse.quote_plus(r'Driver={SQL Server};'
                                            r'Server=STEIN\ECONDEV;'
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_ZLLW_County_MedianSalePrice_AllHomes', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 2:
            print('Publishing Median Value Per Sqft')
            df = pd.read_csv('./Updates/STG_ZLLW_County_MedianValuePerSqft_AllHomes.txt', sep='\t')
            df['Metro'] = df['Metro'].replace(np.nan,'', regex=True)
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_ZLLW_County_MedianValuePerSqft_AllHomes_BACKUP')
            c.execute('''sp_rename 'dbo.STG_ZLLW_County_MedianValuePerSqft_AllHomes','STG_ZLLW_County_MedianValuePerSqft_AllHomes_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_ZLLW_County_MedianValuePerSqft_AllHomes](
                [RegionName] [varchar](19) NULL,
                [RegionID] [smallint] NULL,
                [State] [varchar](2) NULL,
                [Metro] [varchar](38) NULL,
                [StateCodeFIPS] [varchar](2) NULL,
                [MunicipalCodeFIPS] [varchar](3) NULL,
                [SizeRank] [smallint] NULL,
                [1996-04] [float] NULL,
                [1996-05] [float] NULL,
                [1996-06] [float] NULL,
                [1996-07] [float] NULL,
                [1996-08] [float] NULL,
                [1996-09] [float] NULL,
                [1996-10] [float] NULL,
                [1996-11] [float] NULL,
                [1996-12] [float] NULL,
                [1997-01] [float] NULL,
                [1997-02] [float] NULL,
                [1997-03] [float] NULL,
                [1997-04] [float] NULL,
                [1997-05] [float] NULL,
                [1997-06] [float] NULL,
                [1997-07] [float] NULL,
                [1997-08] [float] NULL,
                [1997-09] [float] NULL,
                [1997-10] [float] NULL,
                [1997-11] [float] NULL,
                [1997-12] [float] NULL,
                [1998-01] [float] NULL,
                [1998-02] [float] NULL,
                [1998-03] [float] NULL,
                [1998-04] [float] NULL,
                [1998-05] [float] NULL,
                [1998-06] [float] NULL,
                [1998-07] [float] NULL,
                [1998-08] [float] NULL,
                [1998-09] [float] NULL,
                [1998-10] [float] NULL,
                [1998-11] [float] NULL,
                [1998-12] [float] NULL,
                [1999-01] [float] NULL,
                [1999-02] [float] NULL,
                [1999-03] [float] NULL,
                [1999-04] [float] NULL,
                [1999-05] [float] NULL,
                [1999-06] [float] NULL,
                [1999-07] [float] NULL,
                [1999-08] [float] NULL,
                [1999-09] [float] NULL,
                [1999-10] [float] NULL,
                [1999-11] [float] NULL,
                [1999-12] [float] NULL,
                [2000-01] [float] NULL,
                [2000-02] [float] NULL,
                [2000-03] [float] NULL,
                [2000-04] [float] NULL,
                [2000-05] [float] NULL,
                [2000-06] [float] NULL,
                [2000-07] [float] NULL,
                [2000-08] [float] NULL,
                [2000-09] [float] NULL,
                [2000-10] [float] NULL,
                [2000-11] [float] NULL,
                [2000-12] [float] NULL,
                [2001-01] [float] NULL,
                [2001-02] [float] NULL,
                [2001-03] [float] NULL,
                [2001-04] [float] NULL,
                [2001-05] [float] NULL,
                [2001-06] [float] NULL,
                [2001-07] [float] NULL,
                [2001-08] [float] NULL,
                [2001-09] [float] NULL,
                [2001-10] [float] NULL,
                [2001-11] [float] NULL,
                [2001-12] [float] NULL,
                [2002-01] [float] NULL,
                [2002-02] [float] NULL,
                [2002-03] [float] NULL,
                [2002-04] [float] NULL,
                [2002-05] [float] NULL,
                [2002-06] [float] NULL,
                [2002-07] [float] NULL,
                [2002-08] [float] NULL,
                [2002-09] [float] NULL,
                [2002-10] [float] NULL,
                [2002-11] [float] NULL,
                [2002-12] [float] NULL,
                [2003-01] [float] NULL,
                [2003-02] [float] NULL,
                [2003-03] [float] NULL,
                [2003-04] [float] NULL,
                [2003-05] [float] NULL,
                [2003-06] [float] NULL,
                [2003-07] [float] NULL,
                [2003-08] [float] NULL,
                [2003-09] [float] NULL,
                [2003-10] [float] NULL,
                [2003-11] [float] NULL,
                [2003-12] [float] NULL,
                [2004-01] [float] NULL,
                [2004-02] [float] NULL,
                [2004-03] [float] NULL,
                [2004-04] [float] NULL,
                [2004-05] [float] NULL,
                [2004-06] [float] NULL,
                [2004-07] [float] NULL,
                [2004-08] [float] NULL,
                [2004-09] [float] NULL,
                [2004-10] [float] NULL,
                [2004-11] [float] NULL,
                [2004-12] [float] NULL,
                [2005-01] [float] NULL,
                [2005-02] [float] NULL,
                [2005-03] [float] NULL,
                [2005-04] [float] NULL,
                [2005-05] [float] NULL,
                [2005-06] [float] NULL,
                [2005-07] [float] NULL,
                [2005-08] [float] NULL,
                [2005-09] [float] NULL,
                [2005-10] [float] NULL,
                [2005-11] [float] NULL,
                [2005-12] [float] NULL,
                [2006-01] [float] NULL,
                [2006-02] [float] NULL,
                [2006-03] [float] NULL,
                [2006-04] [float] NULL,
                [2006-05] [float] NULL,
                [2006-06] [float] NULL,
                [2006-07] [float] NULL,
                [2006-08] [float] NULL,
                [2006-09] [float] NULL,
                [2006-10] [float] NULL,
                [2006-11] [float] NULL,
                [2006-12] [float] NULL,
                [2007-01] [float] NULL,
                [2007-02] [float] NULL,
                [2007-03] [float] NULL,
                [2007-04] [float] NULL,
                [2007-05] [float] NULL,
                [2007-06] [float] NULL,
                [2007-07] [float] NULL,
                [2007-08] [float] NULL,
                [2007-09] [float] NULL,
                [2007-10] [float] NULL,
                [2007-11] [float] NULL,
                [2007-12] [float] NULL,
                [2008-01] [float] NULL,
                [2008-02] [float] NULL,
                [2008-03] [float] NULL,
                [2008-04] [float] NULL,
                [2008-05] [float] NULL,
                [2008-06] [float] NULL,
                [2008-07] [float] NULL,
                [2008-08] [float] NULL,
                [2008-09] [float] NULL,
                [2008-10] [float] NULL,
                [2008-11] [float] NULL,
                [2008-12] [float] NULL,
                [2009-01] [float] NULL,
                [2009-02] [float] NULL,
                [2009-03] [float] NULL,
                [2009-04] [float] NULL,
                [2009-05] [float] NULL,
                [2009-06] [float] NULL,
                [2009-07] [float] NULL,
                [2009-08] [float] NULL,
                [2009-09] [float] NULL,
                [2009-10] [float] NULL,
                [2009-11] [float] NULL,
                [2009-12] [float] NULL,
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
            params = urllib.parse.quote_plus(r'Driver={SQL Server};'
                                            r'Server=STEIN\ECONDEV;'
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_ZLLW_County_MedianValuePerSqft_AllHomes', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 3: 
            print('Publishing Zhvi')
            df = pd.read_csv('./Updates/STG_ZLLW_County_Zhvi_AllHomes.txt', sep='\t')
            df['Metro'] = df['Metro'].replace(np.nan,'', regex=True)
            column_list = df.columns.values
            for i in column_list:
                df.loc[df[i].isnull(),i]=0
            c.execute('drop table STG_ZLLW_County_Zhvi_AllHomes_BACKUP')
            c.execute('''sp_rename 'dbo.STG_ZLLW_County_Zhvi_AllHomes','STG_ZLLW_County_Zhvi_AllHomes_BACKUP';''')
            c.execute('''USE [STG2]
            SET ANSI_NULLS ON
            SET QUOTED_IDENTIFIER ON
            CREATE TABLE [dbo].[STG_ZLLW_County_Zhvi_AllHomes](
                [RegionName] [varchar](19) NULL,
                [RegionID] [smallint] NULL,
                [State] [varchar](2) NULL,
                [Metro] [varchar](38) NULL,
                [StateCodeFIPS] [varchar](2) NULL,
                [MunicipalCodeFIPS] [varchar](3) NULL,
                [SizeRank] [smallint] NULL,
                [1996-04] [float] NULL,
                [1996-05] [float] NULL,
                [1996-06] [float] NULL,
                [1996-07] [float] NULL,
                [1996-08] [float] NULL,
                [1996-09] [float] NULL,
                [1996-10] [float] NULL,
                [1996-11] [float] NULL,
                [1996-12] [float] NULL,
                [1997-01] [float] NULL,
                [1997-02] [float] NULL,
                [1997-03] [float] NULL,
                [1997-04] [float] NULL,
                [1997-05] [float] NULL,
                [1997-06] [float] NULL,
                [1997-07] [float] NULL,
                [1997-08] [float] NULL,
                [1997-09] [float] NULL,
                [1997-10] [float] NULL,
                [1997-11] [float] NULL,
                [1997-12] [float] NULL,
                [1998-01] [float] NULL,
                [1998-02] [float] NULL,
                [1998-03] [float] NULL,
                [1998-04] [float] NULL,
                [1998-05] [float] NULL,
                [1998-06] [float] NULL,
                [1998-07] [float] NULL,
                [1998-08] [float] NULL,
                [1998-09] [float] NULL,
                [1998-10] [float] NULL,
                [1998-11] [float] NULL,
                [1998-12] [float] NULL,
                [1999-01] [float] NULL,
                [1999-02] [float] NULL,
                [1999-03] [float] NULL,
                [1999-04] [float] NULL,
                [1999-05] [float] NULL,
                [1999-06] [float] NULL,
                [1999-07] [float] NULL,
                [1999-08] [float] NULL,
                [1999-09] [float] NULL,
                [1999-10] [float] NULL,
                [1999-11] [float] NULL,
                [1999-12] [float] NULL,
                [2000-01] [float] NULL,
                [2000-02] [float] NULL,
                [2000-03] [float] NULL,
                [2000-04] [float] NULL,
                [2000-05] [float] NULL,
                [2000-06] [float] NULL,
                [2000-07] [float] NULL,
                [2000-08] [float] NULL,
                [2000-09] [float] NULL,
                [2000-10] [float] NULL,
                [2000-11] [float] NULL,
                [2000-12] [float] NULL,
                [2001-01] [float] NULL,
                [2001-02] [float] NULL,
                [2001-03] [float] NULL,
                [2001-04] [float] NULL,
                [2001-05] [float] NULL,
                [2001-06] [float] NULL,
                [2001-07] [float] NULL,
                [2001-08] [float] NULL,
                [2001-09] [float] NULL,
                [2001-10] [float] NULL,
                [2001-11] [float] NULL,
                [2001-12] [float] NULL,
                [2002-01] [float] NULL,
                [2002-02] [float] NULL,
                [2002-03] [float] NULL,
                [2002-04] [float] NULL,
                [2002-05] [float] NULL,
                [2002-06] [float] NULL,
                [2002-07] [float] NULL,
                [2002-08] [float] NULL,
                [2002-09] [float] NULL,
                [2002-10] [float] NULL,
                [2002-11] [float] NULL,
                [2002-12] [float] NULL,
                [2003-01] [float] NULL,
                [2003-02] [float] NULL,
                [2003-03] [float] NULL,
                [2003-04] [float] NULL,
                [2003-05] [float] NULL,
                [2003-06] [float] NULL,
                [2003-07] [float] NULL,
                [2003-08] [float] NULL,
                [2003-09] [float] NULL,
                [2003-10] [float] NULL,
                [2003-11] [float] NULL,
                [2003-12] [float] NULL,
                [2004-01] [float] NULL,
                [2004-02] [float] NULL,
                [2004-03] [float] NULL,
                [2004-04] [float] NULL,
                [2004-05] [float] NULL,
                [2004-06] [float] NULL,
                [2004-07] [float] NULL,
                [2004-08] [float] NULL,
                [2004-09] [float] NULL,
                [2004-10] [float] NULL,
                [2004-11] [float] NULL,
                [2004-12] [float] NULL,
                [2005-01] [float] NULL,
                [2005-02] [float] NULL,
                [2005-03] [float] NULL,
                [2005-04] [float] NULL,
                [2005-05] [float] NULL,
                [2005-06] [float] NULL,
                [2005-07] [float] NULL,
                [2005-08] [float] NULL,
                [2005-09] [float] NULL,
                [2005-10] [float] NULL,
                [2005-11] [float] NULL,
                [2005-12] [float] NULL,
                [2006-01] [float] NULL,
                [2006-02] [float] NULL,
                [2006-03] [float] NULL,
                [2006-04] [float] NULL,
                [2006-05] [float] NULL,
                [2006-06] [float] NULL,
                [2006-07] [float] NULL,
                [2006-08] [float] NULL,
                [2006-09] [float] NULL,
                [2006-10] [float] NULL,
                [2006-11] [float] NULL,
                [2006-12] [float] NULL,
                [2007-01] [float] NULL,
                [2007-02] [float] NULL,
                [2007-03] [float] NULL,
                [2007-04] [float] NULL,
                [2007-05] [float] NULL,
                [2007-06] [float] NULL,
                [2007-07] [float] NULL,
                [2007-08] [float] NULL,
                [2007-09] [float] NULL,
                [2007-10] [float] NULL,
                [2007-11] [float] NULL,
                [2007-12] [float] NULL,
                [2008-01] [float] NULL,
                [2008-02] [float] NULL,
                [2008-03] [float] NULL,
                [2008-04] [float] NULL,
                [2008-05] [float] NULL,
                [2008-06] [float] NULL,
                [2008-07] [float] NULL,
                [2008-08] [float] NULL,
                [2008-09] [float] NULL,
                [2008-10] [float] NULL,
                [2008-11] [float] NULL,
                [2008-12] [float] NULL,
                [2009-01] [float] NULL,
                [2009-02] [float] NULL,
                [2009-03] [float] NULL,
                [2009-04] [float] NULL,
                [2009-05] [float] NULL,
                [2009-06] [float] NULL,
                [2009-07] [float] NULL,
                [2009-08] [float] NULL,
                [2009-09] [float] NULL,
                [2009-10] [float] NULL,
                [2009-11] [float] NULL,
                [2009-12] [float] NULL,
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
            params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                            r'Server=STEIN\ECONDEV;'
                                            r'Database=General_Vault;'
                                            r'Trusted_Connection=yes;')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('STG_ZLLW_County_Zhvi_AllHomes', con=engine, if_exists='replace', index=False)
            print('Published.')
            pass
        elif folder == 999:
            print('Have a nice day!')
            exit()          
        else:
            print('Please enter a number from the menu.')
            land_publish_zllw()
        while True:
            endProgram()

##### Natural Products ##### -- working, needs SQL

    #Updating Natural Products section
    def natproducts_update(): #done
        print('NC Data Dashboard Update\n-------------------------\nWelcome to Natural Products!')
        df = pd.read_excel('./Data/TableauData_NC_NaturalProducts_Section.xlsx')
        column_list = df.columns.values
        for i in column_list: 
            df.loc[df[i].isnull(),i]=''
            df.to_csv('./Updates/STG_Natural_Products.txt', sep='\t')
        while True:
            endProgram()

    #Publishing Natural Products data
    def natproducts_publish(): #building, need SQL
        while True:
            endProgram()

    while True:
        runProgram()
        
#Catch Ctrl-C user command
except KeyboardInterrupt:
        print('\n-------------------------\nEnding program...')
        time.sleep(3)
        clear()
        exit()

#Catch non-numeric error   
except ValueError:
        print('Please enter a numeric value.')
        runProgram()