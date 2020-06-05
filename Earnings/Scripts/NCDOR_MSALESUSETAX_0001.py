#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests

# Create backups
df_backup = pd.read_csv('./Updates/STG_BEA_MSALESUSETAX_0001.txt')
df_backup.to_csv('./Backups/STG_BEA_MSALESUSETAX_0001_BACKUP.txt')


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_10-18.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'Sales*', 'Sales*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'10-01-18'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'10-01-18'})
# append dataframes
df_list = [df, df1]
df_master0 = df.append(df_list)
df_master0['Date'] = '10/01/2018'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master0 = df_master0.dropna(how='all')
df_master0 = df_master0.fillna('0')
# Change dtypes to Int
df_master0['10-01-18'] = df_master0['10-01-18'].astype(float)
# Drop junk rows
df_master0 = df_master0[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_11-18.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'Sales*', 'Sales*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'11-01-18'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'11-01-18'})
# append dataframes
df_list = [df, df1]
df_master1 = df.append(df_list)
df_master1['Date'] = '11/01/2018'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master1 = df_master1.dropna(how='all')
df_master1 = df_master1.fillna('0')
# Change dtypes to Int
df_master1['11-01-18'] = df_master1['11-01-18'].astype(float)
# Drop junk rows
df_master1 = df_master1[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_12-18.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'Sales*', 'Sales*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'12-01-18'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'12-01-18'})
# append dataframes
df_list = [df, df1]
df_master2 = df.append(df_list)
df_master2['Date'] = '12/10/2018'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master2 = df_master2.dropna(how='all')
df_master2 = df_master2.fillna('0')
# Change dtypes to Int
df_master2['12-01-18'] = df_master2['12-01-18'].astype(float)
# Drop junk rows
df_master2 = df_master2[:-10]




df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_1-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'01-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'01-01-19'})
# append dataframes
df_list = [df, df1]
df_master3 = df.append(df_list)
df_master3['Date'] = '01/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master3 = df_master3.dropna(how='all')
df_master3 = df_master3.fillna('0')
# Change dtypes to Int
df_master3['01-01-19'] = df_master3['01-01-19'].astype(float)
# Drop junk rows
df_master3 = df_master3[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_2-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'02-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'02-01-19'})
# append dataframes
df_list = [df, df1]
df_master4 = df.append(df_list)
df_master4['Date'] = '02/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master4 = df_master4.dropna(how='all')
df_master4 = df_master4.fillna('0')
# Change dtypes to Int
df_master4['02-01-19'] = df_master4['02-01-19'].astype(float)
# Drop junk rows
df_master4 = df_master4[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_3-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'03-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'03-01-19'})
# append dataframes
df_list = [df, df1]
df_master5 = df.append(df_list)
df_master5['Date'] = '03/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master5 = df_master5.dropna(how='all')
df_master5 = df_master5.fillna('0')
# Change dtypes to Int
df_master5['03-01-19'] = df_master5['03-01-19'].astype(float)
# Drop junk rows
df_master5 = df_master5[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_4-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'04-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'04-01-19'})
# append dataframes
df_list = [df, df1]
df_master6 = df.append(df_list)
df_master6['Date'] = '04/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master6 = df_master6.dropna(how='all')
df_master6 = df_master6.fillna('0')
# Change dtypes to Int
df_master6['04-01-19'] = df_master6['04-01-19'].astype(float)
# Drop junk rows
df_master6 = df_master6[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_5-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'05-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'05-01-19'})
# append dataframes
df_list = [df, df1]
df_master7 = df.append(df_list)
df_master7['Date'] = '05/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master7 = df_master7.dropna(how='all')
df_master7 = df_master7.fillna('0')
# Change dtypes to Int
df_master7['05-01-19'] = df_master7['05-01-19'].astype(float)
# Drop junk rows
df_master7 = df_master7[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_6-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'06-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'06-01-19'})
# append dataframes
df_list = [df, df1]
df_master8 = df.append(df_list)
df_master8['Date'] = '06/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master8 = df_master8.dropna(how='all')
df_master8 = df_master8.fillna('0')
# Change dtypes to Int
df_master8['06-01-19'] = df_master8['06-01-19'].astype(float)
# Drop junk rows
df_master8 = df_master8[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_7-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'07-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'07-01-19'})
# append dataframes
df_list = [df, df1]
df_master9 = df.append(df_list)
df_master9['Date'] = '07/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master9 = df_master9.dropna(how='all')
df_master9 = df_master9.fillna('0')
# Change dtypes to Int
df_master9['07-01-19'] = df_master9['07-01-19'].astype(float)
# Drop junk rows
df_master9 = df_master9[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_8-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'08-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'08-01-19'})
# append dataframes
df_list = [df, df1]
df_master10 = df.append(df_list)
df_master10['Date'] = '08/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master10 = df_master10.dropna(how='all')
df_master10 = df_master10.fillna('0')
# Change dtypes to Int
df_master10['08-01-19'] = df_master10['08-01-19'].astype(float)
# Drop junk rows
df_master10 = df_master10[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_9-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'09-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'09-01-19'})
# append dataframes
df_list = [df, df1]
df_master11 = df.append(df_list)
df_master11['Date'] = '09/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master11 = df_master11.dropna(how='all')
df_master11 = df_master11.fillna('0')
# Change dtypes to Int
df_master11['09-01-19'] = df_master11['09-01-19'].astype(float)
# Drop junk rows
df_master11 = df_master11[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_10-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'10-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'10-01-19'})
# append dataframes
df_list = [df, df1]
df_master12 = df.append(df_list)
df_master12['Date'] = '10/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master12 = df_master12.dropna(how='all')
df_master12 = df_master12.fillna('0')
# Change dtypes to Int
df_master12['10-01-19'] = df_master12['10-01-19'].astype(float)
# Drop junk rows
df_master12 = df_master12[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_11-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'11-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'11-01-19'})
# append dataframes
df_list = [df, df1]
df_master13 = df.append(df_list)
df_master13['Date'] = '11/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master13 = df_master13.dropna(how='all')
df_master13 = df_master13.fillna('0')
# Change dtypes to Int
df_master13['11-01-19'] = df_master13['11-01-19'].astype(float)
# Drop junk rows
df_master13 = df_master13[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_12-19.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'12-01-19'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'12-01-19'})
# append dataframes
df_list = [df, df1]
df_master14 = df.append(df_list)
df_master14['Date'] = '12/01/2019'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master14 = df_master14.dropna(how='all')
df_master14 = df_master14.fillna('0')
# Change dtypes to Int
df_master14['12-01-19'] = df_master14['12-01-19'].astype(float)
# Drop junk rows
df_master14 = df_master14[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_1-20.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'01-01-20'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'01-01-20'})
# append dataframes
df_list = [df, df1]
df_master15 = df.append(df_list)
df_master15['Date'] = '01/01/2020'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master15 = df_master15.dropna(how='all')
df_master15 = df_master15.fillna('0')
# Change dtypes to Int
df_master15['01-01-20'] = df_master15['01-01-20'].astype(float)
# Drop junk rows
df_master15 = df_master15[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_2-20.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'02-01-20'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'02-01-20'})
# append dataframes
df_list = [df, df1]
df_master16 = df.append(df_list)
df_master16['Date'] = '02/01/2020'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master16 = df_master16.dropna(how='all')
df_master16 = df_master16.fillna('0')
# Change dtypes to Int
df_master16['02-01-20'] = df_master16['02-01-20'].astype(float)
# Drop junk rows
df_master16 = df_master16[:-10]


'''df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_3-20.xls', skiprows = 9)
# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]
# Remove columns with amiguous names
df = df.loc[:,~df.columns.str.contains('Unnamed')]
# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
# Drop second set of counties from original dataframe
df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
# Rename columns
df = df.rename(columns = {'County.1':'County', 'Collections*':'03-01-20'})
df1 = df1.rename(columns = {'County.1':'County', 'Collections*.1':'03-01-20'})
# append dataframes
df_list = [df, df1]
df_master17 = df.append(df_list)
df_master17['Date'] = '03/01/2020'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master17 = df_master17.dropna(how='all')
df_master17 = df_master17.fillna('0')
# Change dtypes to Int
df_master17['03-01-20'] = df_master17['03-01-20'].astype(float)
# Drop junk rows
df_master17 = df_master17[:-10]'''

df_master_list = [df_master1, df_master2, df_master3, df_master4, df_master5, df_master6, df_master7, df_master8, df_master9, df_master10, df_master11, df_master12, df_master13, df_master14, df_master15, df_master16]
df_master = df_master0.append(df_master_list)

df_fips = pd.read_csv('./FIPS_Codes.csv')
df_master = pd.merge(df_master, df_fips, on=['County', 'County'])

df_master = df_master.drop(df_master.index[[0]])
df_master =df_master.drop(columns = ['Unnamed: 0', 'RegionName', 'State', 'Metro', 'StateCodeFIPS', 'MunicipalCodeFIPS'], axis=1)
df_master = df_master.drop_duplicates()
df_master = df_master.rename(columns={'County':'GeoArea_Name', 'GeoFIPS':'GeoArea_FIPS'})
df_master['GeoArea_Name'] = df_master['GeoArea_Name'] + ', NC'

df_master = df_master.melt(id_vars=['GeoArea_FIPS', 'GeoArea_Name'], var_name='Date', value_name='Published_Value')
df_master = df_master.dropna()
df_master['Published_Value'] = df_master['Published_Value'].astype(str)
df_master = df_master[~df_master["Published_Value"].str.contains('/')]
df_master = df_master.sort_values(by = ['GeoArea_FIPS','Date'], ascending=True)
df_master = df_master.set_index('GeoArea_FIPS')

df_master.to_csv('./Updates/STG_BEA_MSALESUSETAX_0001.txt', sep='\t')