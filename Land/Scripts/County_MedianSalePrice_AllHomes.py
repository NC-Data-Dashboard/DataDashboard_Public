#!/usr/bin/env python
# coding: utf-8


# Imports
import urllib
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc


# Create Backups
df_backup = pd.read_csv("./Updates/STG_ZLLW_County_MedianSalePrice_AllHomes.txt")
df_backup.to_csv("./Backups/STG_ZLLW_County_MedianSalePrice_AllHomes_BACKUP.txt")


# Load Land data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public/County/Sale_Prices_County.csv",
    encoding="ISO-8859-1",
)


df = df.drop(columns=["RegionID"], axis=1)


# Filter data to NC
filter1 = df["StateName"] == "North Carolina"
df_nc = df[filter1]


# Sort by Region Name
df_nc = df_nc.sort_values("RegionName", ascending=True)


df_fips = pd.read_csv("./FIPS_Codes.csv")


df_msp_nc = df_nc.set_index("RegionName").join(df_fips.set_index("RegionName"))


# Change MunicipalCodeFIPS dtype to add leading 0's
df_msp_nc.loc[:, "MunicipalCodeFIPS"] = df_msp_nc["MunicipalCodeFIPS"].astype(str)


# Add leading 0's and check to ensure they were added
df_msp_nc.loc[:, "MunicipalCodeFIPS"] = df_msp_nc["MunicipalCodeFIPS"].str.zfill(3)


columns = [
    "State",
    "Metro",
    "StateCodeFIPS",
    "MunicipalCodeFIPS",
    "SizeRank",
    "2008-03",
    "2008-04",
    "2008-05",
    "2008-06",
    "2008-07",
    "2008-08",
    "2008-09",
    "2008-10",
    "2008-11",
    "2008-12",
    "2009-01",
    "2009-02",
    "2009-03",
    "2009-04",
    "2009-05",
    "2009-06",
    "2009-07",
    "2009-08",
    "2009-09",
    "2009-10",
    "2009-11",
    "2009-12",
    "2010-01",
    "2010-02",
    "2010-03",
    "2010-04",
    "2010-05",
    "2010-06",
    "2010-07",
    "2010-08",
    "2010-09",
    "2010-10",
    "2010-11",
    "2010-12",
    "2011-01",
    "2011-02",
    "2011-03",
    "2011-04",
    "2011-05",
    "2011-06",
    "2011-07",
    "2011-08",
    "2011-09",
    "2011-10",
    "2011-11",
    "2011-12",
    "2012-01",
    "2012-02",
    "2012-03",
    "2012-04",
    "2012-05",
    "2012-06",
    "2012-07",
    "2012-08",
    "2012-09",
    "2012-10",
    "2012-11",
    "2012-12",
    "2013-01",
    "2013-02",
    "2013-03",
    "2013-04",
    "2013-05",
    "2013-06",
    "2013-07",
    "2013-08",
    "2013-09",
    "2013-10",
    "2013-11",
    "2013-12",
    "2014-01",
    "2014-02",
    "2014-03",
    "2014-04",
    "2014-05",
    "2014-06",
    "2014-07",
    "2014-08",
    "2014-09",
    "2014-10",
    "2014-11",
    "2014-12",
    "2015-01",
    "2015-02",
    "2015-03",
    "2015-04",
    "2015-05",
    "2015-06",
    "2015-07",
    "2015-08",
    "2015-09",
    "2015-10",
    "2015-11",
    "2015-12",
    "2016-01",
    "2016-02",
    "2016-03",
    "2016-04",
    "2016-05",
    "2016-06",
    "2016-07",
    "2016-08",
    "2016-09",
    "2016-10",
    "2016-11",
    "2016-12",
    "2017-01",
    "2017-02",
    "2017-03",
    "2017-04",
    "2017-05",
    "2017-06",
    "2017-07",
    "2017-08",
    "2017-09",
    "2017-10",
    "2017-11",
    "2017-12",
    "2018-01",
    "2018-02",
    "2018-03",
    "2018-04",
    "2018-05",
    "2018-06",
    "2018-07",
    "2018-08",
    "2018-09",
    "2018-10",
    "2018-11",
    "2018-12",
    "2019-01",
    "2019-02",
    "2019-03",
    "2019-04",
    "2019-05",
    "2019-06",
    "2019-07",
    "2019-08",
    "2019-09",
    "2019-10",
    "2019-11",
    "2019-12",
    "2020-01",
    "2020-02",
    "2020-03",
]
df_msp_nc = df_msp_nc[columns]


# Save to csv file for export in Excel
df_msp_nc.to_csv("./Updates/STG_ZLLW_County_MedianSalePrice_AllHomes.txt", sep="\t")
