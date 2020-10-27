#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports
import urllib
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import requests
from sqlalchemy import create_engine
import pyodbc

# In[ ]:


# In[ ]:

# Load BEA CAINC5N_NC data
response = requests.get("https://apps.bea.gov/regional/zip/CAINC5N.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",")


# In[ ]:


# Check for unused fields
df.tail(10)


# In[ ]:


# Remove unused fields
df.drop(df.tail(4).index, inplace=True)


# In[ ]:


# Clean GeoFIPS
df["GeoFIPS"] = df["GeoFIPS"].str.replace('"', "")


# In[ ]:


# Set GeoFIPS as Index
df.set_index(df["GeoFIPS"], inplace=True)


# In[ ]:


# Drop GeoFIPS column
df.drop("GeoFIPS", axis=1, inplace=True)


# # Personal Income
# In[ ]:
print("Updating Personal Income..")

df_per_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Personal_Income.txt", encoding="ISO-8859-1", sep="\t"
)
df_per_backup.to_csv("./Backups/STG_BEA_CA5N_Personal_Income_BACKUP.txt")

filter1 = df["LineCode"] == 10
df_per = df[filter1]

df_per.to_csv("./Updates/STG_BEA_CA5N_Personal_Income.txt", sep="\t")

# # Population

# In[ ]:
print("Done. Updating Population..")

df_pop_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Population.txt", encoding="ISO-8859-1", sep="\t"
)
df_pop_backup.to_csv("./Backups/STG_BEA_CA5N_Population_BACKUP.txt")

filter1 = df["LineCode"] == 20
df_pop = df[filter1]

df_pop.to_csv("./Updates/STG_BEA_CA5N_Population.txt", sep="\t")

# # Per capita Personal Income

# In[ ]:

print("Done. Updating Per Capita Personal Income..")

df_pi_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_pi_backup.to_csv("./Backups/STG_BEA_CA5N_Per_Capita_Personal_Income.txt")

filter1 = df["LineCode"] == 30
df_pi = df[filter1]

df_pi.to_csv("./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt", sep="\t")

# # Wages and Salaries

# In[ ]:


print("Done. Updating Wages and Salaries..")


# In[ ]:


# Create Backups
df_w_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Wages_and_Salaries.txt", encoding="ISO-8859-1", sep="\t"
)
df_w_backup.to_csv("./Backups/STG_BEA_CA5N_Wages_and_Salaries_BACKUP.txt")


# In[ ]:


# Create a new dataframe for wages and salaries
filter1 = df["LineCode"] == 50
df_wages = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_wages.to_csv("./Updates/STG_BEA_CA5N_Wages_and_Salaries.txt", sep="\t")


# # Health Care and Social Assistance

# In[ ]:


print("Done. Updating Health Care and Social Assistance..")


# In[ ]:


# Create Backups
df_h_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_h_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Health_Care_and_Social_Assistance_BACKUP.txt"
)


# In[ ]:


# Create a new dataframe for Health_Care_and_Social_Assistance
filter1 = df["LineCode"] == 1600
df_health = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_health.to_csv(
    "./Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt", sep="\t"
)

# # Information

# In[ ]:


print("Done. Updating Information..")


# In[ ]:


# Create Backups
df_i_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Information.txt", encoding="ISO-8859-1", sep="\t"
)
df_i_backup.to_csv("./Backups/STG_BEA_CA5N_Information_BACKUP.txt")


# In[ ]:


# Create new dataframe for Information
filter1 = df["LineCode"] == 900
df_info = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_info.to_csv("./Updates/STG_BEA_CA5N_Information.txt", sep="\t")


# # Management of Companies and Enterprises

# In[ ]:


print("Done. Updating Management of Companies and Enterprises..")

# Create Backups
df_mang_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_mang_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Management_of_Companies_and_Enterprises_BACKUP.txt"
)

# Create new dataframe for Information
filter1 = df["LineCode"] == 1300
df_management = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_management.to_csv(
    "./Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt", sep="\t"
)

# # Manufacturing

# In[ ]:


print("Done. Updating Manufacturing..")

# Create Backups
df_manu_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Manufacturing.txt", encoding="ISO-8859-1", sep="\t"
)
df_manu_backup.to_csv("./Backups/STG_BEA_CA5N_Manufacturing_BACKUP.txt")

# Create new dataframe for Manufacturing
filter1 = df["LineCode"] == 500
df_manufacturing = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_manufacturing.to_csv("./Updates/STG_BEA_CA5N_Manufacturing.txt", sep="\t")


# # Mining, Quarrying, and Oil and Gas Production

# In[ ]:


print("Done. Updating Mining, Quarrying, and Oil and Gas Production..")

# Create Backups
df_min_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_min_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP.txt"
)

# Create new dataframe for Mining_Quarrying_and_Oil_and_Gas_Extraction
filter1 = df["LineCode"] == 200
df_mining = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_mining.to_csv(
    "./Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt", sep="\t"
)


# # Other Services

# In[ ]:


print("Done. Updating Other Services..")

# Create Backups
df_ser_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Other_Services.txt", encoding="ISO-8859-1", sep="\t"
)
df_ser_backup.to_csv("./Backups/STG_BEA_CA5N_Other_Services_BACKUP.txt")

# Create new dataframe for Other_Services
filter1 = df["LineCode"] == 1900
df_services = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_services.to_csv("./Updates/STG_BEA_CA5N_Other_Services.txt", sep="\t")


# # Professional, Scientific, and Technical Services

# In[ ]:


print("Done. Updating Professional Scientific and Technical Services..")

# Create Backups
df_pst_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_pst_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services_BACKUP.txt"
)

# Create new dataframe for Professional_Scientific_and_Technical_Services
filter1 = df["LineCode"] == 1200
df_professional = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_professional.to_csv(
    "./Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt",
    sep="\t",
)

# # Real Estate and Rental Housing

# In[ ]:


print("Done. Updating Real Estate and Rental Housing..")

# Create Backups
df_hou_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_hou_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing_BACKUP.txt"
)

# Create new dataframe for Real_Estate_and_Rental_and_Leasing
filter1 = df["LineCode"] == 1100
df_realestate = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_realestate.to_csv(
    "./Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt", sep="\t"
)


# # Retail Trade

# In[ ]:


print("Done. Updating Retail Trade..")

# Create Backups
df_r_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Retail_Trade.txt", encoding="ISO-8859-1", sep="\t"
)
df_r_backup.to_csv("./Backups/STG_BEA_CA5N_Retail_Trade_BACKUP.txt")

# Create new dataframe for Retail_Trade
filter1 = df["LineCode"] == 700
df_retail = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_retail.to_csv("./Updates/STG_BEA_CA5N_Retail_Trade.txt", sep="\t")


# # Transportation and Warehousing

# In[ ]:


print("Done. Updating Transportation and Warehousing..")

# Create Backups
df_t_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Transportation_and_Warehousing.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_t_backup.to_csv("./Backups/STG_BEA_CA5N_Transportation_and_Warehousing_BACKUP.txt")

# Create new dataframe for Transportation_and_Warehousing
filter1 = df["LineCode"] == 800
df_transportation = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_transportation.to_csv(
    "./Updates/STG_BEA_CA5N_Transportation_and_Warehousing.txt", sep="\t"
)


# # Utilities

# In[ ]:


print("Done. Updating Utilities..")

# Create Backups
df_u_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Utilities.txt", encoding="ISO-8859-1", sep="\t"
)
df_u_backup.to_csv("./Backups/STG_BEA_CA5N_Utilities_BACKUP.txt")

# Create new dataframe for Utilities
filter1 = df["LineCode"] == 300
df_utilities = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_utilities.to_csv("./Updates/STG_BEA_CA5N_Utilities.txt", sep="\t")

# # Wholesale Trade

# In[ ]:


print("Done. Updating Wholesale Trade..")

# Create Backups
df_wt_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Wholesale_Trade.txt", encoding="ISO-8859-1", sep="\t"
)
df_wt_backup.to_csv("./Backups/STG_BEA_CA5N_Wholesale_Trade_BACKUP.txt")

# Create new dataframe for Wholesale_Trade
filter1 = df["LineCode"] == 600
df_wholesale = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_wholesale.to_csv("./Updates/STG_BEA_CA5N_Wholesale_Trade.txt", sep="\t")

# # Proprietors' Income

# In[ ]:


print("Done. Updating Proprietors Income..")

# Create Backups
df_pi_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Proprietors_Income.txt", encoding="ISO-8859-1", sep="\t"
)
df_pi_backup.to_csv("./Backups/STG_BEA_CA5N_Proprietors_Income_BACKUP.txt")

# Create new dataframe for Proprietors_Income
filter1 = df["LineCode"] == 70
df_propinc = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_propinc.to_csv("./Updates/STG_BEA_CA5N_Proprietors_Income.txt", sep="\t")

# # Government and Government Enterprises

# In[ ]:


print("Done. Updating Government and Government Enterprises..")

# Create Backups
df_g_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_g_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Government_and_Government_Enterprises_BACKUP.txt"
)

# Create new dataframe for Government_and_Government_Enterprises
filter1 = df["LineCode"] == 2000
df_gov = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_gov.to_csv(
    "./Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt", sep="\t"
)

# # Private Nonfarm Compensation

# In[ ]:


print("Done. Updating Private Nonfarm Compensation..")

# Create Backups
df_pnc_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Private_Nonfarm_Compensation.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_pnc_backup.to_csv("./Backups/STG_BEA_CA5N_Private_Nonfarm_Compensation_BACKUP.txt")

# Create new dataframe for Private_Nonfarm_Compensation
filter1 = df["LineCode"] == 90
df_private = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_private.to_csv("./Updates/STG_BEA_CA5N_Private_Nonfarm_Compensation.txt", sep="\t")

# # Farm Compensation

# In[ ]:


print("Done. Updating Farm Compensation..")

# Create Backups
df_fc_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Farm_Compensation.txt", encoding="ISO-8859-1", sep="\t"
)
df_fc_backup.to_csv("./Backups/STG_BEA_CA5N_Farm_Compensation_BACKUP.txt")

# Create new dataframe for Farm_Compensation
filter1 = df["LineCode"] == 81
df_farm = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_farm.to_csv("./Updates/STG_BEA_CA5N_Farm_Compensation.txt", sep="\t")


# # Nonfarm Compensation

# In[ ]:


print("Done. Updating Nonfarm Compensation..")

# Create Backups
df_nf_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Nonfarm_Compensation.txt", encoding="ISO-8859-1", sep="\t"
)
df_nf_backup.to_csv("./Backups/STG_BEA_CA5N_Nonfarm_Compensation_BACKUP.txt")

# Create new dataframe for Nonfarm_Compensation
filter1 = df["LineCode"] == 82
df_nonfarm = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_nonfarm.to_csv("./Updates/STG_BEA_CA5N_Nonfarm_Compensation.txt", sep="\t")


# # Supplements to Wages and Salaries

# In[ ]:


print("Done. Updating Supplements to Wages and Salaries..")

# Create Backups
df_supp_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_supp_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries_BACKUP.txt"
)

# Create new dataframe for Supplements_to_Wages_and_Salaries
filter1 = df["LineCode"] == 60
df_supplement = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_supplement.to_csv(
    "./Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt", sep="\t"
)


# # Federal, Civilian

# In[ ]:


print("Done. Updating Govt Type Federal Civilian..")

# Create Backups
df_fcgov_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Federal_Civilian_Government.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_fcgov_backup.to_csv("./Backups/STG_BEA_CA5N_Federal_Civilian_Government_BACKUP.txt")

# Create new dataframe for Federal_Civilian_Government
filter1 = df["LineCode"] == 2001
df_federal = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_federal.to_csv("./Updates/STG_BEA_CA5N_Federal_Civilian_Government.txt", sep="\t")

# # Accommodation and Food Services

# In[ ]:


print("Done. Updating Accommodation and Food Services..")

# Create Backups
df_acc_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_acc_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Accommodation_and_Food_Services_BACKUP.txt"
)

# Create new dataframe for Accommodation_and_Food_Services
filter1 = df["LineCode"] == 1800
df_food = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_food.to_csv("./Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt", sep="\t")

# # Administrative Support

# In[ ]:


print("Done. Updating Administrative Support..")

# Create Backups
df_as_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_as_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP.txt"
)

# Create new dataframe for Administrative_and_Support_and_Waste_Management_and_Remediation_Services
filter1 = df["LineCode"] == 1400
df_admin = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_admin.to_csv(
    "./Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt",
    sep="\t",
)

# # Arts, Entertainment, and Recreation

# In[ ]:


print("Done. Updating Arts, Entertainment, and Recreation..")

# Create Backups
df_aer_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_aer_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Arts_Entertainment_and_Recreation_BACKUP.txt"
)

# Create new dataframe for Arts_Entertainment_and_Recreation
filter1 = df["LineCode"] == 1700
df_arts = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_arts.to_csv("./Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt", sep="\t")

# # Construction

# In[ ]:


print("Done. Updating Construction..")

# Create Backups
df_con_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Construction.txt", encoding="ISO-8859-1", sep="\t"
)
df_con_backup.to_csv("./Backups/STG_BEA_CA5N_Construction_BACKUP.txt")

# Create new dataframe for Construction
filter1 = df["LineCode"] == 400
df_construction = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_construction.to_csv("./Updates/STG_BEA_CA5N_Construction.txt", sep="\t")

# # Educational Services

# In[ ]:


print("Done. Updating Educational Services..")

# Create Backups
df_es_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Educational_Services.txt", encoding="ISO-8859-1", sep="\t"
)
df_es_backup.to_csv("./Backups/STG_BEA_CA5N_Educational_Services_BACKUP.txt")

# Create new dataframe for Educational_Services
filter1 = df["LineCode"] == 1500
df_eduserv = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_eduserv.to_csv("./Updates/STG_BEA_CA5N_Educational_Services.txt", sep="\t")


# # Finance and Insurance

# In[ ]:


print("Done. Updating Finance and Insurance..")

# Create Backups
df_fi_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Finance_and_Insurance.txt", encoding="ISO-8859-1", sep="\t"
)
df_fi_backup.to_csv("./Backups/STG_BEA_CA5N_Finance_and_Insurance_BACKUP.txt")

# Create new dataframe for Finance_and_Insurance
filter1 = df["LineCode"] == 1000
df_finance = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_finance.to_csv("./Updates/STG_BEA_CA5N_Finance_and_Insurance.txt", sep="\t")


# # Forestry, Fishing, and Related Activities

# In[ ]:


print("Done. Updating Forestry, Fishing, and Related Activities..")

# Create Backups
df_ffr_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_ffr_backup.to_csv(
    "./Backups/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities_BACKUP.txt"
)

# Create new dataframe for Forestry_Fishing_and_Related_Activities
filter1 = df["LineCode"] == 100
df_forestry = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_forestry.to_csv(
    "./Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt", sep="\t"
)

# # Military

# In[ ]:


print("Done. Updating Govt Type Federal Civilian..")

# Create Backups
df_mgov_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Military_Government.txt", encoding="ISO-8859-1", sep="\t"
)
df_mgov_backup.to_csv("./Backups/STG_BEA_CA5N_Military_Government_BACKUP.txt")

# Create new dataframe for Military_Government
filter1 = df["LineCode"] == 2002
df_military = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_military.to_csv("./Updates/STG_BEA_CA5N_Military_Government.txt", sep="\t")

# # State and Local

# In[ ]:


print("Done. Updating Govt Type State Local..")

# Create Backups
df_slgov_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_State_Local_Government.txt", encoding="ISO-8859-1", sep="\t"
)
df_slgov_backup.to_csv("./Backups/STG_BEA_CA5N_State_Local_Government_BACKUP.txt")

# Create new dataframe for State_Local_Government
filter1 = df["LineCode"] == 2010
df_state_local = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_state_local.to_csv("./Updates/STG_BEA_CA5N_State_Local_Government.txt", sep="\t")

# # State Government

# In[ ]:


print("Done. Updating Govt Type State..")

# Create Backups
df_sgov_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_State_Government.txt", encoding="ISO-8859-1", sep="\t"
)
df_sgov_backup.to_csv("./Backups/STG_BEA_CA5N_State_Government_BACKUP.txt")

# Create new dataframe for State_Government
filter1 = df["LineCode"] == 2011
df_state = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_state.to_csv("./Updates/STG_BEA_CA5N_State_Government.txt", sep="\t")

# # Local Government

# In[ ]:


print("Done. Updating Govt Type Local..")

# Create Backups
df_lgov_backup = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Local_Government.txt", encoding="ISO-8859-1", sep="\t"
)
df_lgov_backup.to_csv("./Backups/STG_BEA_CA5N_Local_Government_BACKUP.txt")

# Create new dataframe for Local_Government
filter1 = df["LineCode"] == 2012
df_local = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_local.to_csv("./Updates/STG_BEA_CA5N_Local_Government.txt", sep="\t")
