#!/usr/bin/env python
# coding: utf-8


# Imports
from io import BytesIO
from zipfile import ZipFile
import urllib
import pandas as pd
import requests
from sqlalchemy import create_engine
import pyodbc


# Load BEA CAINC6N_NC data
response = requests.get("https://apps.bea.gov/regional/zip/CAINC6N.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",")


# Check for unused fields
df.tail(10)


# Remove unused fields
df.drop(df.tail(4).index, inplace=True)


# Clean GeoFIPS
df["GeoFIPS"] = df["GeoFIPS"].replace({"": ""})


# Set GeoFIPS as Index
df.set_index(df["GeoFIPS"], inplace=True)


# Drop GeoFIPS column
df.drop("GeoFIPS", axis=1, inplace=True)


# # Create Compensation of Employees


print("Updating Compensation of Employees...")


# Create Backups
df_comp_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Compensation_of_Employees.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_comp_backup.to_csv("./Backups/STG_BEA_CA6N_Compensation_of_Employees_BACKUP.txt")


# Create new dataframe for Per capita Information
filter1 = df["LineCode"] == 1
df_compensation = df[filter1]
df_compensation.head()


# Clean Description column
df_compensation.loc[:, "Description"] = df_compensation["Description"].str.strip("1/")


# Save as tab-delimited txt file for export to SSMS
df_compensation.to_csv("./Updates/STG_BEA_CA6N_Compensation_of_Employees.txt", sep="\t")


# # Create Wages and Salaries


print("Done. Updating Wages and Salaries...")


# Create Backups
df_w_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Wages_and_Salaries.txt", encoding="ISO-8859-1", sep="\t"
)
df_w_backup.to_csv("./Backups/STG_BEA_CA6N_Wages_and_Salaries_BACKUP.txt")


# Create a new dataframe for Earnings by place of work
filter1 = df["LineCode"] == 5
df_wages = df[filter1]


# Save as tab-delimited txt file for export to SSMS
df_wages.to_csv("./Updates/STG_BEA_CA6N_Wages_and_Salaries.txt", sep="\t")


# # Create Health Care and Social Assistance


print("Done. Updating Health Care and Social Assistance...")


# Create Backups
df_h_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Health_Care_and_Social_Assistance.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_h_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Health_Care_and_Social_Assistance_BACKUP.txt"
)


# Create a new dataframe for Health_Care_and_Social_Assistance
filter1 = df["LineCode"] == 1600
df_health = df[filter1]


# Save as tab-delimited txt file for export to SSMS
df_health.to_csv(
    "./Updates/STG_BEA_CA6N_Health_Care_and_Social_Assistance.txt", sep="\t"
)

# # Create Information


print("Done. Updating Information..")


# Create Backups
df_i_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Information.txt", encoding="ISO-8859-1", sep="\t"
)
df_i_backup.to_csv("./Backups/STG_BEA_CA6N_Information_BACKUP.txt")


# Create new dataframe for Information
filter1 = df["LineCode"] == 900
df_info = df[filter1]


# Save as tab-delimited txt file for export to SSMS
df_info.to_csv("./Updates/STG_BEA_CA6N_Information.txt", sep="\t")


# # Create Management of Companies and Enterprises


print("Done. Updating Management of Companies and Enterprises..")

# Create Backups
df_mang_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Management_of_Companies_and_Enterprises.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_mang_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Management_of_Companies_and_Enterprises_BACKUP.txt"
)

# Create new dataframe for Information
filter1 = df["LineCode"] == 1300
df_management = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_management.to_csv(
    "./Updates/STG_BEA_CA6N_Management_of_Companies_and_Enterprises.txt", sep="\t"
)

# # Manufacturing


print("Done. Updating Manufacturing..")

# Create Backups
df_manu_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Manufacturing.txt", encoding="ISO-8859-1", sep="\t"
)
df_manu_backup.to_csv("./Backups/STG_BEA_CA6N_Manufacturing_BACKUP.txt")

# Create new dataframe for Manufacturing
filter1 = df["LineCode"] == 500
df_manufacturing = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_manufacturing.to_csv("./Updates/STG_BEA_CA6N_Manufacturing.txt", sep="\t")

# # Mining, Quarrying, and Oil and Gas Production


print("Done. Updating Mining, Quarrying, and Oil and Gas Production..")

# Create Backups
df_min_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_min_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP.txt"
)

# Create new dataframe for Mining_Quarrying_and_Oil_and_Gas_Extraction
filter1 = df["LineCode"] == 200
df_mining = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_mining.to_csv(
    "./Updates/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt", sep="\t"
)


# # Other Services


print("Done. Updating Other Services..")

# Create Backups
df_ser_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Other_Services.txt", encoding="ISO-8859-1", sep="\t"
)
df_ser_backup.to_csv("./Backups/STG_BEA_CA6N_Other_Services_BACKUP.txt")

# Create new dataframe for Other_Services
filter1 = df["LineCode"] == 1900
df_services = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_services.to_csv("./Updates/STG_BEA_CA6N_Other_Services.txt", sep="\t")

# # Professional, Scientific, and Technical Services


print("Done. Updating Professional Scientific and Technical Services..")

# Create Backups
df_pst_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_pst_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services_BACKUP.txt"
)

# Create new dataframe for Professional_Scientific_and_Technical_Services
filter1 = df["LineCode"] == 1200
df_professional = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_professional.to_csv(
    "./Updates/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services.txt",
    sep="\t",
)


# # Real Estate and Rental Housing


print("Done. Updating Real Estate and Rental Housing..")

# Create Backups
df_hou_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_hou_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing_BACKUP.txt"
)

# Create new dataframe for Real_Estate_and_Rental_and_Leasing
filter1 = df["LineCode"] == 1100
df_realestate = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_realestate.to_csv(
    "./Updates/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing.txt", sep="\t"
)

# # Retail Trade


print("Done. Updating Retail Trade..")

# Create Backups
df_r_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Retail_Trade.txt", encoding="ISO-8859-1", sep="\t"
)
df_r_backup.to_csv("./Backups/STG_BEA_CA6N_Retail_Trade_BACKUP.txt")

# Create new dataframe for Retail_Trade
filter1 = df["LineCode"] == 700
df_retail = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_retail.to_csv("./Updates/STG_BEA_CA6N_Retail_Trade.txt", sep="\t")


# # Transportation and Warehousing


print("Done. Updating Transportation and Warehousing..")

# Create Backups
df_t_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Transportation_and_Warehousing.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_t_backup.to_csv("./Backups/STG_BEA_CA6N_Transportation_and_Warehousing_BACKUP.txt")

# Create new dataframe for Transportation_and_Warehousing
filter1 = df["LineCode"] == 800
df_transportation = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_transportation.to_csv(
    "./Updates/STG_BEA_CA6N_Transportation_and_Warehousing.txt", sep="\t"
)

# # Utilities


print("Done. Updating Utilities..")

# Create Backups
df_u_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Utilities.txt", encoding="ISO-8859-1", sep="\t"
)
df_u_backup.to_csv("./Backups/STG_BEA_CA6N_Utilities_BACKUP.txt")

# Create new dataframe for Utilities
filter1 = df["LineCode"] == 300
df_utilities = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_utilities.to_csv("./Updates/STG_BEA_CA6N_Utilities.txt", sep="\t")

# # Wholesale Trade


print("Done. Updating Wholesale Trade..")

# Create Backups
df_wt_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Wholesale_Trade.txt", encoding="ISO-8859-1", sep="\t"
)
df_wt_backup.to_csv("./Backups/STG_BEA_CA6N_Wholesale_Trade_BACKUP.txt")

# Create new dataframe for Wholesale_Trade
filter1 = df["LineCode"] == 600
df_wholesale = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_wholesale.to_csv("./Updates/STG_BEA_CA6N_Wholesale_Trade.txt", sep="\t")

# # Employer Contributions for Employee Pension and Insurance Funds


print(
    "Done. Updating Employer Contributions for Employee Pension and Insurance Funds.."
)

# Create Backups
df_p_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_p_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds_BACKUP.txt"
)

# Create new dataframe for Employer_Contributions_for_Employee_Pension_and_Insurance_Funds
filter1 = df["LineCode"] == 7
df_pension = df[filter1]

# Clean Description column
df_pension.loc[:, "Description"] = df_pension["Description"].str.strip("2/")

# Save as tab-delimited txt file for export to SSMS
df_pension.to_csv(
    "./Updates/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds.txt",
    sep="\t",
)

# # Employer Contributions for Government Social Insurance


print("Done. Updating Employer Contributions for Government Social Insurance..")

# Create Backups
df_si_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_si_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance_BACKUP.txt"
)

# Create new dataframe for Employer_Contributions_for_Government_Social_Insurance
filter1 = df["LineCode"] == 8
df_social = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_social.to_csv(
    "./Updates/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance.txt",
    sep="\t",
)


# # Government and Government Enterprises


print("Done. Updating Government and Government Enterprises..")

# Create Backups
df_g_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Government_and_Government_Enterprises.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_g_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Government_and_Government_Enterprises_BACKUP.txt"
)

# Create new dataframe for Government_and_Government_Enterprises
filter1 = df["LineCode"] == 2000
df_gov = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_gov.to_csv(
    "./Updates/STG_BEA_CA6N_Government_and_Government_Enterprises.txt", sep="\t"
)

# # Private Nonfarm Compensation


print("Done. Updating Private Nonfarm Compensation..")

# Create Backups
df_pnc_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Private_Nonfarm_Compensation.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_pnc_backup.to_csv("./Backups/STG_BEA_CA6N_Private_Nonfarm_Compensation_BACKUP.txt")

# Create new dataframe for Private_Nonfarm_Compensation
filter1 = df["LineCode"] == 90
df_private = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_private.to_csv("./Updates/STG_BEA_CA6N_Private_Nonfarm_Compensation.txt", sep="\t")

# # Farm Compensation


print("Done. Updating Farm Compensation..")

# Create Backups
df_fc_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Farm_Compensation.txt", encoding="ISO-8859-1", sep="\t"
)
df_fc_backup.to_csv("./Backups/STG_BEA_CA6N_Farm_Compensation_BACKUP.txt")

# Create new dataframe for Farm_Compensation
filter1 = df["LineCode"] == 81
df_farm = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_farm.to_csv("./Updates/STG_BEA_CA6N_Farm_Compensation.txt", sep="\t")


# # Nonfarm Compensation


print("Done. Updating Nonfarm Compensation..")

# Create Backups
df_nf_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Nonfarm_Compensation.txt", encoding="ISO-8859-1", sep="\t"
)
df_nf_backup.to_csv("./Backups/STG_BEA_CA6N_Nonfarm_Compensation_BACKUP.txt")

# Create new dataframe for Nonfarm_Compensation
filter1 = df["LineCode"] == 82
df_nonfarm = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_nonfarm.to_csv("./Updates/STG_BEA_CA6N_Nonfarm_Compensation.txt", sep="\t")


# # Supplements to Wages and Salaries


print("Done. Updating Supplements to Wages and Salaries..")

# Create Backups
df_supp_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_supp_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries_BACKUP.txt"
)

# Create new dataframe for Supplements_to_Wages_and_Salaries
filter1 = df["LineCode"] == 6
df_supplement = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_supplement.to_csv(
    "./Updates/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries.txt", sep="\t"
)
# # Average Compensation Per Job


print("Done. Updating Average Compensation Per Job..")

# Create Backups
df_ac_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_ac_backup.to_csv("./Backups/STG_BEA_CA6N_Average_Compensation_Per_Job_BACKUP.txt")

# Create new dataframe for Average_Compensation_Per_Job
filter1 = df["LineCode"] == 9
df_comp = df[filter1]

# Clean Description column
df_comp.loc[:, "Description"] = df_comp["Description"].str.strip("3/")

# Save as tab-delimited txt file for export to SSMS
df_comp.to_csv("./Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt", sep="\t")

# # Accommodation and Food Services


print("Done. Updating Accommodation and Food Services..")

# Create Backups
df_acc_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Accommodation_and_Food_Services.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_acc_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Accommodation_and_Food_Services_BACKUP.txt"
)

# Create new dataframe for Accommodation_and_Food_Services
filter1 = df["LineCode"] == 1800
df_food = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_food.to_csv("./Updates/STG_BEA_CA6N_Accommodation_and_Food_Services.txt", sep="\t")


# # Administrative Support


print("Done. Updating Administrative Support..")

# Create Backups
df_as_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_as_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP.txt"
)

# Create new dataframe for Administrative_and_Support_and_Waste_Management_and_Remediation_Services
filter1 = df["LineCode"] == 1400
df_admin = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_admin.to_csv(
    "./Updates/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt",
    sep="\t",
)

# # Arts, Entertainment, and Recreation


print("Done. Updating Arts, Entertainment, and Recreation..")

# Create Backups
df_aer_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Arts_Entertainment_and_Recreation.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_aer_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Arts_Entertainment_and_Recreation_BACKUP.txt"
)

# Create new dataframe for Arts_Entertainment_and_Recreation
filter1 = df["LineCode"] == 1700
df_arts = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_arts.to_csv("./Updates/STG_BEA_CA6N_Arts_Entertainment_and_Recreation.txt", sep="\t")


# # Construction


print("Done. Updating Construction..")

# Create Backups
df_con_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Construction.txt", encoding="ISO-8859-1", sep="\t"
)
df_con_backup.to_csv("./Backups/STG_BEA_CA6N_Construction_BACKUP.txt")

# Create new dataframe for Construction
filter1 = df["LineCode"] == 400
df_construction = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_construction.to_csv("./Updates/STG_BEA_CA6N_Construction.txt", sep="\t")

# # Educational Services


print("Done. Updating Educational Services..")

# Create Backups
df_es_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Educational_Services.txt", encoding="ISO-8859-1", sep="\t"
)
df_es_backup.to_csv("./Backups/STG_BEA_CA6N_Educational_Services_BACKUP.txt")

# Create new dataframe for Educational_Services
filter1 = df["LineCode"] == 1500
df_eduserv = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_eduserv.to_csv("./Updates/STG_BEA_CA6N_Educational_Services.txt", sep="\t")

# # Finance and Insurance


print("Done. Updating Finance and Insurance..")

# Create Backups
df_fi_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Finance_and_Insurance.txt", encoding="ISO-8859-1", sep="\t"
)
df_fi_backup.to_csv("./Backups/STG_BEA_CA6N_Finance_and_Insurance_BACKUP.txt")

# Create new dataframe for Finance_and_Insurance
filter1 = df["LineCode"] == 1000
df_finance = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_finance.to_csv("./Updates/STG_BEA_CA6N_Finance_and_Insurance.txt", sep="\t")

# # Forestry, Fishing, and Related Activities


print("Done. Updating Forestry, Fishing, and Related Activities..")

# Create Backups
df_ffr_backup = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities.txt",
    encoding="ISO-8859-1",
    sep="\t",
)
df_ffr_backup.to_csv(
    "./Backups/STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities_BACKUP.txt"
)

# Create new dataframe for Forestry_Fishing_and_Related_Activities
filter1 = df["LineCode"] == 100
df_forestry = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_forestry.to_csv(
    "./Updates/STG_BEA_CA6N_Forestry_Fishing_and_Related_Activities.txt", sep="\t"
)
