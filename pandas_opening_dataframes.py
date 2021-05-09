import pandas as pd
import glob
import requests
import xlrd
import sqlite3
import re
from datetime import datetime, timedelta, date

con = sqlite3.connect('example.db')
cur = con.cursor()

try:
    cur.execute("DROP TABLE texas_capacity")
except:
    pass
try:
    cur.execute("DROP TABLE county_unemployment")
except:
    pass
try:
    cur.execute("DROP TABLE texas_icu_beds_avail")
except:
    pass
try:
    cur.execute("DROP TABLE texas_covid_icu_beds")
except:
    pass
try:
    cur.execute("DROP TABLE texas_icu_utilization")
except:
    pass

#######################
##### STATE LEVEL######
#######################

# DSHS Hospital Capacity
dshs_data_over_time_url = "http://dshs.texas.gov/coronavirus/CombinedHospitalDataoverTimebyTSA.xlsx"
dshs_hospital_capacity_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="GA-32 COVID % Capacity")

dshs_hospital_capacity_df = dshs_hospital_capacity_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)

column_names = ["["+ str(col) + "]" for col in dshs_hospital_capacity_df.columns] # Generate a the column declaration for the table
dshs_column_declaration = " numeric, ".join(column_names) + " numeric"

cur.execute("CREATE TABLE texas_capacity (" + dshs_column_declaration + ")") # Create table

dshs_hospital_capacity_df.to_sql("texas_capacity", con=con, if_exists="append", index=False) # Add the data to the table

for row in cur.execute("SELECT [2021-02-14] FROM texas_capacity"):
    pass  #print(row)

# DSHS ICU bed utilization
dshs_icu_beds_avail_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="ICU Beds Available")
dshs_icu_beds_avail_df = dshs_icu_beds_avail_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)

dshs_covid_icu_beds_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="COVID-19 ICU")
dshs_covid_icu_beds_df = dshs_covid_icu_beds_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)

col_names = [str(col) for col in dshs_hospital_capacity_df.columns] # Generate a the column declaration for the table

dshs_icu_bed_utilization = dshs_covid_icu_beds_df.values/(dshs_covid_icu_beds_df.values + dshs_icu_beds_avail_df.values)
dshs_icu_bed_utilization_df = pd.DataFrame(dshs_icu_bed_utilization, columns=col_names)

cur.execute("CREATE TABLE texas_icu_utilization (" + dshs_column_declaration + ")") # Create table
dshs_icu_bed_utilization_df.to_sql("texas_icu_utilization", con=con, if_exists="append", index=False) # Add the data to the table

for row in cur.execute("SELECT [2021-02-14] FROM texas_icu_utilization"):
    print(row)

# Business applications 
business_app_dfs = []
"https://www.census.gov/construction/bps/xls/statemonthly_202001.xls"

business_app_path = r"https://www.census.gov/construction/bps/xls/statemonthly_"
business_app_url = []
#business_app_url = glob.glob(business_app_path + "/*.xls")
#FIX ME!!! USE A FOR LOOP TO CALCULATE DATES
#print(business_app_url)

for url in business_app_url:
    r = requests.get(url)

    file=open("./business_apps.xlsx", 'wb')
    file.write(r.content)
    file.close()

    business_app_dfs.append(pd.read_excel("./business_apps.xlsx", header=7))

#print(business_app_dfs)

#######################
##### COUNTY LEVEL#####
#######################

# Confirmed Cases - Github Data
confirmed_url = "http://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
confirmed_df = pd.read_csv(confirmed_url)
#print(confirmed_df["Admin2"])
    # Admin2 = county

# Death Cases - Github Data
death_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
death_cases_df = pd.read_csv(death_cases_url)
#print(death_cases_df)

# Unemployment Claims
unemployment_url = "https://www.twc.texas.gov/files/agency/weekly-claims-by-county-twc.xlsx"
unemployment_df = pd.read_excel(unemployment_url, header=2)

unemployment_df = unemployment_df.iloc[:254, :]             # Eliminate unnecessary rows (that do not contain data)

unemployment_df = unemployment_df.dropna(axis=1, how="all") # Drop any columns that are all NaN

column_names = ["["+ str(col) + "]" for col in unemployment_df.columns] # Generate a the column declaration for the table
column_declaration = "County text, " + " int, ".join(column_names[1:]) + " int"

cur.execute("CREATE TABLE county_unemployment (" + column_declaration + ")") # Create table

unemployment_df.to_sql("county_unemployment", con=con, if_exists="append", index=False) # Add the data to the table

for row in cur.execute("SELECT [4/10/2021] FROM county_unemployment"):
    pass
    #print(row)