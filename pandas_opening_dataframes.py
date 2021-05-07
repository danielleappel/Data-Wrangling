import pandas as pd
import glob
import requests
import xlrd
import sqlite3
import re
from datetime import datetime, timedelta, date

#######################
##### STATE LEVEL######
#######################

# DSHS Hospital Capacity
dshs_data_over_time_url = "http://dshs.texas.gov/coronavirus/CombinedHospitalDataoverTimebyTSA.xlsx"
dshs_hospital_capacity_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="GA-32 COVID % Capacity")
#print(dshs_hospital_capacity_df)

# DSHS ICU bed utilization
#dshs_icu_utilization = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name=SOMETHING)

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

dates = []
for col_name in unemployment_df.columns:                    # Get a list of the column names
    if isinstance(col_name, datetime):
        dates.append(col_name.strftime("%x") + " int")
    else:
        dates.append(col_name)









con = sqlite3.connect('example.db')
cur = con.cursor()

# Create table
#cur.execute("""CREATE TABLE county_unemployment
 #       ()""")

