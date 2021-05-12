import pandas as pd
import sqlite3
import re
from datetime import datetime, timedelta, date
import math

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
try:
    cur.execute("DROP TABLE texas_business_apps")
except:
    pass
try:
    cur.execute("DROP TABLE county_deaths")
except:
    pass
#######################
##### STATE LEVEL######
#######################

#------------------------#
# DSHS Hospital Capacity #
#------------------------#
dshs_data_over_time_url = "https://dshs.texas.gov/coronavirus/CombinedHospitalDataoverTimebyTSA.xlsx"
dshs_hospital_capacity_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="GA-32 COVID % Capacity")
dshs_hospital_capacity_df = dshs_hospital_capacity_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)

cur.execute("CREATE TABLE texas_capacity (Date TEXT, CovidHospOutOfCapacity FLOAT)") # Create table

clean_date = date(2020, 4, 11) # Data begins on April 11, 2020. 
                               # Use this date to generate clean date entries since several have errors

for i in dshs_hospital_capacity_df:
    col = dshs_hospital_capacity_df[i].values # Get the column values
    capac = re.sub("[\%|\'|\[|\]]", "", str(col))    # Strip the %, ', [, ] from capacity
    cur.execute("INSERT INTO texas_capacity (Date, CovidHospOutOfCapacity) VALUES('{}', {})".format(clean_date, capac))
                                                 # Add the entry to the table

    clean_date = clean_date + timedelta(days=1)  # Icrement the date

#for row in cur.execute("SELECT Date, CovidHospOutOfCapacity FROM texas_capacity"):
    #pass
    #print(row)

#--------------------------#
# DSHS ICU bed utilization #
#--------------------------#

# Create ICU beds available and Covid ICU beds DFs to calculate the ICU utilization
dshs_icu_beds_avail_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="ICU Beds Available")
dshs_icu_beds_avail_df = dshs_icu_beds_avail_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)

dshs_covid_icu_beds_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="COVID-19 ICU")
dshs_covid_icu_beds_df = dshs_covid_icu_beds_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)

col_names_dshs = [str(col) for col in dshs_icu_beds_avail_df.columns] # Generate a the column declaration for the table

dshs_icu_bed_utilization_df = dshs_covid_icu_beds_df/(dshs_covid_icu_beds_df + dshs_icu_beds_avail_df)

cur.execute("CREATE TABLE texas_icu_utilization (Date TEXT, ICU_Utilization INTEGER)") # Create table

clean_date = date(2020, 4, 11) # Data begins on April 11, 2020. 
                               # Use this date to generate clean date entries since several have errors

for i in dshs_icu_bed_utilization_df:
    col = dshs_icu_bed_utilization_df[i].values   # Get the column values
    utilization = re.sub("[\[|\]]", "", str(col)) # Strip the %, ', [, ] from capacity
    cur.execute("INSERT INTO texas_icu_utilization (Date, ICU_Utilization) VALUES('{}', {})".format(clean_date, utilization))
                                                  # Add the entry to the table

    clean_date = clean_date + timedelta(days=1)   # Icrement the date

#for row in cur.execute("SELECT * FROM texas_icu_utilization"):
    #pass
    #print(row)

#-----------------------#
# Business applications #
#-----------------------#
business_app_url = "https://www.census.gov/econ/bfs/csv/bfs_monthly.csv"
business_app_df = pd.read_csv(business_app_url)

# Remove unnecessary rows/columns
business_app_df = business_app_df[business_app_df.series == "BA_BA" ]            # Only consider business applications
business_app_df = business_app_df[business_app_df.year >= 2020 ]                 # Only consider business applications since 2019
business_app_df = business_app_df[business_app_df.sa != "U"]                     # Use only seasonally adjusted rows
business_app_df = business_app_df[business_app_df.geo == "TX"]                   # Drop rows for states other than Texas
business_app_df = business_app_df.drop(columns=["sa", "naics_sector", "series", "geo"]) # Drop columns that are unnecessary

business_app_df = business_app_df.fillna("NULL") # Replace NaN with Null

cur.execute("CREATE TABLE texas_business_apps (Date TEXT, BusinessApps INTEGER)")  # Create table


day = 1 # Enter all monthly data on the first of the corresponding month

for i, row in business_app_df.iterrows():
    year = row[0]
    for month in range(1,12):
        d = date(year, month, day)
        apps = row[month]

        # Only add entries that are not null
        if apps != "NULL":
            cur.execute("INSERT INTO texas_business_apps (Date, BusinessApps) VALUES('{}', {})".format(d, apps))

#for row in cur.execute("SELECT * FROM texas_business_apps"):
    #pass
    #print(row)

#######################
##### COUNTY LEVEL#####
#######################

#-------------------------------#
# Confirmed Cases - Github Data #
#-------------------------------#
confirmed_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
confirmed_df = pd.read_csv(confirmed_url)

confirmed_df = confirmed_df[confirmed_df.iso2 == "US"] # Drop rows of data outside of the US
confirmed_df = confirmed_df.drop(columns=["UID", "iso2", "iso3", "FIPS", "code3", "Country_Region", "Lat", "Long_", "Combined_Key"]) # Drop columns that are unnecessary

#---------------------------#
# Death Cases - Github Data #
#---------------------------#
death_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
death_cases_df = pd.read_csv(death_cases_url)

death_cases_df = death_cases_df[death_cases_df.iso2 == "US"] # Drop rows of data outside of the US
death_cases_df = death_cases_df.drop(columns=["UID", "iso2", "iso3", "FIPS", "code3", "Country_Region", "Lat", "Long_", "Population", "Combined_Key"]) # Drop columns that are unnecessary

column_names_death = ["["+ str(col) + "]" for col in death_cases_df.columns] # Generate a the column declaration for the table
column_declaration_death = "Admin2 text, Province_State text, " + " int, ".join(column_names_death[2:]) + " int"

cur.execute("CREATE TABLE county_deaths (" + column_declaration_death + ")") # Create table

#for row in cur.execute("SELECT * FROM county_deaths"):
    #pass
    #print(row)

#---------------------#
# Unemployment Claims #
#---------------------#
unemployment_url = "https://www.twc.texas.gov/files/agency/weekly-claims-by-county-twc.xlsx"
unemployment_df = pd.read_excel(unemployment_url, header=2)

unemployment_df = unemployment_df.iloc[:254, :]             # Eliminate unnecessary rows (that do not contain data)
unemployment_df = unemployment_df.dropna(axis=1, how="all") # Drop any columns that are all NaN

column_names_dshs = ["["+ str(col) + "]" for col in unemployment_df.columns] # Generate a the column declaration for the table
column_declaration = "County text, " + " int, ".join(column_names_dshs[1:]) + " int"

cur.execute("CREATE TABLE county_unemployment (" + column_declaration + ")") # Create table

#for row in cur.execute("SELECT * FROM county_unemployment"):
    #pass
    #print(row)
