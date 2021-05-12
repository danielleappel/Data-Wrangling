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
try:
    cur.execute("DROP TABLE business_apps")
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

cur.execute("CREATE TABLE texas_capacity (Date text, CovidHospOutOfCapacity float)") # Create table

clean_date = date(2020, 4, 11) # Data begins on April 11, 2020. 
                               # Use this date to calculate clean date entries since several entries have errors

for date in dshs_hospital_capacity_df:
    col = dshs_hospital_capacity_df[date].values # Get the column values
    col = date, re.sub(r"[%|']", "", str(col))   # Strip the % and ' from entries
    date, capac = col[0], col[1][1:-1]           # Strip the brackets arround capacity
    
    cur.execute("INSERT INTO texas_capacity (Date, CovidHospOutOfCapacity) VALUES('{}', {})".format(str(clean_date), str(capac)))
                                                 # Add the entry to the table

    clean_date = clean_date + timedelta(days=1)  # Icrement the date

#for row in cur.execute("SELECT date(Date), CovidHospOutOfCapacity FROM texas_capacity"):
    #pass
    #print(row)

#--------------------------#
# DSHS ICU bed utilization #
#--------------------------#
# Use the ICU beds available and Covid ICU beds DFs to calculate the ICU utilization
dshs_icu_beds_avail_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="ICU Beds Available")
dshs_icu_beds_avail_df = dshs_icu_beds_avail_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)

dshs_covid_icu_beds_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="COVID-19 ICU")
dshs_covid_icu_beds_df = dshs_covid_icu_beds_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)

col_names_dshs = [str(col) for col in dshs_icu_beds_avail_df.columns] # Generate a the column declaration for the table

dshs_icu_bed_utilization = dshs_covid_icu_beds_df.values/(dshs_covid_icu_beds_df.values + dshs_icu_beds_avail_df.values)
dshs_icu_bed_utilization_df = pd.DataFrame(dshs_icu_bed_utilization, columns=col_names_dshs)

cur.execute("CREATE TABLE texas_icu_utilization (Date numeric, ICU_Utilization numeric)") # Create table
dshs_icu_bed_utilization_df.to_sql("texas_icu_utilization", con=con, if_exists="append", index=False) # Add the data to the table

#for row in cur.execute("SELECT [2021-02-14] FROM texas_icu_utilization"):
    #pass
    #print(row)

#-----------------------#
# Business applications #
#-----------------------#
business_app_url = "https://www.census.gov/econ/bfs/csv/bfs_monthly.csv"
business_app_df = pd.read_csv(business_app_url)

business_app_df = business_app_df[business_app_df.series == "BA_BA" ]            # Only consider business applications
business_app_df = business_app_df[business_app_df.year >= 2020 ]                 # Only consider business applications since 2019
business_app_df = business_app_df[business_app_df.sa != "A"]                     # Drop seasonally adjusted rows
business_app_df = business_app_df.drop(columns=["sa", "naics_sector", "series"]) # Drop columns that are unnecessary
business_app_df = business_app_df[business_app_df.geo != "US"]                   # Drop rows with no specified state
business_app_df = business_app_df[business_app_df.geo != "NO"]                   # Drop rows with no specified state
business_app_df = business_app_df[business_app_df.geo != "MW"]                   # Drop rows with no specified state
business_app_df = business_app_df[business_app_df.geo != "SO"]                   # Drop rows with no specified state
business_app_df = business_app_df[business_app_df.geo != "WE"]                   # Drop rows with no specified state

business_column_declaration = "geo text, " + " numeric, ".join(business_app_df.columns[1:]) + " numeric"
cur.execute("CREATE TABLE business_apps (" + business_column_declaration + ")")  # Create table

business_app_df.to_sql("business_apps", con=con, if_exists="append", index=False) # Add the data to the table

#for row in cur.execute("SELECT * FROM business_apps"):
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
death_cases_df.to_sql("county_deaths", con=con, if_exists="append", index=False) # Add the data to the table

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

unemployment_df.to_sql("county_unemployment", con=con, if_exists="append", index=False) # Add the data to the table

#for row in cur.execute("SELECT * FROM county_unemployment"):
    #pass
    #print(row)
