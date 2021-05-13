import pandas as pd
import sqlite3
import re
import datetime
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
try:
    cur.execute("DROP TABLE confirmed_by_county")
except:
    pass
try:
    cur.execute("DROP TABLE county")
except:
    pass
try:
    cur.execute("DROP TABLE state")
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

cur.execute("""CREATE TABLE texas_capacity (
                Date TEXT PRIMARY KEY, 
                CovidHospOutOfCapacity FLOAT
            )""") # Create table

for i in dshs_hospital_capacity_df:
    # Only add entries to the table with valid dates
    try:
        d = datetime.datetime.strptime(i, r"%Y-%m-%d").date() # Strip the date
        col = dshs_hospital_capacity_df[i].values      # Get the column values
        capac = re.sub("[\%|\'|\[|\]]", "", str(col))  # Strip the %, ', [, ] from capacity
        cur.execute("INSERT INTO texas_capacity (Date, CovidHospOutOfCapacity) VALUES('{}', {})".format(d, capac))
                                                       # Add the entry to the table
    except:
        pass

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

dshs_icu_bed_utilization_df = dshs_covid_icu_beds_df/(dshs_covid_icu_beds_df + dshs_icu_beds_avail_df)

cur.execute("""CREATE TABLE texas_icu_utilization (
                Date TEXT PRIMARY KEY, 
                ICU_Utilization INTEGER
            )""") # Create table

for i in dshs_icu_bed_utilization_df:
    # Only add entries to the table with valid dates
    try:
        d = datetime.datetime.strptime(i, r"%Y-%m-%d").date() # Strip the date
        col = dshs_icu_bed_utilization_df[i].values   # Get the column values
        utilization = re.sub("[\[|\]]", "", str(col)) # Strip the %, ', [, ] from capacity
        cur.execute("""INSERT INTO texas_icu_utilization (Date, ICU_Utilization) 
                    VALUES('{}', {})""".format(d, utilization))
                                                      # Add the entry to the table
    except:
        pass

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

cur.execute("""CREATE TABLE texas_business_apps (
                Date TEXT PRIMARY KEY, 
                BusinessApps INTEGER
            )""")  # Create table

day = 1 # Enter all monthly data on the 1st of the month

# Add entries to the table
for i, row in business_app_df.iterrows():
    year = row[0]
    for month in range(1,12):
        d = datetime.date(year, month, day)
        apps = row[month]

        # Only add entries that are not null
        if apps != "NULL":
            cur.execute("""INSERT INTO texas_business_apps (Date, BusinessApps) 
                            VALUES('{}', {})""".format(d, apps))

#for row in cur.execute("SELECT * FROM texas_business_apps"):
    #pass
    #print(row)

#######################
##### STATE TABLE######
#######################

cur.execute("""CREATE TABLE state AS
                SELECT
                    capac.Date,
                    capac.CovidHospOutOfCapacity,
                    icu.ICU_Utilization,
                    bus.BusinessApps
                FROM
                    texas_capacity capac
                    LEFT JOIN texas_icu_utilization icu ON capac.Date = icu.Date
                    LEFT JOIN texas_business_apps bus ON capac.Date = bus.Date
                """)

#for row in cur.execute("""SELECT * FROM state"""):
    #print(row)

#######################
##### COUNTY LEVEL#####
#######################

#-------------------------------#
# Confirmed Cases - Github Data #
#-------------------------------#
confirmed_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
confirmed_df = pd.read_csv(confirmed_url)

confirmed_df = confirmed_df[confirmed_df.Province_State == "Texas"] # Drop rows of data outside of the US
confirmed_df = confirmed_df.drop(columns=["UID", "Province_State", "iso2", "iso3", "FIPS", "code3", "Country_Region", "Lat", "Long_", "Combined_Key"]) # Drop columns that are unnecessary

cur.execute("""CREATE TABLE confirmed_by_county (
                County TEXT, 
                Date TEXT, 
                Confirmed INTEGER,
                PRIMARY KEY(County, Date)    
            )""")  # Create table

# Add entries to the table
for index, row in confirmed_df.iterrows():
    county = row[0]

    for i in range(1, len(row)):
        d = confirmed_df.columns[i] # Grab the date for the current entry and clean it
        d = datetime.datetime.strptime(d, r"%m/%d/%y")
        d = d.date()

        confirmed = row[i] # Grab the number of confirmed cases
        
        cur.execute("""INSERT INTO confirmed_by_county (County, Date, Confirmed) 
                        VALUES('{}', '{}', {})""".format(county, d, confirmed))

#for row in cur.execute("SELECT * FROM confirmed_by_county"):
    #pass
    #print(row)

#---------------------------#
# Death Cases - Github Data #
#---------------------------#
death_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
death_cases_df = pd.read_csv(death_cases_url)

death_cases_df = death_cases_df[death_cases_df.Province_State == "Texas"] # Drop rows of data outside of the US
death_cases_df = death_cases_df.drop(columns=["UID", "Province_State", "iso2", "iso3", "FIPS", "code3", "Country_Region", "Lat", "Long_", "Population", "Combined_Key"]) # Drop columns that are unnecessary

cur.execute("""CREATE TABLE deaths_by_county (
                County TEXT, 
                Date TEXT, 
                Deaths INTEGER,
                PRIMARY KEY(County, Date)
            )""")  # Create table

# Add entries to the table
for index, row in death_cases_df.iterrows():
    county = row[0]

    for i in range(1, len(row)):
        d = death_cases_df.columns[i] # Grab the date for the current entry and clean it
        d = datetime.datetime.strptime(d, r"%m/%d/%y")
        d = d.date()

        deaths = row[i] # Grab the number of deaths

        cur.execute("""INSERT INTO deaths_by_county (County, Date, Deaths) 
                        VALUES('{}', '{}', {})""".format(county, d, deaths))

#for row in cur.execute("SELECT * FROM deaths_by_county"):
    #pass
    #print(row)

#---------------------#
# Unemployment Claims #
#---------------------#
unemployment_url = "https://www.twc.texas.gov/files/agency/weekly-claims-by-county-twc.xlsx"
unemployment_df = pd.read_excel(unemployment_url, header=2)

unemployment_df = unemployment_df.iloc[:254, :]             # Eliminate unnecessary rows (that do not contain data)
unemployment_df = unemployment_df.dropna(axis=1, how="all") # Drop any columns that are all NaN
unemployment_df = unemployment_df.fillna("NULL")            # And replace all NaN with Null

cur.execute("""CREATE TABLE unemployment_by_county (
                County TEXT, 
                Date TEXT, 
                UnemploymentClaims INTEGER,
                PRIMARY KEY(County, Date) 
            )""") # Create table

# Add entries to the table
for index, row in unemployment_df.iterrows():
    county = row[0]

    for col in range(1,len(row)):
        d = unemployment_df.columns[col] # The date for the current entry

        # If the date is not a datetime object, make it one
        if not isinstance(d, datetime.datetime):
            d = datetime.datetime.strptime(d, r"%m/%d/%Y")
        d = d.date()

        unemp_claims = row[col]

        # Only add entries that are not null
        if unemp_claims != "NULL":
            print("""INSERT INTO unemployment_by_county (County, Date, UnemploymentClaims) 
                            VALUES('{}', '{}', {})""".format(county, d, unemp_claims))

            cur.execute("""INSERT INTO unemployment_by_county (County, Date, UnemploymentClaims) 
                            VALUES('{}', '{}', {})""".format(county, d, unemp_claims))

#for row in cur.execute("SELECT * FROM unemployment_by_county"):
    #pass
    #print(row)

#######################
##### COUNTY TABLE#####
#######################

cur.execute("""CREATE TABLE county AS 
                SELECT conf.County, conf.Date, conf.Confirmed, death.Deaths, unemp.UnemploymentClaims FROM confirmed_by_county conf
                    LEFT JOIN deaths_by_county death
                        ON conf.Date=death.Date and conf.County=death.County
                    LEFT JOIN unemployment_by_county unemp
                        ON conf.Date=unemp.Date and conf.County=unemp.County """)

#for row in cur.execute("""SELECT * FROM county WHERE County="Johnson" """):
    #print(row)