import pandas as pd
import sqlite3
import re
import datetime
import math
import time
from reader import feed

# Time the code from start to end
tic = time.perf_counter()

con = sqlite3.connect('covid.db')
cur = con.cursor()

# Make sure all tables are dropped before initializing the database
commands = []
for row in cur.execute("""SELECT
                            'DROP TABLE ' || name
                        FROM
                            sqlite_master
                        WHERE
                            type = 'table'
                        """):
    commands.append(row[0])

for command in commands:
    cur.execute(command)

# Create Dates & DateID table
cur.execute("""CREATE TABLE dates (
                DateID INTEGER PRIMARY KEY,
                Date TEXT
            )""")

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
                DateID INTEGER PRIMARY KEY, 
                CovidHospOutOfCapacity FLOAT,
                FOREIGN KEY(DateID) 
                    REFERENCES dates(DateID)
            )""") # Create texas capacity table

# Iterate through the columns (since this is a 1xn df)
for i in dshs_hospital_capacity_df:
    # Only add entries to the table with valid dates
    try:
        d = datetime.datetime.strptime(i, r"%Y-%m-%d").date() # Strip the date

        # Add the first occurence of each date to the date table
        try:
            cur.execute("""INSERT INTO dates (Date)
                            VALUES('{}')""".format(d))
        except:
            pass

        capac = dshs_hospital_capacity_df[i].values      # Get the column values
        capac = re.sub("[\%|\'|\[|\]]", "", str(capac))  # Strip the %, ', [, ] from capacity

        cur.execute("""INSERT INTO texas_capacity (DateID, CovidHospOutOfCapacity) 
                        VALUES((SELECT DateID from dates where Date='{}'), {})""".format(d, capac))
                                                         # Add the date and capac to the capacity table
    except:
        pass

print("Finished State Hospital Capacity Table Initialization")

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
                DateID INTEGER PRIMARY KEY, 
                ICU_Utilization INTEGER,
                FOREIGN KEY(DateID) 
                    REFERENCES dates(DateID)
            )""") # Create table

for i in dshs_icu_bed_utilization_df:
    # Only add entries to the table with valid dates
    try:
        d = datetime.datetime.strptime(i, r"%Y-%m-%d").date() # Strip the date
        
        # Add the first occurence of each date to the date table
        try:
            cur.execute("""INSERT INTO dates (Date)
                            VALUES('{}')""".format(d))
        except:
            pass
        
        utilization = dshs_icu_bed_utilization_df[i].values   # Get the column values
        utilization = re.sub("[\[|\]]", "", str(utilization)) # Strip the %, ', [, ] from capacity
        
        cur.execute("""INSERT INTO texas_icu_utilization (DateID, ICU_Utilization) 
                    VALUES((SELECT DateID from dates where Date='{}'), {})""".format(d, utilization))
                                                      # Add the entry to the table
    except:
        pass

print("Finished State ICU Utilization Table Initialization")

#-----------------------#
# Business applications #
#-----------------------#
business_app_url = "https://www.census.gov/econ/bfs/csv/bfs_monthly.csv"
business_app_df = pd.read_csv(business_app_url)

# Remove unnecessary rows/columns
business_app_df = business_app_df[business_app_df.series == "BA_BA" ]  # Only consider business applications
business_app_df = business_app_df[business_app_df.year >= 2020 ]       # Only consider business applications starting in 2020
business_app_df = business_app_df[business_app_df.sa != "U"]           # Drop rows that are not seasonally adjusted
business_app_df = business_app_df[business_app_df.geo == "TX"]         # Drop rows for states other than Texas
business_app_df = business_app_df.drop(columns=["sa", "naics_sector", "series", "geo"]) # Drop columns that are unnecessary

business_app_df = business_app_df.fillna("NULL") # Replace NaN with Null

cur.execute("""CREATE TABLE texas_business_apps (
                DateID INTEGER PRIMARY KEY, 
                BusinessApps INTEGER,
                FOREIGN KEY(DateID) 
                    REFERENCES dates(DateID)
            )""")  # Create table

# Add business application entries to the table
day = 1 # Enter all monthly data on the 1st of the month
for i, row in business_app_df.iterrows():
    year = row[0]
    # Loop through the date columns
    for month in range(1,12):
        d = datetime.date(year, month, day)

        # Add the first occurence of each date to the date table
        try:
            cur.execute("""INSERT INTO dates (Date)
                            VALUES('{}')""".format(d))
        except:
            pass

        apps = row[month] # Grab the number of business applications

        # Add entries that are not null to the business apps table
        if apps != "NULL":
            cur.execute("""INSERT INTO texas_business_apps (DateID, BusinessApps) 
                            VALUES((SELECT DateID from dates where Date='{}'), {})""".format(d, apps))

print("Finished State Business Applications Table Initialization")

#######################
##### STATE TABLE######
#######################

cur.execute("""CREATE TABLE state AS
                SELECT
                    dates.Date,
                    capac.CovidHospOutOfCapacity,
                    icu.ICU_Utilization,
                    bus.BusinessApps
                FROM
                    dates
                    JOIN texas_capacity capac ON dates.DateID = capac.DateID
                    LEFT JOIN texas_icu_utilization icu ON capac.DateID = icu.DateID
                    LEFT JOIN texas_business_apps bus ON capac.DateID = bus.DateID
                """)

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
confirmed_df = confirmed_df[confirmed_df.Admin2 != "Unassigned"] # Drop rows with unassigned counties
confirmed_df = confirmed_df[confirmed_df.Admin2 != "Out of TX"] # Drop rows with unassigned counties

cur.execute("""CREATE TABLE confirmed_by_county (
                County TEXT, 
                DateID INTEGER, 
                Confirmed INTEGER,
                PRIMARY KEY(DateID, County)
                FOREIGN KEY(DateID)
                    REFERENCES dates(DateID)    
            )""")  # Create table

# Add entries to the table
for index, row in confirmed_df.iterrows():
    county = row[0]

    # Loop through the date columns
    for i in range(1, len(row)):
        d = confirmed_df.columns[i] # Grab the date for the current entry and clean it
        d = datetime.datetime.strptime(d, r"%m/%d/%y")
        d = d.date()

        # Add the first occurence of each date to the date table
        try:
            cur.execute("""INSERT INTO dates (Date)
                            VALUES('{}')""".format(d))
        except:
            pass

        confirmed = row[i] # Grab the number of confirmed cases

        cur.execute("""INSERT INTO confirmed_by_county (County, DateID, Confirmed) 
                        VALUES('{}', (SELECT DateID from dates where Date='{}'), {})""".format(county, d, confirmed))

print("Finished County Confirmed Cases Table Initialization")

#---------------------------#
# Death Cases - Github Data #
#---------------------------#
death_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
death_cases_df = pd.read_csv(death_cases_url)

death_cases_df = death_cases_df[death_cases_df.Province_State == "Texas"] # Drop rows of data outside of the US
death_cases_df = death_cases_df.drop(columns=["UID", "Province_State", "iso2", "iso3", "FIPS", "code3", "Country_Region", "Lat", "Long_", "Population", "Combined_Key"]) # Drop columns that are unnecessary
death_cases_df = death_cases_df[death_cases_df.Admin2 != "Unassigned"] # Drop rows with unassigned counties
death_cases_df = death_cases_df[death_cases_df.Admin2 != "Out of TX"] # Drop rows with unassigned counties

cur.execute("""CREATE TABLE deaths_by_county (
                County TEXT, 
                DateID INTEGER, 
                Deaths INTEGER,
                PRIMARY KEY(County, DateID)
                FOREIGN KEY(DateID) 
                    REFERENCES dates(DateID)
            )""")  # Create deaths table

# Add entries to the table
for index, row in death_cases_df.iterrows():
    county = row[0]

    # Loop through each date
    for i in range(1, len(row)):
        d = death_cases_df.columns[i] # Grab the date for the current entry and clean it
        d = datetime.datetime.strptime(d, r"%m/%d/%y")
        d = d.date()

        # Add the first occurence of each date to the date table
        try:
            cur.execute("""INSERT INTO dates (Date)
                            VALUES('{}')""".format(d))
        except:
            pass

        deaths = row[i] # Grab the number of deaths

        cur.execute("""INSERT INTO deaths_by_county (County, DateID, Deaths) 
                        VALUES('{}', (SELECT DateID from dates where Date='{}'), {})""".format(county, d, deaths))

print("Finished County Deaths Table Initialization")

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
                DateID INTEGER, 
                UnemploymentClaims INTEGER,
                PRIMARY KEY(County, DateID) 
                FOREIGN KEY(DateID)
                    REFERENCES dates(DateID)
            )""") # Create unemployment table

# Add entries to the table
for index, row in unemployment_df.iterrows():
    county = row[0]
    for col in range(1,len(row)):
        d = unemployment_df.columns[col] # The date for the current entry

        # If the date is not a datetime object, make it one
        if not isinstance(d, datetime.datetime):
            d = datetime.datetime.strptime(d, r"%m/%d/%Y")
        d = d.date()

        # Add the first occurence of each date to the date table
        try:
            cur.execute("""INSERT INTO dates (Date)
                            VALUES('{}')""".format(d))
        except:
            pass

        unemp_claims = row[col]
        
        # Only add entries that are not null
        if unemp_claims != "NULL":
            # Try add the data for the date to the table (so long as it is not a repeat)
            try:
                cur.execute("""INSERT INTO unemployment_by_county (County, DateID, UnemploymentClaims) 
                                VALUES('{}', (SELECT DateID from dates where Date='{}'), {})""".format(county, d, unemp_claims))
            except:
                pass
print("Finished County Unemployment Claims Table Initialization")

#######################
##### COUNTY TABLE#####
#######################

cur.execute("""CREATE TABLE county AS 
                SELECT 
                    conf.County, 
                    dates.Date, 
                    conf.Confirmed, 
                    death.Deaths, 
                    unemp.UnemploymentClaims 
                FROM 
                    dates
                    JOIN confirmed_by_county conf ON dates.DateID = conf.DateID
                    LEFT JOIN deaths_by_county death ON conf.DateID=death.DateID and conf.County=death.County
                    LEFT JOIN unemployment_by_county unemp ON conf.DateID=unemp.DateID and conf.County=unemp.County
                    """)

# Commit the changes and close the connection to the database
con.commit()
con.close()

toc = time.perf_counter()
print(f"Initialized in {toc - tic:0.4f} seconds")
# Usually runs in ~ 480 seconds/8 minutes 