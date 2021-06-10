import pandas as pd
import sqlite3
import re
import datetime
import time
from reader import feed

# Functions for use in the script
def to_sql_state(df, table_name, con, var_col_name, bus_apps=False):
    """ df - the dataframe
        table_name - the name of the SQL table to write to
        con - the name of the connection to the SQLite db
        var_col_name - the specific name of data column holding the measurements
    """
    # If we have the business application data, we need to generate the list of dates
    if bus_apps:
        # Figure out how many years are stored in the dataframe
        num_years = df.shape[0]

        df = df.melt()

        # Update month labels to be the date of the first of each month
        dates = generate_1st_of_the_month(2020, 2020 + num_years - 1)
        df['variable'] = dates

        # Drop the rows with NaNs & clean the dates
        df = df.dropna(axis=0)
    else:
        # Reshape the df from wide -> long format & clean the dates
        df = df.melt()                # First melt the data to reshape it
        
    df['variable'] = pd.to_datetime(df['variable'], errors="ignore") # Next make sure dates are in the proper format  

    # Create the SQL table
    df.to_sql(name=table_name, con=con, index=False)

    # Change the default names of the columns
    cur.execute("""ALTER TABLE {}
                    RENAME COLUMN 'variable' TO Date""".format(table_name))
    cur.execute("""ALTER TABLE {}
                    RENAME COLUMN 'value' TO {}""".format(table_name, var_col_name))

def to_sql_county(df, table_name, con, county_var, var_col_name):
    """ df - the dataframe
        table_name - the name of the SQL table to write to
        con - the name of the connection to the SQLite db
        id_var - the column that will be acting as the id
        var_col_name - the specific name of data column holding the measurements
    """
    # Reshape the df from wide -> long format & clean the dates
    df = df.melt(id_vars=county_var)                # First melt the data to reshape it
    df['variable'] = pd.to_datetime(df['variable']) # Next make sure dates are in the proper format  

    # Create the SQL table
    df.to_sql(name=table_name, con=con, index=False)

    # Change the default names of the columns
    cur.execute("""ALTER TABLE {}
                    RENAME COLUMN '{}' TO County""".format(table_name, county_var))
    cur.execute("""ALTER TABLE {}
                    RENAME COLUMN 'variable' TO Date""".format(table_name))
    cur.execute("""ALTER TABLE {}
                    RENAME COLUMN 'value' TO {}""".format(table_name, var_col_name))

def generate_1st_of_the_month(base_year, end_year):
    dates = []
    for month in range(1, 13):
        for offset in range(end_year - base_year + 1):
            dates.append('{}-01-{}'.format(month, end_year - offset))
    return dates


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

#######################
##### STATE LEVEL######
#######################

#------------------------#
# DSHS Hospital Capacity #
#------------------------#

# Form Texas capacity df 
dshs_data_over_time_url = "https://dshs.texas.gov/coronavirus/CombinedHospitalDataoverTimebyTSA.xlsx"
dshs_hospital_capacity_df = pd.read_excel(dshs_data_over_time_url, header=2, sheet_name="GA-32 COVID % Capacity")
dshs_hospital_capacity_df = dshs_hospital_capacity_df.iloc[22:23, 2:]  # Eliminate unnecessary rows and columns (that do not contain data)
dshs_hospital_capacity_df = dshs_hospital_capacity_df.replace({'%':''}, regex=True) # And strip the '%' from entries

# Add it to the sql database
to_sql_state(dshs_hospital_capacity_df, 'texas_capacity', con, 'CovidHospOutOfCapacity')

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

# Add it to the sql database
to_sql_state(dshs_icu_bed_utilization_df, 'texas_icu_utilization', con, 'ICU_Utilization')

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
business_app_df = business_app_df.drop(columns=["year", "sa", "naics_sector", "series", "geo"]) # Drop columns that are unnecessary

# Add the df to the SQL database
to_sql_state(business_app_df, "texas_business_apps", con, "BusinessApps", True)

print("Finished State Business Applications Table Initialization")

#######################
##### STATE TABLE######
#######################

cur.execute("""CREATE TABLE state AS
                SELECT
                    date(capac.Date),
                    capac.CovidHospOutOfCapacity,
                    icu.ICU_Utilization,
                    bus.BusinessApps
                FROM
                    texas_capacity capac
                    LEFT JOIN texas_icu_utilization icu ON date(capac.Date) = date(icu.Date)
                    LEFT JOIN texas_business_apps bus ON date(capac.Date) = date(bus.Date)
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

# Add it to the sql database
to_sql_county(confirmed_df, 'confirmed_by_county', con, 'Admin2', 'Confirmed')

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

# Add it to the sql database
to_sql_county(death_cases_df, 'deaths_by_county', con, 'Admin2', 'Deaths')

print("Finished County Deaths Table Initialization")

#---------------------#
# Unemployment Claims #
#---------------------#
unemployment_url = "https://www.twc.texas.gov/files/agency/weekly-claims-by-county-twc.xlsx"
unemployment_df = pd.read_excel(unemployment_url, header=2)

unemployment_df = unemployment_df.iloc[:254, :]             # Eliminate unnecessary rows (that do not contain data)
unemployment_df = unemployment_df.dropna(axis=1, how="all") # Drop any columns that are all NaN
unemployment_df = unemployment_df.fillna("NULL")            # And replace all NaN with Null

# Add it to the sql database
to_sql_county(unemployment_df, 'unemployment_by_county', con, 'County' , 'UnemploymentClaims')

print("Finished County Unemployment Claims Table Initialization")

#######################
##### COUNTY TABLE#####
#######################

cur.execute("""CREATE TABLE county AS 
                SELECT 
                    conf.County, 
                    date(conf.Date),
                    conf.Confirmed, 
                    death.Deaths, 
                    unemp.UnemploymentClaims 
                FROM 
                    confirmed_by_county conf
                    LEFT JOIN deaths_by_county death ON conf.Date=death.Date and conf.County=death.County
                    LEFT JOIN unemployment_by_county unemp ON conf.Date=unemp.Date and conf.County=unemp.County
                    """)

# Commit the changes and close the connection to the database
con.commit()
con.close()

toc = time.perf_counter()
print(f"Initialized in {toc - tic:0.4f} seconds")
# Usually runs in ~ 25 seconds

""" Fix list:
        * Create a seperate file with all of the hardcoded information
        * Download files using requests
"""