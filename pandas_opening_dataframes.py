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
    cur.execute("DROP TABLE county_unemployment")
except:
    pass

# Unemployment Claims
unemployment_url = "https://www.twc.texas.gov/files/agency/weekly-claims-by-county-twc.xlsx"
unemployment_df = pd.read_excel(unemployment_url, header=2)

unemployment_df = unemployment_df.iloc[:254, :]             # Eliminate unnecessary rows (that do not contain data)

unemployment_df = unemployment_df.dropna(axis=1, how="all") # Drop any columns that are all NaN

column_names = []
for col in unemployment_df.columns:                         # Generate a the column declaration for the table
    #if isinstance(col, datetime):
        #column_names.append(col.strftime("[%x]"))
    #else:
    column_names.append("[" + str(col) + "]")
column_declaration = "County text, " + " int, ".join(column_names[1:]) + " int"

cur.execute("CREATE TABLE county_unemployment (" + column_declaration + ")") # Create table

unemployment_df.to_sql("county_unemployment", con=con, if_exists="append", index=False) # Add the data to the table

for row in cur.execute("SELECT [4/10/2021] FROM county_unemployment"):
    print(row)