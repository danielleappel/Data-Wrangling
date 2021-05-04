import pandas as pd
import glob
import requests
import xlrd
import sqlite3


# GITHUB DATA
github_covid_path = r".\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports_us"
filenames = glob.glob(github_covid_path + "/*.csv")

dataframes = []

for filename in filenames:
    dataframes.append(pd.read_csv(filename))

#print(dataframes[0])


# DSHS Data over Time
dshs_texas_url = "https://dshs.texas.gov/coronavirus/CombinedHospitalDataoverTimebyTSA.xlsx"
r = requests.get(dshs_texas_url)

file=open("./dshs.xlsx", 'wb')
file.write(r.content)
file.close()

dataframes.append(pd.read_excel("./dshs.xlsx", header=2))

print(dataframes[-1])
# THIS IS NOT WORKINGGGGGG





#
