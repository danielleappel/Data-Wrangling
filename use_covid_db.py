import sqlite3

con = sqlite3.connect("covid.db")
cur = con.cursor()

# Print state table
#for row in cur.execute("""SELECT * FROM state"""):
    #print(row)

# Print county table
for row in cur.execute("""SELECT * FROM county
                            WHERE county in ('Bee', 'Bell')
                            ORDER BY county"""):
    print(row)

# Calculate the 7 day non-overlapping average ICU Utilization
#for row in cur.execute("""SELECT Date, strftime('%W', Date) WeekNumber, AVG(ICU_Utilization)
                            #FROM state
                                #GROUP BY WeekNumber 
                                #ORDER BY Date
                        #"""):
    #print(row)

# Calculate the 7 day moving average ICU Utilization
#for row in cur.execute("""SELECT Date,
                            #AVG(ICU_Utilization) OVER(ORDER BY Date
                                #ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
                            #FROM state
                        #"""):
    #print(row)

con.commit()
con.close()