import sqlite3

con = sqlite3.connect("covid.db")
cur = con.cursor()

# Print county table
#for row in cur.execute("""SELECT * FROM county"""):
    #print(row)

# Print state table
#for row in cur.execute("""SELECT * FROM state"""):
    #print(row)

for row in cur.execute("""SELECT AVG(ICU_Utilization) FROM state"""):
    print(row)

con.commit()
con.close()