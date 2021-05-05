import sqlite3

con = sqlite3.connect('covid.db')
cur = con.cursor()

cur.execute(
    """
    CREATE TABLE DSHS_data_over_time
    ()
    """ 
)

con.close()