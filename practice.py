import sqlite3

con = sqlite3.connect('example.db')

cur = con.cursor()

cur.execute('''CREATE TABLE stocks
        (date text, trans text, symbol text, qty real, price real)''')

cur.execute("INSERT INTO stocks VALUES('2021-05-03','BUY','RHAT',100,35.14)")

print(cur)