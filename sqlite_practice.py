import sqlite3

# Create connection object - database
con = sqlite3.connect('example.db')

# Create a cursor object to execute SQL commands
cur = con.cursor()

# Create table
#cur.execute('''CREATE TABLE stocks
#        (date text, trans text, symbol text, qty real, price real)''')

# Add row
#cur.execute("INSERT INTO stocks VALUES('2021-05-03','BUY','RHAT',100,35.14)")
#cur.execute("INSERT INTO stocks VALUES('2006-03-28', 'BUY', 'IBM', 1000, 45.0)")
#cur.execute("INSERT INTO stocks VALUES('2006-04-06', 'SELL', 'IBM', 500, 53.0)")
#cur.execute("INSERT INTO stocks VALUES('2006-04-05', 'BUY', 'MSFT', 1000, 72.0)")


for row in cur.execute("SELECT * from stocks ORDER BY price"):
    print(row)



# Create programming language table
#cur.execute("CREATE TABLE lang(lang_name, lang_age)")

#cur.execute("INSERT INTO lang VALUES(?, ?)", ("C", 49))

# The qmark style used with executemany():
lang_list = [
    ("Fortran", 64),
    ("Python", 30),
    ("Go", 11),
]
#cur.executemany("INSERT INTO lang VALUES (?, ?)", lang_list)

# And this is the named style:
#cur.execute("SELECT * FROM lang WHERE lang_name=:name and lang_age=:age",
 #           {"name": "C", "age": 49})
#print(cur.fetchall())

for row in cur.execute("SELECT * FROM lang"):
    print(row)


# Save/commit the db
con.commit()

# Close db after it is committed
con.close()