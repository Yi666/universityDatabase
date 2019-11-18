import psycopg2
import csv
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ======================================================
# You need to create a database in the PostgreSQL server first, let's call it universityDatabase
conn = psycopg2.connect(user='postgres', password='admin', database = 'universityDatabase')
print("PostgreSQL connection is successful")
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
curs = conn.cursor()


# ======================================================
# Create a database, e.g.demoDatabase
curs.execute('create database demoDatabase')


# ======================================================
# Create a table by using Query command
commands = """create table contributor(
            ID serial primary key,
            name varchar(255) not null,
            demoData varchar(255) )
            """
# curs.execute(commands)

# ======================================================
# Import data
insert_query = "insert into contributor values {}".format("(1,'Abadi','first data')")
curs.execute(insert_query)


# Read csv file
"""
with open('demo.csv','r') as file:
    reader = csv.reader(file)
    next(reader) # skip header row
    for row in reader:
        curs.execute("insert into contributor values (%s, %s, %s)", row)
conn.commit()
"""


# ======================================================
# Visualize data
showData_query = "select * from contributor"
curs.execute(showData_query)
rows = curs.fetchall()
for i in rows:
    print(i)



# ======================================================
# Closing database connection
if(conn):
    curs.close()
    conn.close()
    print("PostgreSQL connection is closed")