from collections import defaultdict
import pandas as pd
import sqlalchemy as sqlal
from collections import defaultdict
import psycopg2
import csv
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DatabaseInitial:
    def __init__(self):
        self.conn
        self.curs

    def databaseInitialization(self):
        self.conn = psycopg2.connect(user='postgres', password='admin', database='universityDatabase')
        print("PostgreSQL connection is successful")
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.curs = self.conn.cursor()

    # create a table if the file is a new dataset
    def createTable(self,fileName):
        # Read csv file
        with open(fileName, 'r') as file:
            reader = csv.reader(file)
            firstRow = next(reader)

        tableName = fileName.split(".")[0]
        createTableCommands = "create table " + tableName + "("
        str = " varchar(255)"

        createTableCommands += "index int, "

        for i in firstRow:
            splitStr = ""
            for j in i:
                if j != ' ':
                    splitStr += j
                else:
                    splitStr += ' '
            createTableCommands += "\"" + splitStr + "\"" + str + ", "
        createTableCommands += "Hashes" + str + ","
        createTableCommands += "Contributor" + str + ","
        createTableCommands = createTableCommands[:-1]
        createTableCommands += ")"

        print(createTableCommands)
        self.curs.execute(createTableCommands)
        self.conn.commit()

    def turnOffDatabase(self):
        if (self.conn):
            self.curs.close()
            self.conn.close()
            print("PostgreSQL connection is closed")


