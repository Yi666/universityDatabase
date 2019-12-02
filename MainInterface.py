import pandas as pd
from pip._vendor.distlib.compat import raw_input
from sqlalchemy import create_engine

import DatabaseInitialization
import DataManipulator
import Administrator
import DataQuery

# ========================================================================
# Initialize the dataSet table
def createDataTable():
    database = DatabaseInitialization.DatabaseInitial
    database.databaseInitialization(database)
    # database.createTable(database,"Fire_Safety_Deficiencies.csv")
    database.turnOffDatabase(database)


# ========================================================================
# Initialize the adminTable and moneyTable
def connectEngine():
    adminEngine = create_engine('postgresql://postgres:admin@localhost:5432/adminDatabase')
    moneyEngine = create_engine('postgresql://postgres:admin@localhost:5432/moneyDatabase')
    dataEngine = create_engine('postgresql://postgres:admin@localhost:5432/universityDatabase')
    return (adminEngine, moneyEngine, dataEngine)

# ========================================================================
# Manipulate the moneyTable/adminTable directly by admin
def adminManipulation(moneyEngine, adminEngine):
    admin = Administrator.Admin(moneyEngine,adminEngine)
    admin.addadmin("testPerson","123")
    admin.adduser("testContributor1","123")

# ========================================================================
# Adding data into the dataset by contributors
def addData(dataEngine, fileName):
    manipulator = DataManipulator.DataManipulator
    tableName = fileName.split(".")[0]
    tableName = tableName.lower()
    manipulator.validate("testContributor1",dataEngine,fileName,tableName)

# ========================================================================
# Query data and distribute money to the contributors
def queryData(dataEngine, moneyEngine, queryCommand, donation):
    dataQuery = DataQuery.DataQuery
    moneyTable = "moneyTable"
    dataQuery.dataquerier(donation,dataEngine,queryCommand,moneyTable,moneyEngine)



def main():
    engines = connectEngine()
    adminEngine = engines[0]
    moneyEngine = engines[1]
    dataEngine = engines[2]

    username = raw_input("Enter username: ")
    print("your username is: " + username)
    password = raw_input("Enter password: ")
    print("your password is: ******")

    admin = Administrator.Admin(moneyEngine, adminEngine)
    usernamePasswordMatch = admin.idcheck(username,password)
    if not usernamePasswordMatch:
        print("username and password not match")
        return
    while True:
        contributeOrQuery = raw_input("want contribute or query data: Y for contribute, N for query")
        if (contributeOrQuery):
            # contribute
            filename = raw_input("Enter your file name:")
            addData(dataEngine,filename)

        else:
            # query
            donation = raw_input("Enter the money you want to donate:")
            haveEnoughMoney = admin.checkmoney(username,donation)
            if not haveEnoughMoney:
                print("your do not have enough money")
                return
            queryCommand = raw_input("Enter your query requests:")
            queryData(dataEngine,moneyEngine,queryCommand,donation)









if __name__ == '__main__':
    main()