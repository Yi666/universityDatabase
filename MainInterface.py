import pandas as pd
from pip._vendor.distlib.compat import raw_input
from sqlalchemy import create_engine

import DatabaseInitialization
import DataManipulator
import Administrator
import DataQuery
import random
import time
import threading

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
    dataEngine2 = create_engine('postgresql://postgres:admin@localhost:5432/universityDatabase2')
    dataEngine3 = create_engine('postgresql://postgres:admin@localhost:5432/universityDatabase3')
    dataEngine4 = create_engine('postgresql://postgres:admin@localhost:5432/universityDatabase4')
    dataEngine5 = create_engine('postgresql://postgres:admin@localhost:5432/universityDatabase5')
    return (adminEngine, moneyEngine, dataEngine, dataEngine2, dataEngine3, dataEngine4, dataEngine5)

# ========================================================================
# Manipulate the moneyTable/adminTable directly by admin
def adminManipulation(moneyEngine, adminEngine):
    admin = Administrator.Admin(moneyEngine,adminEngine)
    admin.addadmin("testPerson","123")
    admin.adduser("testContributor1","123")

# ========================================================================
# Adding data into the dataset by contributors
def addData(username, dataEngine, fileName):
    manipulator = DataManipulator.DataManipulator
    tableName = fileName.split(".")[0]
    tableName = tableName.lower()
    manipulator.validate(username,dataEngine,fileName,tableName)

# ========================================================================
# Query data and distribute money to the contributors
def queryData(dataEngine, moneyEngine, queryCommand, donation):
    dataQuery = DataQuery.DataQuery
    moneyTable = "moneytable"
    dataQuery.dataquerier(donation,dataEngine,queryCommand,moneyTable,moneyEngine)



def main():
    engines = connectEngine()
    adminEngine = engines[0]
    moneyEngine = engines[1]
    dataEngine = engines[2:5]
    admin = Administrator.Admin(moneyEngine, adminEngine)

    username = raw_input("Enter username: ")
    print("your username is: " + username)
    password = raw_input("Enter password: ")
    print("your password is: ******")


    usernamePasswordMatch = admin.idcheck(username,password)
    if not usernamePasswordMatch:
        print("username and password not match")
        return
    while True:
        contributeOrQuery = raw_input("want contribute or query data: Y for contribute, N for query")
        if (contributeOrQuery == "Y" or contributeOrQuery == "y"):
            # contribute
            filename = raw_input("Enter your file name:")
            start_time = time.time()

            threads = [None]*len(dataEngine)

            for i in range(len(dataEngine)):
                class myThread(threading.Thread):
                    def __init__(self, threadID, name):
                        threading.Thread.__init__(self)
                        self.threadID = threadID
                        self.name = name

                    def run(self):
                        addData(username, dataEngine[i], filename)

                threads[i] = myThread(i,"thread"+str(i))
                print("thread"+str(i)+" started")

            for t in threads:
                t.start()
            for t in threads:
                t.join()

            elapsed_time = time.time() - start_time
            print(elapsed_time)
        else:
            # query

            donation = raw_input("Enter the money you want to donate:")
            haveEnoughMoney = admin.checkmoney(username,donation)
            if not haveEnoughMoney:
                print("your do not have enough money")
                return
            queryCommand = raw_input("Enter your query requests:")



            #Number of requests
            length = 5

            #With replica
            start_time = time.time()
            threads = [None] * length
            class queryThread(threading.Thread):
                def __init__(self, threadID):
                    threading.Thread.__init__(self)
                    self.threadID = threadID
                def run(self):
                    randomReplica = random.randint(0, len(dataEngine) - 1)
                    queryData(dataEngine[randomReplica], moneyEngine, queryCommand, int(donation))

            for i in range(length):
                threads[i] = queryThread(i)
                threads[i].start()
                print("thread" + str(i) + " started")
            for t in threads:
                t.join()
            elapsed_time = time.time() - start_time
            print("query takes time" + str(elapsed_time))

            # Without replica
            second_time = time.time()
            while length > 0:
                randomReplica = random.randint(0, len(dataEngine) - 1)
                queryData(dataEngine[randomReplica], moneyEngine, queryCommand, int(donation))
                length -= 1
            elapsed_time = time.time() - second_time
            print("query takes time with no replica" + str(elapsed_time))



if __name__ == '__main__':
    main()


