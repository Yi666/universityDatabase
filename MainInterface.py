import pandas as pd
from sqlalchemy import create_engine

import DatabaseInitialization
import DataManipulator
import Administrator
import DataQuery

# ========================================================================
# Initialize the dataSet table

database = DatabaseInitialization.DatabaseInitial
database.databaseInitialization(database)
# database.createTable(database,"Fire_Safety_Deficiencies.csv")
database.turnOffDatabase(database)


# ========================================================================
# Initialize the adminTable and moneyTable
adminEngine = create_engine('postgresql://postgres:admin@localhost:5432/adminDatabase')
moneyEngine = create_engine('postgresql://postgres:admin@localhost:5432/moneyDatabase')
dataEngine = create_engine('postgresql://postgres:admin@localhost:5432/universityDatabase')

# ========================================================================
# Manipulate the moneyTable/adminTable directly by admin
admin = Administrator.Admin(moneyEngine,adminEngine)
admin.addadmin("testPerson","123")
admin.adduser("testContributor1","123")

# ========================================================================
# Adding data into the dataset by contributors
"""
manipulator = DataManipulator.DataManipulator
fileName = "Fire_Safety_Deficiencies.csv"
tableName = fileName.split(".")[0]
tableName = tableName.lower()
manipulator.validate("testContributor1",dataEngine,fileName,tableName)
"""
# ========================================================================
# Query data and distribute money to the contributors

dataQuery = DataQuery.DataQuery
donation = 100
command = "select * from fire_safety_deficiencies"
moneyTable = "moneyTable"
dataQuery.dataquerier(100,dataEngine,command,moneyTable,moneyEngine)
