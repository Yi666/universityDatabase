import pandas as pd
import psycopg2 as pcg
import sqlalchemy as sqlal
from collections import defaultdict


class Admin:
    def __init__(self,moneyengine,adminengine):
        self.moneyengine=moneyengine
        self.adminengine=adminengine
        admintbl=pd.DataFrame([['YuWang999','314159','A'],['YiLiu123','123456','A']],columns=['Name','Password','Privilege'])
        if not adminengine.dialect.has_table(adminengine, 'admintable'):
            admintbl.to_sql('admintable',self.adminengine)
            print("create admin table, with administrators info inserted")
        else:
            print("admin table already created")
    def idcheck(self,name,password):
        admintbl=pd.read_sql_table('admintable',self.adminengine)
        return name in admintbl['Name'] and password==admintbl.loc[admintbl['Name']==name,[['Password']]]

    def addpeople(self,name,password,privilege):

        tadmintbl=pd.DataFrame([[name,password,privilege]],columns=['Name','Password','Privilege'])
        tadmintbl.to_sql('admintable',self.adminengine,if_exists='append')
        print("add a new user {}, with password {}, privilege {}".format(name, password, privilege))

    def adduser(self,name,password):
        self.addpeople(name, password, 'B')

    def addadmin(self,name,password):
        self.addpeople(name, password, 'A')

    def putmoney(self,name,money):
        mtdf=pd.read_sql_table('moneytable',self.moneyengine,columns=['Name','Money'])
        mtdf['Money'] = mtdf['Money'].apply(lambda x: float(x))
        if name in mtdf['Name']:
            mtdf.loc[mtdf['Name'] == name] += money[name]
        else:
            temp = pd.DataFrame([name, money], columns=['Name', 'Money'])
            mtdf = mtdf.append(temp)
        mtdf.to_sql('moneytable', self.moneyengine, if_exists='replace')

    def chargemoney(self,name,money):
        assert(money>0)
        self.putmoney(name,-money)

    def checkmoney(self,name,moneyToBeCharged):
        mtdf = pd.read_sql_table('moneytable', self.moneyengine, columns=['Name', 'Money'])
        mtdf['Money'] = mtdf['Money'].apply(lambda x: float(x))
        if mtdf.loc[mtdf['Name'] == name ] > moneyToBeCharged:
            return True
        else:
            return False
