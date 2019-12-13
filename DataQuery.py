import pandas as pd
import psycopg2 as pcg
import sqlalchemy as sqlal
from collections import defaultdict
import threading

class DataQuery:
    @staticmethod
    def dataquerier(donation, dataengine, command, moneytable, moneyengine):
        #lock = threading.Lock
        #lock.acquire()
        conn = dataengine.connect()
        result = conn.execute(command)
        """
        dfresult = pd.DataFrame(result)
        print(dfresult)
        length = dfresult.count()
        contributors = dfresult[len(dfresult.columns)-1]
        portions = defaultdict(lambda: 0)
        money = defaultdict(lambda : 0)
        for name in contributors:
            portions[name] += 1
        mtdf = pd.read_sql_table(moneytable, moneyengine, columns=['Name', 'Money'])
        mtdf['Money'] = mtdf['Money'].apply(lambda x: float(x))
        for name in set(contributors):
            portions[name] = portions[name] / length[0]
            money[name] = portions[name] * donation

            if name in mtdf['Name']:
                mtdf.loc[mtdf['Name'] == name] += money[name]
            else:
                temp = pd.DataFrame([[name, money[name]]], columns=['Name', 'Money'])

                mtdf = mtdf.append(temp)

        mtdf.to_sql(moneytable, moneyengine, if_exists='replace')
        #lock.release()
        """