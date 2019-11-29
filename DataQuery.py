import pandas as pd
import psycopg2 as pcg
import sqlalchemy as sqlal
from collections import defaultdict


class DataQuery:
    @staticmethod
    def dataquerier(donation, dataengine, command, moneytable, moneyengine):
        conn = dataengine.connect()
        result = conn.execute(command)
        dfresult = pd.DataFrame(result)
        length = dfresult.count()
        contributors = dfresult[22]
        portions = defaultdict(lambda: 0)
        money = defaultdict()
        for name in contributors:
            portions[name] += 1
        mtdf = pd.read_sql_table(moneytable, moneyengine)
        mtdf['Money'] = mtdf['Money'].apply(lambda x: float(x))
        for name in set(contributors):
            portions[name] /= length
            money[name] = portions[name] * donation
            if name in mtdf['Name']:
                mtdf.loc[mtdf['Name'] == name] += money[name]
            else:
                temp = pd.DataFrame([name, money[name]], columns=['Name', 'Money'])
                mtdf = mtdf.append(temp)
        mtdf.to_sql(moneytable, moneyengine, if_exists='replace')
