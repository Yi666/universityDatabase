import pandas as pd
import psycopg2 as pcg
import sqlalchemy as sqlal
from collections import defaultdict


class DataManipulator:
    # This class is for validating the data and merging the new data
    # with the old dataset. Instead of comparing the detailed values,
    # this method checks the corresponding hash value, which makes it
    # a little bit faster. After all the process, return true if the
    # new data is stored properly, return false when there is no new
    # data at all.

    @staticmethod
    def validate(contributer, dataengine, csv, dataset):
        newdf = pd.read_csv(csv)
        newdf = newdf.drop_duplicates(keep='first')
        hashes = newdf.apply(lambda x: str(hash(str(tuple(x)))), axis=1)
        newdf['hashes'] = hashes
        olddf = pd.read_sql_table(dataset, dataengine)
        oldhashes = set(olddf['hashes'])

        if (not olddf.empty):
            for i in oldhashes:
                newdf = newdf.loc[newdf['hashes'] != i]
            """
            for i in newdf['hashes']:
                if i in oldhashes:
                    newdf.drop(i)
            """
            # newdf=newdf.loc[newdf.Hashes not in oldhashes]
        if(newdf.empty):
            return False
        newdf['contributor'] = contributer
        newdf.to_sql(dataset, dataengine, if_exists='append')
        print("Successfully saved the csv data to the database")
        return True



