import os
import pandas as pd
from sqlalchemy import create_engine


engine = create_engine("postgresql+psycopg2://postgres:tree@localhost:5432/vendor_Performance")

def ingest_db(df, table_name, engine):
    # this function will ingest the df to postgres db
    df.to_sql(table_name, if_exists="replace", index=False, con=engine)

def load_raw_data():
    # this function will load the csv as df 
    for file in os.listdir("../data"):
        if ".csv" in file:
            df = pd.read_csv("../data/"+ file)
            print(df.shape)
            ingest_db(df, file[:-4], engine)

if __name__=="__main__":
    load_raw_data()