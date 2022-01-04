import sqlalchemy
import config_params as cp
import pandas as pd

def append_df_to_tbl(df, tbl_name, schema, if_exists='append'):
    engine = sqlalchemy.create_engine(cp.DB_CONN_STRING)

    df.to_sql(tbl_name, con=engine, schema=schema, if_exists=if_exists, chunksize=10**4, index=False)

    print(df.shape[0], " rows of data inserted into", tbl_name)


def query_db(query):
    engine = sqlalchemy.create_engine(cp.DB_CONN_STRING)
    df = pd.read_sql(query, con=engine)
    return df
