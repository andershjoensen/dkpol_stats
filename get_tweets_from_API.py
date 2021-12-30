import requests
import config_params as cp
import sqlalchemy


def get_recent_tweets():


    params = {'query':  "#dkpol" }


    response = requests.get(cp.SEARCH_URL, headers=cp.HEADERS, params = params)

    print(response.text)


#get_recent_tweets()



def connect_to_db():
    engine = sqlalchemy.create_engine(cp.DB_CONN_STRING)

    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text("SELECT 1"))
        for row in result:
            print(row)
            

connect_to_db()
