import requests
import config_params as cp
import sqlalchemy
from datetime import date

def connect_to_db():
    engine = sqlalchemy.create_engine(cp.DB_CONN_STRING)

    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text("SELECT 1"))
        for row in result:
            print(row)


#connect_to_db()



def get_user_info(author_id):

    query_params = {'ids': author_id,
                #'start_time': start_date,
                #'end_time': end_date,
                #'max_results': 11,
                #'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                #'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                #'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                'next_token': {}
                }

    response = requests.get(cp.USER_URL, headers=cp.HEADERS, params = query_params)

    print(response.text)
    resp_json = response.json()

    #print(resp_json['data'])



def get_recent_tweets(next_token = None):

    query_params = {'query': "#dkpol",
                #'start_time': start_date,
                #'end_time': end_date,
                'max_results': 10,
                'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                'next_token': {next_token}}


    response = requests.get(cp.SEARCH_URL, headers=cp.HEADERS, params = query_params)

    print(response.status_code)

    resp_json = response.json()

    #tweet = resp_json['data'][0]
    print(resp_json['meta'])

    next_token = resp_json['meta']['next_token']
    result_count = resp_json['meta']['result_count']
    return next_token, result_count

    #for k,v in tweet.items():
    #    print(k,"::", v)

    #print(response.text['meta'])


#get_user_info("1290861566758277120")

next_token = None
result_cnt = 0
request_cnt = 0
while True:
    next_token, n_results = get_recent_tweets(next_token)
    result_cnt += n_results
    request_cnt += 1
    print(result_cnt, "results at,", request_cnt, "requests ")
    print()
    print()
