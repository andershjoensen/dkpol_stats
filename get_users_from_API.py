import requests
import config_params as cp
import db_manager as dm

import sqlalchemy
from datetime import date
import datetime
import pandas as pd
import time





def extract_user_data(user_data):
    pub_metrics = user_data['public_metrics']

    del user_data['public_metrics']
    user_data['following_count'] = pub_metrics['following_count']
    user_data['followers_count'] = pub_metrics['followers_count']
    user_data['tweet_count'] = pub_metrics['tweet_count']
    user_data['listed_count'] = pub_metrics['listed_count']
    return user_data


def query_user_endpoint(author_ids_string):

    query_params = {'ids': author_ids_string,

                'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                'next_token': {}
                }

    response = requests.get(cp.USER_URL, headers=cp.HEADERS, params = query_params)

    resp_json = response.json()

    if response.status_code != 200:
        print(response.status_code)
        return response.status_code, None


    user_rows = []

    for user_data in resp_json['data']:

        user_rows.append(extract_user_data(user_data))

    return response.status_code, user_rows


def get_user_stats(users):

    chunksize = 99

    users_to_pop = len(users)

    all_user_rows = []

    users_to_query = ",".join(users[:chunksize])[:-1]
    users = users[chunksize:]
    users_to_pop -= chunksize
    i = 1
    while users_to_pop > 0:
        print(users_to_pop, "users to pop. len of all fetched users:", len(all_user_rows))

        status_code, user_rows = query_user_endpoint(users_to_query)

        all_user_rows.extend(user_rows)

        if status_code != 200 or i %50 == 0:
            print("status_code:", status_code)
            print("writing chunk to db and waiting a bit")
            user_df = pd.DataFrame(all_user_rows)
            dm.append_df_to_tbl(user_df, "users", "twitter", 'append')

            time.sleep(15 * 60)


        users_to_query = ",".join(users[:chunksize])[:-1]
        users = users[chunksize:]
        users_to_pop -= chunksize


        i +=1

    status_code, user_rows = query_user_endpoint(users_to_query)

    all_user_rows.extend(user_rows)

    print("writing final chunk to db")
    user_df = pd.DataFrame(all_user_rows)

    print(user_df)
    dm.append_df_to_tbl(user_df, "users", "twitter", 'append')


candidate_user_ids = set(dm.query_db("select distinct author_id from twitter.tweets;").author_id.values)

existing_user_ids = set(dm.query_db("select distinct id from twitter.users;").id.values)

user_ids_to_process = list(candidate_user_ids.difference(existing_user_ids))

print(user_ids_to_process)

if len(user_ids_to_process) > 0:

    print(len(user_ids_to_process), "user ids to process")

    get_user_stats(user_ids_to_process)
else:
    print("no new users. quitting")
