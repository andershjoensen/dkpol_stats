import requests
import config_params as cp
import db_manager as dm

import sqlalchemy
from datetime import date
import datetime
import pandas as pd
import time





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
    #resp_json = response.json()

    #print(resp_json['data'])



def handle_entities(data):
    #{'urls', 'hashtags', 'mentions', 'annotations'}

    #cols: tweet_id,mentioned_id,url,hashtag

    entity_rows = []

    for entity_type, entity_list in data['entities'].items():
        for entity_d in entity_list:

            entity_row_d = {}
            entity_row_d['tweet_id'] = data['id']

            #user mention case
            if "username" in entity_d:
                entity_row_d['mentioned_id'] = entity_d['id']

            #hashtag case
            elif "tag" in entity_d:
                entity_row_d["tag"] = entity_d['tag']

            #url case
            elif "url" in entity_d:
                entity_row_d['url'] = entity_d['expanded_url']
            elif "probability" in entity_d:
                entity_row_d['annotation_type'] = entity_d['type']
                entity_row_d['annontation_text'] = entity_d['normalized_text']

            else:
                print(entity_d)

        entity_rows.append(entity_row_d)

    return entity_rows


def handle_tweet_data(data):

    tweet_extract = {}

    tweet_extract['author_id'] = data['author_id']
    tweet_extract['tweet_id'] = data['id']
    tweet_extract['conversation_id'] = data['conversation_id']

    if 'referenced_tweets' in data:
        #tweet_extract['referenced_tweets'] = data['referenced_tweets'].replace("'", '"') #fix this when you know the data
        tweet_extract['referenced_type'] = data['referenced_tweets'][0]['type']
        tweet_extract['referenced_id'] = data['referenced_tweets'][0]['id']
    tweet_extract['created_at'] = datetime.datetime.strptime(data['created_at'],'%Y-%m-%dT%H:%M:%S.000Z')


    tweet_extract['text'] = data['text']


    tweet_extract['retweet_count'] = data['public_metrics']['retweet_count']
    tweet_extract['reply_count'] = data['public_metrics']['reply_count']
    tweet_extract['like_count'] = data['public_metrics']['like_count']
    tweet_extract['quote_count'] = data['public_metrics']['quote_count']

    return tweet_extract

def get_recent_tweets(max_results = 10, next_token = None):

    query_params = {'query': "#dkpol",
                #'start_time': start_date,
                #'end_time': end_date,
                'max_results': max_results,
                'tweet.fields': 'entities,id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                'next_token': {next_token}}


    response = requests.get(cp.SEARCH_URL, headers=cp.HEADERS, params = query_params)


    if response.status_code != 200:
        print("API limit reached")
        print(response.text)
        print()
        return response.status_code, None, None, None


    resp_json = response.json()
    next_token = None

    if "next_token" in resp_json['meta']:

        next_token = resp_json['meta']['next_token']

    result_count = resp_json['meta']['result_count']
    return response.status_code, next_token, result_count, resp_json['data']




def handle_all_responses(all_responses):

    all_tweet_rows = []
    all_entity_rows =  []
    typeset = set()

    for tweet_data in all_responses:
        all_tweet_rows.append(handle_tweet_data(tweet_data))

        all_entity_rows.extend(handle_entities(tweet_data))



    tweet_df = pd.DataFrame(all_tweet_rows)
    entity_df = pd.DataFrame(all_entity_rows)

    return tweet_df, entity_df




def main():


    #get_user_info("23321405")
    #return

    next_token = None
    result_cnt = 0
    request_cnt = 0

    keep_getting_tweets = True


    all_responses = []

    i = 1
    while keep_getting_tweets:
        status_code, next_token, n_results, response_list = get_recent_tweets(100, next_token)




        if status_code != 200 or i % 100 == 0:
            print("the status code is:", status_code,)
            print("Inserting data and sleeping for 20 minutes")


            #process the data
            tweet_df, entity_df = handle_all_responses(all_responses)

            #insert data into table
            dm.append_df_to_tbl(tweet_df, "tweets", "twitter", 'append')
            dm.append_df_to_tbl(tweet_df, "entity_tweets", "twitter", 'append')

            #clear
            del all_responses
            all_responses = []

            print("SLEEEPING ")
            time.sleep(20 * 60)
            status_code = 200

        i += 1

        all_responses.extend(response_list)

        result_cnt += n_results
        request_cnt += 1
        print(result_cnt, "results at,", request_cnt, "requests ")









    append_df_to_tbl(tweet_df, "tweets", "twitter", 'replace')


if __name__ == '__main__':
    main()
