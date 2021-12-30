import requests
import config_params as cf



def get_recent_tweets():


    params = {'query':  "#dkpol" }

    
    response = requests.get(cf.SEARCH_URL, headers=cf.HEADERS, params = params)

    print(response.text)


get_recent_tweets()
