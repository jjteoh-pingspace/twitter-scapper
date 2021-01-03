import requests
import pandas as pd
import json
import ast
import yaml
import datetime


def process_yaml():
    with open("config.yaml") as file:
        return yaml.safe_load(file)

def create_bearer_token(data):
    return data["search_tweets_api"]["bearer_token"]

def twitter_auth_and_connect(bearer_token, url):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    response = requests.request("GET", url, headers=headers)
    print(response)
    return response.json()

def create_twitter_url():
    # start_date = datetime.datetime(2020, 1, 1)
    # end_date = datetime.datetime(2021, 1, 1)

    q = "to:tescomalaysia OR @tescomalaysia OR %23tescomalaysia OR tescomalaysia OR tesco -is:retweet"
    mrf = "max_results={}".format(100)
    tweet_fields = 'tweet.fields=created_at,lang'
    # place_fields = 'place.fields=country_code'
    # sdq = "start_time={}".format(start_date)
    # edq = "end_time={}".format(end_date)

    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}".format(
        q, tweet_fields, mrf
    )

    return url

def save_json(file_name, file_content):
  with open(file_name, 'w', encoding='utf-8') as f:
    json.dump(file_content, f, ensure_ascii=False, indent=4)

def json_to_csv(json_file, csv_file, header):
    jsonContent = json.load(open(json_file, encoding="utf8"))
    # results = pd.read_json (json_file)

    data = { 'id': [],'text':[],'created_at':[], 'lang': []}

    for t in jsonContent['data']:
        data['id'].append(t['id'])
        data['text'].append(t['text'])
        data['created_at'].append(t['created_at'])
        data['lang'].append(t['lang'])

    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=None, mode="a", header=header)

# 1 request return 100, 50 request = 5000 rows
def recursive_scape(bearer_token, url, next_token, counter):
    new_url = url

    if next_token:
        new_url = "{}&next_token={}".format(url, next_token)

    print("calling to url: {}".format(new_url))
    res_json = twitter_auth_and_connect(bearer_token, new_url)
    save_json('tesco.json', res_json)
    
    header = False
    if counter == 0:
        header = True

    json_to_csv(r'tesco.json', r'tesco_scaped.csv', header)

    if( res_json['meta']['next_token'] and counter < 50):
        next_token = res_json['meta']['next_token']
        counter += 1
        recursive_scape(bearer_token, url, next_token, counter)

def main():
    api_config = process_yaml() # api config
    bearer_token = create_bearer_token(api_config)
    url = create_twitter_url()
    # last_token = 'b26v89c19zqg8o3foshtsjuxot22t1kof0wb9mltkmt8d'
    recursive_scape(bearer_token, url, None, 0)
    

if __name__ == "__main__":
    main()