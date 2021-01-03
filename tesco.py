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

    q = "q=to:tescomalaysia OR @tescomalaysia OR #tescomalaysia OR tescomalaysia -is:retweet"
    max_results = 100
    mrf = "count={}".format(max_results)
    # sdq = "start_time={}".format(start_date)
    # edq = "end_time={}".format(end_date)

    # url = "https://api.twitter.com/2/tweets/search/all?{}&{}&{}&{}".format(
    #     mrf, q, sdq, edq
    # )

    url = "https://api.twitter.com/1.1/search/tweets.json?tweet_mode=extended&{}&{}".format(
        q, mrf
    )
    return url

def save_json(file_name, file_content):
  with open(file_name, 'w', encoding='utf-8') as f:
    json.dump(file_content, f, ensure_ascii=False, indent=4)

def json_to_csv(json_file, csv_file):
    jsonContent = json.load(open(json_file, encoding="utf8"))
    # results = pd.read_json (json_file)

    data = {'full_text':[],'created_at':[]}

    for t in jsonContent['statuses']:
        data['full_text'].append(t['full_text'])
        data['created_at'].append(t['created_at'])

    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=None)

def main():
    url = create_twitter_url()
    print(url)
    api_config = process_yaml() # api config
    bearer_token = create_bearer_token(api_config)
    res_json = twitter_auth_and_connect(bearer_token, url)
    save_json('tesco.json', res_json)
    json_to_csv(r'tesco.json', r'tesco.csv')

if __name__ == "__main__":
    main()