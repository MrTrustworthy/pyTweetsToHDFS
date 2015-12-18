__author__ = 'MrTrustworthy'

import oauth2
import json
import config
from time import time, sleep
from subprocess import call
from os import getcwd

replace = {
    "at"   : "%40",
    "hash" : "%23",
    "space": "+"
}

base_url = "https://api.twitter.com/1.1/search/tweets.json"

query = None


def encode_query(string):
    return string.replace("@", replace["at"]).replace("#", replace["hash"]).replace(" ", replace["space"])


def get_auth_client():
    consumer = oauth2.Consumer(key=config.keys["consumerKey"], secret=config.keys["consumerSecret"])
    access_token = oauth2.Token(key=config.keys["accessToken"], secret=config.keys["accessTokenSecret"])
    return oauth2.Client(consumer, access_token)


def create_initial_query():
    global query

    s = replace["space"]
    query = "".join([encode_query(val) + s + "OR" + s for val in config.keywords])[0:-4]


def save_file(data):
    if not data:
        print("No new data to save")
        return

    file_name = "filedump_" + str(int(time())) + ".json"
    with open(file_name, 'w', encoding='utf-8') as file:
        file.write("\n".join(data))

    # push file to hdfs
    original_path = getcwd() + "/" + file_name
    target_path = "/twitter/" + file_name
    call(["hdfs", "dfs", "-copyFromLocal", original_path, target_path])


def make_call():
    global query
    if query is None:
        create_initial_query()

    print(query)

    query_url = base_url + "?q=" + str(query)

    client = get_auth_client()
    response_header, response = client.request(query_url)
    data = json.loads(response.decode("utf-8"))

    # calculate new query based on twitter response to avoid having the same tweets twice
    query = data["search_metadata"]["query"] + "&since_id=" + data["search_metadata"]["max_id_str"]
    result = [status["created_at"] + "," + status["text"].replace("\n", " ") for status in data["statuses"]]
    return result


def main():
    while True:
        try:
            data = make_call()
            save_file(data)
        except Exception as e:
            global query
            query = None
            print("Error:", e)
        finally:
            sleep(config.lookup_interval)


if __name__ == "__main__":
    main()
