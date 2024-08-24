import argparse
import requests
import configparser
import logging
import logging.handlers
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import json
import time

# python src\main.py -configfile_path "C:\Workspace\python\get_Instagram\configs\config_instagram.ini" -csv_path "C:\Workspace\python\get_Instagram\data" -sleep_sec 0.3

BASE_URL = """https://graph.facebook.com/{api_version}/{id}"""

# args
parser = argparse.ArgumentParser(description='This script gets Instagram data. ')
parser.add_argument('-configfile_path', help = "Set path including config file name e.g. C:/Users/hogehoge/config.ini ", required = True)
parser.add_argument('-csv_path', help = "Set path to save csv path e.g. C:/Users/hogehoge", required = True)
parser.add_argument('-csv_prefix_name', help = "Set prefix csv name as [prefix name]hogehoge[suffix name].csv", default = "", required = False)
parser.add_argument('-csv_suffix_name', help = "Set suffix csv name as [prefix name]hogehoge[suffix name].csv", default = "", required = False)
parser.add_argument('-sleep_sec', help = "Set wait time [s] per 1 requests to avoid HTTPSConnectionPool Error. ", default = 0, required = False)
args = parser.parse_args()

# config
config_ini = configparser.ConfigParser()
config_ini.read(args.configfile_path, encoding='utf-8')

# logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

format = "%(levelname)-9s  %(asctime)s [%(filename)s:%(lineno)d] %(message)s"

st_handler = logging.StreamHandler()
st_handler.setLevel(logging.DEBUG)
st_handler.setFormatter(logging.Formatter(format))

fl_handler = logging.handlers.TimedRotatingFileHandler(filename= config_ini["LOG"]["PATH"] + "/" + config_ini["LOG"]["FILENAME"] + ".log", encoding="utf-8", when = "MIDNIGHT")
fl_handler.setLevel(logging.DEBUG)
fl_handler.setFormatter(logging.Formatter(format))

logger.addHandler(st_handler)
logger.addHandler(fl_handler)

logger.info("start instagram")

def get_me():
    endpoint = BASE_URL.format(api_version = config_ini['DEFAULT']['API_VERSION'],
                                id = json.loads(config_ini['DEFAULT']['IG_USER_ID'])
                                ) + "/"
    params = {
        "access_token" : config_ini['DEFAULT']['ACCESS_TOKEN'],
        "fields" : "business_discovery.username(" + config_ini['DEFAULT']['USER_NAME'] + "){name,username,biography,follows_count,followers_count,media_count}"
    }
    res = requests.get(endpoint, params = params)

    if(res.status_code == 200):
        data = res.json()
        print(f"Successfully set data at get_me.")
        logger.info(f"Successfully set data at get_me.")
        return pd.DataFrame(data = data["business_discovery"], index=[0])
    else:
        logger.error(f"Error at get_me. status code: {res.status_code}. {res.text}")
        raise Exception(f"Error at get_me status code: {res.status_code}. {res.text}")

# media
media = {
    "caption": [],
    "comments_count": [],
    "id": [],
    "ig_id": [],
    "is_comment_enabled": [],
    "is_shared_to_feed": [],
    "like_count": [],
    "media_product_type": [],
    "media_type": [],
    "media_url": [],
    "owner": [],
    "permalink": [],
    "shortcode": [],
    "thumbnail_url": [],
    "timestamp": [],
    "username": [],
}
def get_media(sleep_sec = 0):
    endpoint = BASE_URL.format(api_version = config_ini['DEFAULT']['API_VERSION'], id = config_ini['DEFAULT']['IG_USER_ID']) + "/media"
    params = {
        "access_token" : config_ini['DEFAULT']['ACCESS_TOKEN'],
        "fields": ",".join(media.keys())
    }
    cnt = 0
    while(True):
        res = requests.get(endpoint, params = params)
        time.sleep(sleep_sec)
        if(res.status_code == 200):
            data = res.json()
            for row in data["data"]:
                media["caption"] += [row["caption"]] if "caption" in row else [""]
                media["comments_count"] += [row["comments_count"]] if "comments_count" in row else [""]
                media["id"] += [row["id"]] if "id" in row else [""]
                media["ig_id"] += [row["ig_id"]] if "ig_id" in row else [""]
                media["is_comment_enabled"] += [row["is_comment_enabled"]] if "is_comment_enabled" in row else [""]
                media["is_shared_to_feed"] += [row["is_shared_to_feed"]] if "is_shared_to_feed" in row else [""] # media_product_typeがREELのときのみ取得可能。
                media["like_count"] += [row["like_count"]] if "like_count" in row else [""]
                media["media_product_type"] += [row["media_product_type"]] if "media_product_type" in row else [""]
                media["media_type"] += [row["media_type"]] if "media_type" in row else [""]
                media["media_url"] += [row["media_url"]] if "media_url" in row else [""]
                media["owner"] += [row["owner"]["id"]] if "owner" in row else [""]
                media["permalink"] += [row["permalink"]] if "permalink" in row else [""]
                media["shortcode"] += [row["shortcode"]] if "shortcode" in row else [""]
                media["thumbnail_url"] += [row["thumbnail_url"]] if "thumbnail_url" in row else [""] # media_typeがVIDEOのときみ取得可能。
                media["timestamp"] += [(datetime.datetime.strptime(row["timestamp"], '%Y-%m-%dT%H:%M:%S+0000') + relativedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")] if "timestamp" in row else [""]
                media["username"] += [row["username"]] if "username" in row else [""]
        else:
            pass

        if "next" in data["paging"].keys():
            endpoint = data["paging"]["next"]
        else:
            break
        print("media info: {cnt}".format(cnt = cnt))
        cnt += 1

        if(cnt > 5): break # debug

    print(f"Successfully set data at get_media.")
    logger.info(f"Successfully set data at get_media.")
    return pd.DataFrame(data = media)

# comment
comment = {
    "hidden": [],
    "id": [],
    "text": [],
    "like_count": [],
    "timestamp": [],
    "username": [],
    "media_id": [],
}
def get_comments(df_media, sleep_sec = 0):

    params = {
        "access_token" : config_ini['DEFAULT']['ACCESS_TOKEN'],
        "fields": "hidden,id,text,like_count,timestamp,username"
    }
    cnt = 0
    for media_id in df_media["id"]:
        print("comments: {cnt}/{total}".format(cnt = cnt, total = len(df_media["id"])))
        endpoint = BASE_URL.format(api_version = config_ini['DEFAULT']['API_VERSION'], id = media_id) + "/comments"
        res = requests.get(endpoint, params = params)
        time.sleep(sleep_sec)
        if(res.status_code == 200):
            data = res.json()
            for row in data["data"]:
                comment["hidden"] += [row["hidden"]]
                comment["id"] += [row["id"]]
                comment["text"] += [row["text"]]
                comment["like_count"] += [row["like_count"]]
                comment["timestamp"] += [(datetime.datetime.strptime(row["timestamp"], '%Y-%m-%dT%H:%M:%S+0000') + relativedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")]
                comment["username"] += [row["username"]]
                comment["media_id"] += [media_id]

        else:
            pass
        cnt += 1

        if(cnt > 50): break # debug

    print(f"Successfully set data at get_comment.")
    logger.info(f"Successfully set data at get_comment.")
    return pd.DataFrame(data = comment)

reels = {
    "media_id": [],
    "comments": [],
    "likes": [],
    "plays": [],
    "reach": [],
    "saved": [],
    "shares": [],
    "total_interactions": []
}
def set_data_reelvideo(endpoint, media_id, sleep_sec = 0):
    params = {
        "access_token" : config_ini['DEFAULT']['ACCESS_TOKEN'],
        "metric": "comments,likes,plays,reach,saved,shares,total_interactions"
    }
    res = requests.get(endpoint, params = params)
    time.sleep(sleep_sec)
    if(res.status_code == 200):
        data = res.json()
        metrics = data["data"]
        reels["media_id"] += [media_id]
        reels["comments"] += [metrics[0]["values"][0]["value"]]
        reels["likes"] += [metrics[1]["values"][0]["value"]]
        reels["plays"] += [metrics[2]["values"][0]["value"]]
        reels["reach"] += [metrics[3]["values"][0]["value"]]
        reels["saved"] += [metrics[4]["values"][0]["value"]]
        reels["shares"] += [metrics[5]["values"][0]["value"]]
        reels["total_interactions"] += [metrics[6]["values"][0]["value"]]
        return False
    elif(res.status_code == 400):
        logger.info(f"After the media id {media_id} are before get business acount: {res.text}")
        print(f"After the media id {media_id} are before get business acount: {res.text}")
        return True
    else:
        raise Exception(f"Error at set_data_reels status code: {res.status_code}. {res.text}")

albums = {
    "media_id": [],
    "engagement": [],
    "impressions": [],
    "reach": [],
    "saved": [],
    "video_views": [],
}
def set_data_albums(endpoint, media_id, sleep_sec = 0):
    params = {
        "access_token" : config_ini['DEFAULT']['ACCESS_TOKEN'],
        "metric": "total_interactions,impressions,reach,saved,video_views"
    }
    res = requests.get(endpoint, params = params)
    time.sleep(sleep_sec)
    if(res.status_code == 200):
        data = res.json()
        metrics = data["data"]
        albums["media_id"] += [media_id]
        albums["engagement"] += [metrics[1]["values"][0]["value"]]
        albums["impressions"] += [metrics[2]["values"][0]["value"]]
        albums["reach"] += [metrics[3]["values"][0]["value"]]
        albums["saved"] += [metrics[4]["values"][0]["value"]]
        albums["video_views"] += [metrics[0]["values"][0]["value"]]
        return False
    elif(res.status_code == 400):
        logger.info(f"After the media id {media_id} are before get business acount: {res.text}")
        print(f"After the media id {media_id} are before get business acount: {res.text}")
        return True
    else:
        raise Exception(f"Error at set_data_albums status code: {res.status_code}. {res.text}")

def get_impressions(df_media, sleep_sec = 0):
    cnt = 0
    is_media_before_get_bussiness_acount = False
    for media_id, media_type, media_product_type in zip(df_media["id"], df_media["media_type"], df_media["media_product_type"]):
        endpoint = BASE_URL.format(api_version = config_ini['DEFAULT']['API_VERSION'], id = media_id) + "/insights"
        if(media_type == "VIDEO" and media_product_type == "REELS"):
            is_media_before_get_bussiness_acount = set_data_reelvideo(endpoint, media_id, sleep_sec)
        elif(media_type == "CAROUSEL_ALBUM" or media_type == "IMAGE" or (media_type == "VIDEO" and media_product_type == "FEED")):
            is_media_before_get_bussiness_acount = set_data_albums(endpoint, media_id, sleep_sec)
        else:
            print(f"{media_id} {media_type} {media_product_type}: no target media type.")
            logger.info(f"{media_id} {media_type} {media_product_type}: no target media type.")

        if is_media_before_get_bussiness_acount is True: break
        cnt += 1

        if(cnt > 10): break # debug

    df_reels = pd.DataFrame(data = reels)
    df_albums = pd.DataFrame(data = albums)
    print(f"Successfully set data at get_impressions.")
    logger.info(f"Successfully set data at get_impressions.")
    return df_reels, df_albums

def main():
    config_path = args.configfile_path
    csv_path = args.csv_path
    csv_prefix_name = args.csv_prefix_name
    csv_suffix_name = args.csv_suffix_name
    sleep_sec = float(args.sleep_sec)

    get_me().to_csv(f"{csv_path}/{csv_prefix_name}me{csv_suffix_name}.csv", index=False)

    df_media = get_media(sleep_sec)
    df_media.to_csv(f"{csv_path}/{csv_prefix_name}media{csv_suffix_name}.csv", index=False)

    get_comments(df_media, sleep_sec).to_csv(f"{csv_path}/{csv_prefix_name}comment{csv_suffix_name}.csv", index=False)

    df_reels, df_albums = get_impressions(df_media, sleep_sec)
    df_reels.to_csv(f"{csv_path}/{csv_prefix_name}reels{csv_suffix_name}.csv", index=False)
    df_albums.to_csv(f"{csv_path}/{csv_prefix_name}albums{csv_suffix_name}.csv", index=False)

if __name__ == "__main__":
    main()