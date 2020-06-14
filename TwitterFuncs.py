import time
import tweepy
import json
import random
from datetime import datetime
from datetime import date
from constant import *
from DBConnection import *

from threading import Lock

consumerKey = "aLpplbXfL8Jj31Pqqrddli650"
consumerSecret = "iZeCavAPafJQnsSFbLJpBRX08mSNol8gbIWSxqHvb9K1FjMoDm"
accessToken = "972946316560093184-L95Kl3IVUDOTasH1xu81cCWEIZQ9Br0"
accessTokenKey = "uLS5moBlBZRJyHIunHYd0BCJdrN2GYziQbPgikmvBtuze"

api = None

match_words = ""
account_age = 259200
status_count = 1000
my_id = 972946316560093184
#my_id = 198505587
following_ratio = 5

focus_time = 86400
loop_time = 900
limit_time = 86400 * 7

mutex = Lock()

# ****************************************************************
# ============== get twitter api object =================
# ************************************************************** #
def get_api_object():
    global api
    global my_id

    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenKey)
    api = tweepy.API(auth)

    my_id = api.me().id

    return api

# ****************************************************************
# ===================== set criteria ========================
# ************************************************************** #
def set_basic_data(config):
    global match_words
    global account_age
    global status_count
    global following_ratio
    global focus_time
    global loop_time
    global limit_time

    if "match_word" in config:
        match_words = config["match_word"]
    if "account_age" in config:
        account_age = config["account_age"]
    if "status_count" in config:
        status_count = config["status_count"]
    if "following_ratio" in config:
        following_ratio = config["following_ratio"]
    if "focus_time" in config:
        focus_time = config["focus_time"]
    if "loop_time" in config:
        loop_time = config["loop_time"]
    if "limit_time" in config:
        limit_time = config["limit_time"]

    return 0


# ****************************************************************
# ===================== run query ========================
# ************************************************************** #
def run_query(query, mode = 1):
    mutex.acquire()

    dbconn = MySQLConnection()

    res = dbconn.connect(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE)
    if res != 0:
        mutex.release()
        return -1

    # ===== mode = 1 -> insert, update ===== #
    if mode == 1:
        dbconn.execute(query)
        dbconn.disconnect()
        mutex.release()

        return 0

    # ===== mode = 2 -> select ===== #
    elif mode == 2:
        record = dbconn.select(query)
        dbconn.disconnect()
        mutex.release()
        return record

# ****************************************************************
# ===================== main thread ========================
# ************************************************************** #
def main_thread(thread_id, tweet_id, tweet_text):
    #user_obj = api.get_user("198505587")

    if tweet_text.lower().find(match_words.lower()) == -1: return

    print("======== Candidate Tweet has been found =============")
    print(tweet_text)
    print ("*****************************************************")

    # ======= start main thread function ========== #
    start_time = int(time.time())
    print (str(datetime.now()) + " : Thread - " + str(thread_id) + " started for Tweet - " + str(tweet_id))

    while (1) :
        try:
            cur_time = int(time.time())
            # ========== if over THREAD_LIFETIME, kill thread ========== #
            if cur_time - start_time >= focus_time:
                break

            # ========== get retweeters =========== #
            retweeters = api.retweets(tweet_id, 100)
            if len(retweeters) == 0:
                time.sleep(loop_time)
                continue

            user_id = retweeters[0].user.id
            user_obj = api.get_user(user_id)

            # ========== check if user is in block list ========== #
            query = "SELECT * FROM dashboard_tbl_focus_users WHERE user_id = " + str(user_obj.id) + " AND flag=4"
            record = run_query(query, 2)
            if len(record) != 0:
                time.sleep(loop_time)
                continue

            # ========== check if this user is following me or i am following him =========== #
            friendship = api.show_friendship(source_id = my_id, target_id = user_obj.id)
            if (friendship[0]._json["followed_by"] == True) or (friendship[1]._json["followed_by"] == True):
                time.sleep(loop_time)
                continue

            # ========== check user's following ratio =========== #
            #if following_ratio * user_obj.friends_count < user_obj.followers_count:
            #    time.sleep(loop_time)
            #    continue

            flag = 1

            # ========== check if user meets criteria =========== #
            # === 1. account age === #
            if (datetime.now() - user_obj.created_at).seconds < account_age:
                flag = 0
            # === 2. status count, favourite count === #
            if user_obj.statuses_count < status_count or user_obj.favourites_count < status_count:
                flag = 0

            # === save user to db === #
            id_ary = ["id", "name", "screen_name", "location", "protected", "followers_count", "friends_count", "listed_count", "created_at", "favourites_count",
                      "utc_offset", "time_zone", "geo_enabled", "verified", "statuses_count", "lang", "description"]

            user_dict = {}
            for id in id_ary:
                if id in user_obj._json:
                    user_dict[id] = user_obj._json[id]

            user_dict["favourite_count"] = user_obj.favourites_count

            user_str = json.dumps(user_dict).replace("'", "\\'")
            query = "Insert INTO dashboard_tbl_focus_users (user_id, user, match_word, flag, time, followed_time) VALUE (" + str(user_obj.id) + ",'" + user_str + "', '" + match_words + "'," + str(flag) + ", '" + str(datetime.now()) + "', 0)"
            print ("***********************************************************************")
            print ("***************************** Run Query ****************************** ")
            print (query)
            print("************************************************************************")
            print("************************************************************************")
            run_query(query)

            time.sleep(loop_time)

        except Exception as e:
            sleep_time = random.randint(10, 180)
            print (str(e) + " ----- will try after " + str(sleep_time) + " seconds")
            time.sleep(sleep_time)

    print(str(datetime.now()) + " : Thread - " + str(thread_id) + " for Tweet - " + str(tweet_id) + " is killed")


# ****************************************************************
# ===================== sub thread ========================
# ************************************************************** #
def sub_thread():
    print ("=========== Sub Thread Started ============= ")
    while (1):
        try:
            print("=========== Sub Thread started working ============= ")
            cur_time = int(time.time())
            th = cur_time - limit_time
            query = "SELECT * FROM dashboard_tbl_focus_users WHERE flag=2 AND followed_time >= " + str(th)
            following_users = run_query(query, 2)

            # ===== check if friendship exists ===== #
            for user in following_users:
                #print ("************" + str(user[0]) + "*****************")
                #print ("************" + str(user[1]) + "*****************")
                #print ("************" + str(user[2]) + "*****************")
                #print ("************" + str(user[3]) + "*****************")
                #print ("************" + str(user[4]) + "*****************")
                #print ("************" + str(user[5]) + "*****************")
                #print (my_id)
                friendship = api.show_friendship(source_id=my_id, target_id=user[3])
                if (friendship[0]._json["followed_by"]==True): print("A")
                if (friendship[1]._json["followed_by"]==True):print("B")
                if (friendship[0]._json["followed_by"] == True) and (friendship[1]._json["followed_by"] == True):
                    query = "UPDATE dashboard_tbl_focus_users SET flag = 3 WHERE user_id = " + str(user[3])
                    print ("************** run query ***************" + query)
                    run_query(query)

                    cur_date = str(date.today())
                    query = "SELECT * FROM dashboard_tbl_follow_statistics WHERE date = '" + cur_date + "' AND mode = 3"
                    record = run_query(query, 2)
                    if len(record == 0):
                        query = "INSERT INTO dashboard_tbl_follow_statistics (mode, count, date) VALUE (3, 1, '" + cur_date + "')"
                        run_query(query)
                    else:
                        record = record[0]
                        cnt = record[2] + 1
                        query = "UPDATE dashboard_tbl_follow_statistics SET count = " + str(cnt) + " WHERE date = '" + cur_date + "' AND mode = 3"
                        run_query(query)

            print("=========== Sub Thread finished working, will continue after 1 minute ============= ")
            time.sleep(30 * MINUTE)

        except Exception as e:
            print ("Error occured in sub thread - " + str(e))
            print ("Sub thread will be continued after 5 minue")
            time.sleep(5 * MINUTE)

    print("=========== Sub Thread Ended ============= ")





