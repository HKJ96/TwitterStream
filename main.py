import os
import tweepy

from pathlib import Path

from constant import *

from TwitterFuncs import *
from TwitterStreamListener import *
from DBConnection import *

# ****************************************************************
# ============== load basic data =================
# ************************************************************** #
def load_basic_data():
    # ========== get criteria information ============ #
    dbconn = MySQLConnection()
    res = dbconn.connect(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE)
    if res != 0: return res
    criteria = dbconn.select("select * from dashboard_tbl_criteria")
    if criteria == -1:
        print("Error while getting criteria information")
        return -1
    match_words = criteria[0][1]
    account_age = criteria[0][2]

    dbconn.disconnect()

    # ========== set basic data ============ #
    set_basic_data(match_words, account_age, 0, 198505587, 5)
    return {
        "match_words"   : match_words,
        "account_age"   : account_age,
    }

# ****************************************************************
# ============== main function =================
# ************************************************************** #
def main(match_words):
    try:
        # ========== get twitter api object ============ #
        api = get_api_object()

        # ========== start twitter streaming ============ #
        streamListner = TwitterSteamListener()
        streamListner = tweepy.Stream(auth=api.auth, listener=TwitterSteamListener())
        streamListner.filter(track=[match_words])

    except Exception as e:
        print (str(e))
        return -1

    return 0


if __name__ == "__main__":
    #basic_data = load_basic_data()

    cur_path = os.path.abspath(os.path.dirname(__file__))
    cur_path = str(Path(cur_path).parent)
    cur_path = os.path.join(cur_path, "config.json")

    fp = open(cur_path, "r")
    config = json.loads(fp.read())

    # ========== set basic data ============ #
    set_basic_data(config)

    # ========= start sub thread ========== #
    th = threading.Thread(target=sub_thread)
    th.start()

    while (1):
        main(config["match_word"])
        print ("********** Process will try to attempt again after 1 minue **********")
        time.sleep(MINUTE)