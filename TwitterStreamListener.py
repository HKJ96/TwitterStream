import threading
import json
import tweepy
import random

from TwitterFuncs import *

class TwitterSteamListener(tweepy.StreamListener):
    def on_status(self, status):
        if status.lang == "en":
            th_id = random.randint(1000000000000, 9999999999999)
            th = threading.Thread(target=main_thread,
                                  args=(th_id, status.id, status.text))
            th.start()

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False