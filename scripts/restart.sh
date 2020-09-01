#!/bin/bash
pkill -9 geo_tweets_harvester.py
/usr/local/bin/python3.6 /data/geotweets_cga/scripts/geo_tweets_harvester.py &
