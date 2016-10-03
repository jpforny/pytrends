import time
import os
import sys
import logging
import schedule
import ntplib
import datetime
import traceback

import pandas as pd

from random import randint
from time import ctime, strftime
from pytrends.pyGTrends import pyGTrends
from pandas.io.json import json_normalize


# Logging
logger = logging.getLogger(os.path.basename(sys.argv[0]))
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

ntpclient = ntplib.NTPClient()
    
def get_trends(connector):
      
    cat_param = 'b'
    geo_param = 'BR'
    
    # request_timestamp = pd.Timestamp(datetime.datetime.fromtimestamp(ntpclient.request('br.pool.ntp.org').tx_time))
    request_timestamp = pd.Timestamp(ctime())
    
    data = connector.get_trending_stories(hl=hl_param, cat=cat_param, geo=geo_param)
    
    stories = data['storySummaries']['trendingStories']
    
    result = json_normalize(stories)
    result['timestamp'] = request_timestamp
    
    return result

def get_day_trends():
    
    start = time.time()
    
    # Google Trends client
    logger.info("Connecting to Google")
    connector = pyGTrends('', '')
    logger.info("Connected to Google")
    
    df = pd.DataFrame()
    
    counter = 0
    
    kill_at = pd.Timestamp(strftime("%Y-%m-%d 21:00:00"))
    
    spent_time_connection = time.time() - start - (start % 1)
    
    try:
        while True:

            start = time.time()
            
            now = pd.Timestamp(ctime())
            if now > kill_at:
                logger.info("Killing job...")
                break
                
            current_trends = get_trends(connector)
            df = df.append(current_trends)
            
            counter += current_trends.shape[0]
            
            # Print a status message
            if counter % 1000 == 0:
                logger.info("{} stories fetched".format(counter))
            
            spent_time = time.time() - start 
            
            
            logger.info("Current job took {}\n".format(spent_time))
            
            if spent_time_connection > 0:
                time.sleep(60 - spent_time - spent_time_connection)
                spent_time_connection = 0
            else:
                time.sleep(60 - spent_time - (start % 1))
    except:
        logger.error("Exiting: " + str(sys.exc_info()[0]))
        logger.error(traceback.format_exc())

    beginning = str(df.iloc[0]['timestamp'])
    end = str(df.iloc[df.shape[0] - 1]['timestamp'])
    df.index.name = 'rank'
    df.to_csv("trends[" + beginning + ", " + end + "].csv")
    
    logger.info("Saved day trends from {} to {}".format(beginning, end))

def main(start_at):
    schedule.every().day.at(start_at).do(get_day_trends)
    while True:
        schedule.run_pending()
        time.sleep(0.0001)
        
if __name__ == u'__main__':
    logger.info("Running %s" % ' '.join(sys.argv))
   
    global hl_param
    hl_param = sys.argv[1] if len(sys.argv) > 1 else 'pt-BR'

    start_at = sys.argv[2] if len(sys.argv) > 2 else '05:00'
    
    main(start_at)
    
