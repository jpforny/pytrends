import time
import os
import sys
import logging
import schedule

import pandas as pd

from random import randint
from time import ctime, strftime
from pytrends.pyGTrends import pyGTrends
from pandas.io.json import json_normalize

# Logging
logger = logging.getLogger(os.path.basename(sys.argv[0]))
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

def get_trends(connector):
      
    cat_param = 'b'
    geo_param = 'BR'
    hl_param = 'pt-BR'
    
    data = connector.get_trending_stories(hl=hl_param, cat=cat_param, geo=geo_param)
    
    stories = data['storySummaries']['trendingStories']
    
    result = json_normalize(stories)
    result['timestamp'] = pd.Timestamp(ctime())
    
    return result

def get_day_trends(google_username='', google_password=''):

    logger.info("Connecting to Google as {}".format(google_username))
    connector = pyGTrends(google_username, google_password)
    beggining = str(pd.Timestamp(ctime()))
  
    df = pd.DataFrame()
    counter = 0
    
    kill_at = pd.Timestamp(strftime("%Y-%m-%d 21:00:00"))
    try:
        while True:
            now = pd.Timestamp(ctime())
            if now > kill_at:
                break
                
            current_trends = get_trends(connector)
            df = df.append(current_trends)
            
            counter += current_trends.shape[0]
            
            # Print a status message
            if counter % 1000 == 0:
                logger.info("{} stories fetched".format(counter))
            
            time.sleep(randint(55, 60))
    except:
        logger.info("Exiting: " + str(sys.exc_info()[0]))

    beginning = str(df.iloc[0]['timestamp'])
    end = str(df.iloc[df.shape[0] - 1]['timestamp'])
    df.index.name = 'rank'
    df.to_csv("trends[" + beginning + ", " + end + "].csv")

if __name__ == u'__main__':
    logger.info("Running %s" % ' '.join(sys.argv))
    # user = sys.argv[1] if len(sys.argv) > 1 else "email@gmail.com"
    # password = sys.argv[2] if len(sys.argv) > 2 else "password"
    
    schedule.every().day.at("05:00").do(get_day_trends)
    while True:
        schedule.run_pending()
        time.sleep(1)
    
