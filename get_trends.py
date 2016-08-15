import time
import os
import sys
import logging

import pandas as pd

from random import randint
from time import ctime
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

if __name__ == u'__main__':
    google_username = "an_email@gmail.com"
    google_password = "password"

    logger.info("Connecting to Google as {}".format(google_username))
    connector = pyGTrends(google_username, google_password)
    beggining = str(pd.Timestamp(ctime()))
  
    df = pd.DataFrame()
    counter = 0

    try:
        while True:
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
