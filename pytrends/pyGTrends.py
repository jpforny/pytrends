from __future__ import absolute_import, print_function, unicode_literals

import copy
import csv
from datetime import datetime
from io import open
import re
import sys
import requests
import json
import os
import logging
from fake_useragent import UserAgent
if sys.version_info[0] == 2:  # Python 2
    from cookielib import CookieJar
    from cStringIO import StringIO
    from urllib import urlencode
    from urllib import quote
    from urllib2 import build_opener, HTTPCookieProcessor
else:  # Python 3
    from http.cookiejar import CookieJar
    from io import StringIO
    from urllib.parse import urlencode
    from urllib.parse import quote
    from urllib.request import build_opener, HTTPCookieProcessor

# Logging
logger = logging.getLogger(os.path.basename(sys.argv[0]))
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class pyGTrends(object):
    """
    Google Trends API
    """
    def __init__(self, username, password):
        """
        Initialize hard-coded URLs, HTTP headers, and login parameters
        needed to connect to Google Trends, then connect.
        """
        self.login_params = {
            'continue': 'http://www.google.com/trends',
            'PersistentCookie': 'yes',
            'Email': username,
            'Passwd': password}
        # provide fake user agent to look like a desktop browser
        self.fake_ua = UserAgent()
        self.headers = [
            ('Referrer', 'https://www.google.com/accounts/ServiceLoginBoxAuth'),
            ('Content-type', 'application/x-www-form-urlencoded'),
            ('User-Agent', self.fake_ua.chrome),
            ('Accept', 'text/plain')]
        self.url_ServiceLoginBoxAuth = 'https://accounts.google.com/ServiceLoginBoxAuth'
        self.url_Export = 'http://www.google.com/trends/trendsReport'
        self.url_CookieCheck = 'https://www.google.com/accounts/CheckCookie?chtml=LoginDoneHtml'
        self.url_PrefCookie = 'http://www.google.com'
        self._connect()

    def _connect(self):
        """
        Connect to Google Trends. Use cookies.
        """
        self.cj = CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(self.cj))
        self.opener.addheaders = self.headers

        resp = self.opener.open(self.url_ServiceLoginBoxAuth).read()
        resp = re.sub(r'\s\s+', ' ', resp.decode(encoding='utf-8'))

        galx = re.compile('<input name="GALX"[\s]+type="hidden"[\s]+value="(?P<galx>[a-zA-Z0-9_-]+)">')
        m = galx.search(resp)
        if not m:
            galx = re.compile('<input type="hidden"[\s]+name="GALX"[\s]+value="(?P<galx>[a-zA-Z0-9_-]+)">')
            m = galx.search(resp)
            if not m:
                raise Exception('Cannot parse GALX out of login page')

        self.login_params['GALX'] = m.group('galx')
        params = urlencode(self.login_params).encode('utf-8')
        # self.opener.open(self.url_ServiceLoginBoxAuth, params)
        # HTTP 400 Bad Request since 23/08/2016 (CookieCheck)
        # self.opener.open(self.url_CookieCheck)
        # self.opener.open(self.url_PrefCookie)

    def request_report(self, keywords, hl='en-US', cat=None, geo=None, date=None, tz=None, gprop=None):
        query_param = 'q=' + quote(keywords)

        # This logic handles the default of skipping parameters
        # Parameters that are set to '' will not filter the data requested.
        # See Readme.md for more information
        if cat is not None:
            cat_param = '&cat=' + cat
        else:
            cat_param = ''
        if date is not None:
            date_param = '&date=' + quote(date)
        else:
            date_param = ''
        if geo is not None:
            geo_param = '&geo=' + geo
        else:
            geo_param = ''
        if tz is not None:
            tz_param = '&tz=' + tz
        else:
            tz_param = ''
        if gprop is not None:
            gprop_param = '&gprop=' + gprop
        else:
            gprop_param = ''
        hl_param = '&hl=' + hl

        # These are the default parameters and shouldn't be changed.
        cmpt_param = "&cmpt=q"
        content_param = "&content=1"
        export_param = "&export=1"

        combined_params = query_param + cat_param + date_param + geo_param + hl_param + tz_param + cmpt_param \
                          + content_param + export_param + gprop_param

        print("Now downloading information for:")
        print("http://www.google.com/trends/trendsReport?" + combined_params)

        raw_data = self.opener.open("http://www.google.com/trends/trendsReport?" + combined_params).read()
        self.decode_data = raw_data.decode('utf-8')

        if self.decode_data in ["You must be signed in to export data from Google Trends"]:
            print("You must be signed in to export data from Google Trends")
            raise Exception(self.decode_data)

    def save_csv(self, path, trend_name):
        fileName = path + trend_name + ".csv"
        with open(fileName, mode='wb') as f:
            f.write(self.decode_data.encode('utf8'))

    def get_data(self):
        return self.decode_data

    def get_suggestions(self, keyword):
        kw_param = quote(keyword)
        raw_data = self.opener.open("https://www.google.com/trends/api/autocomplete/" + kw_param).read()
        # response is invalid json but if you strip off ")]}'," from the front it is then valid
        json_data = json.loads(raw_data[5:].decode())
        return json_data
        
    def get_trending_stories(self, hl='en-US', tz=None, cat=None, fi=None, fs=None, geo=None, ri=None, rs=None, sort=None):
        """
        Example URL:
        https://www.google.com/trends/api/stories/latest?hl=pt-BR&tz=180&cat=b&fi=9&fs=9&geo=BR&ri=150&rs=10&sort=0
        """
        
        if tz is not None:
            tz_param = '&tz=' + tz
        else:
            tz_param = ''
        if cat is not None:
            cat_param = '&cat=' + cat
        else:
            cat_param = ''
        if fi is not None:
            fi_param = '&fi=' + fi
        else:
            fi_param = ''
        if fs is not None:
            fs_param = '&fs=' + fs
        else:
            fs_param = ''
        if geo is not None:
            geo_param = '&geo=' + geo
        else:
            geo_param = ''
        if ri is not None:
            ri_param = 'ri=' + ri
        else:
            ri_param = ''
        if rs is not None:
            rs_param = '&rs=' + rs
        else:
            rs_param = ''
        if sort is not None:
            sort_param = '&sort=' + sort
        else:
            sort_param = ''
        hl_param = '&hl=' + hl
        
        # Limit of stories
        ri_param = "&ri=200"
        # Unknown (As big as possible)
        rs_param = "&rs=200"
        # Default parameters
        tz_param = "&tz=180"
        fi_param = "&fi=9"
        fs_param = "&fs=9"
        sort_param = "&sort=0"

        combined_params = hl_param + tz_param + cat_param + fi_param + fs_param + geo_param + ri_param + rs_param + sort_param
        url = "https://www.google.com/trends/api/stories/latest?" + combined_params
        
        logger.info("Requesting latest trending stories {}".format(url))
        raw_data = self.opener.open(url).read()
        latest_trends_json = json.loads(raw_data[5:].decode())
        logger.info("Latest trending stories successfully fetched")
        
        return latest_trends_json
        
    def get_story_summary(self, hl=None, tz=None, ids=None):
        """
        Example URL:
        https://www.google.com/trends/api/stories/summary?hl=pt-BR&tz=180&id=BR_lnk_-QxwwAAwAACJfM_pt&id=BR_lnk_xZVxwAAwAAC05M_pt&id=BR_lnk_rJt3wAAwAADb6M_pt
        """
        
        if tz is not None:
            tz_param = '&tz=' + tz
        else:
            tz_param = "&tz=180"
        hl_param = '&hl=' + hl

        combined_params = hl_param + tz_param + ids
        url = "https://www.google.com/trends/api/stories/summary?" + combined_params
        
        logger.info("Requesting story summaries from {}".format(url))
        raw_data = self.opener.open(url).read()
        json_data = json.loads(raw_data[5:].decode())
        return json_data
        
    def get_story(self, id=None, hl=None, tz=None, sw=None):
        """
        Example URL:
        https://www.google.com/trends/api/stories/BR_lnk_om7cwAAwAAB-HM_pt?hl=en-US&tz=180&sw=10
        
        """
        
        if id is not None:
            id_param = id
        else:
             id = ''
        if tz is not None:
            tz_param = '&tz=' + tz
        else:
            tz_param = "&tz=180"
        if sw is not None:
            sw_param = '&sw=' + sw
        else:
            sw_param = ''
        hl_param = '&hl=' + hl

        combined_params = id_param + hl_param + tz_param + sw_param
        url = "https://www.google.com/trends/api/stories/" + combined_params
        
        logger.info("Requesting story {} information from {}".format(id_param, url))
        raw_data = self.opener.open(url).read()
        json_data = json.loads(raw_data[5:].decode())
        return json_data
            
    def get_story_timeline(self, hl=None, tz=None, req=None, token=None):
        """
        Example URL:
        https://www.google.com/trends/api/widgetdata/timeline?hl=en-US&tz=180&req=%7B%22geo%22:%7B%22country%22:%22BR%22%7D,%22time%22:%222016-08-14T00%5C%5C:00%5C%5C:00+2016-08-16T01%5C%5C:40%5C%5C:00%22,%22resolution%22:%22HOUR%22,%22mid%22:%5B%22%2Fm%2F01b571%22%5D,%22locale%22:%22en-US%22%7D&token=APP6_UEAAAAAV7PDC0OSkUeHhRfsfOEeEnLzr3v9GdWr&tz=180                
        FIXME: It has a duplicate tz parameter at the end 
        """

        if tz is not None:
            tz_param = '&tz=' + tz
        else:
            tz_param = "&tz=180"
        if req is not None:
            req_param = '&req=' + req
        else:
             req_param = ''
        if token is not None:
            token_param = '&token=' + token
        else:
            token_param = ''
        hl_param = '&hl=' + hl
        
        combined_params = hl_param + tz_param + req_param + token_param
        url = "https://www.google.com/trends/api/widgetdata/timeline?" + combined_params
    
        logger.info("Requesting story timeline from {}".format(url))
        raw_data = self.opener.open(url).read()
        json_data = json.loads(raw_data[5:].decode())
        return json_data
        
    def get_story_related_queries(self, hl=None, tz=None, req=None, token=None):
        """
        Example URL:
        https://www.google.com/trends/api/widgetdata/relatedqueries?token=APP6_UEAAAAAV7PDC5R1lquTAi8HM0EX_zFwR7h9bhC9&hl=en-US&tz=180
        
        TODO: Pass request payload
        """

        if tz is not None:
            tz_param = '&tz=' + tz
        else:
            tz_param = "&tz=180"
        if req is not None:
            req_param = '&req=' + req
        else:
             req_param = ''
        if token is not None:
            token_param = '&token=' + token
        else:
            token_param = ''
        hl_param = '&hl=' + hl
        
        combined_params = hl_param + tz_param + req_param + token_param
        url = "https://www.google.com/trends/api/widgetdata/relatedqueries?" + combined_params

        logger.info("Requesting story timeline from {}".format(url))
        raw_data = self.opener.open(url).read()
        json_data = json.loads(raw_data[5:].decode())
        return json_data
        
def parse_data(data):
    """
    Parse data in a Google Trends CSV export (as `str`) into JSON format
    with str values coerced into appropriate Python-native objects.

    Parameters
    ----------
    data : str
        CSV data as text, output by `pyGTrends.get_data()`

    Returns
    -------
    parsed_data : dict of lists
        contents of `data` parsed into JSON form with appropriate Python types;
        sub-tables split into separate dict items, keys are sub-table "names",
        and data values parsed according to type, e.g.
        '10' => 10, '10%' => 10, '2015-08-06' => `datetime.datetime(2015, 8, 6, 0, 0)`
    """
    parsed_data = {}
    for i, chunk in enumerate(re.split(r'\n{2,}', data)):
        if i == 0:
            match = re.search(r'^(.*?) interest: (.*)\n(.*?); (.*?)$', chunk)
            if match:
                source, query, geo, period = match.groups()
                parsed_data['info'] = {'source': source, 'query': query,
                                       'geo': geo, 'period': period}
        else:
            chunk = _clean_subtable(chunk)
            rows = [row for row in csv.reader(StringIO(chunk)) if row]
            if not rows:
                continue
            label, parsed_rows = _parse_rows(rows)
            if label in parsed_data:
                parsed_data[label+'_1'] = parsed_data.pop(label)
                parsed_data[label+'_2'] = parsed_rows
            else:
                parsed_data[label] = parsed_rows

    return parsed_data


def _clean_subtable(chunk):
    """
    The data output by Google Trends is human-friendly, not machine-friendly;
    this function fixes a couple egregious data problems.
    1. Google replaces rising search percentages with "Breakout" if the increase
    is greater than 5000%: https://support.google.com/trends/answer/4355000 .
    For parsing's sake, we set it equal to that high threshold value.
    2. Rising search percentages between 1000 and 5000 have a comma separating
    the thousands, which is terrible for CSV data. We strip it out.
    """
    chunk = re.sub(r',Breakout', ',5000%', chunk)
    chunk = re.sub(r'(,[+-]?[1-4]),(\d{3}%\n)', r'\1\2', chunk)
    return chunk


def _infer_dtype(val):
    """
    Using regex, infer a limited number of dtypes for string `val`
    (only dtypes expected to be found in a Google Trends CSV export).
    """
    if re.match(r'\d{4}-\d{2}(?:-\d{2})?', val):
        return 'date'
    elif re.match(r'[+-]?\d+$', val):
        return 'int'
    elif re.match(r'[+-]?\d+%$', val):
        return 'pct'
    elif re.match(r'[a-zA-Z ]+', val):
        return 'text'
    else:
        msg = "val={0} dtype not recognized".format(val)
        raise ValueError(msg)


def _convert_val(val, dtype):
    """
    Convert string `val` into Python-native object according to its `dtype`:
    '10' => 10, '10%' => 10, '2015-08-06' => `datetime.datetime(2015, 8, 6, 0, 0)`,
    ' ' => None, 'foo' => 'foo'
    """
    if not val.strip():
        return None
    elif dtype == 'date':
        match = re.match(r'(\d{4}-\d{2}-\d{2})', val)
        if match:
            return datetime.strptime(match.group(), '%Y-%m-%d')
        else:
            return datetime.strptime(re.match(r'(\d{4}-\d{2})', val).group(), '%Y-%m')
    elif dtype == 'int':
        return int(val)
    elif dtype == 'pct':
        return int(val[:-1])
    else:
        return val


def _parse_rows(rows, header='infer'):
    """
    Parse sub-table `rows` into JSON form and convert str values into appropriate
    Python types; if `header` == `infer`, will attempt to infer if header row
    in rows, otherwise pass True/False.
    """
    if not rows:
        raise ValueError('rows={0} is invalid'.format(rows))
    rows = copy.copy(rows)
    label = rows[0][0].replace(' ', '_').lower()

    if header == 'infer':
        if len(rows) >= 3:
            if _infer_dtype(rows[1][-1]) != _infer_dtype(rows[2][-1]):
                header = True
            else:
                header = False
        else:
            header = False
    if header is True:
        colnames = rows[1]
        data_idx = 2
    else:
        colnames = None
        data_idx = 1

    data_dtypes = [_infer_dtype(val) for val in rows[data_idx]]
    if any(dd == 'pct' for dd in data_dtypes):
        label += '_pct'

    parsed_rows = []
    for row in rows[data_idx:]:
        vals = [_convert_val(val, dtype) for val, dtype in zip(row, data_dtypes)]
        if colnames:
            parsed_rows.append({colname:val for colname, val in zip(colnames, vals)})
        else:
            parsed_rows.append(vals)

    return label, parsed_rows
