# -*- coding: utf-8 -*-
"""
Last Updated: Jun 11, 2018

Author: Gokce Kahvecioglu
         Northwestern University
"""

import requests
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
from datetime import datetime, timedelta
import pytz
from numpy import nanmean
import numpy as np
import matplotlib.pyplot as plt


def request_url(start_date, end_date, market_info, node='ALL'):
    """
    market:
            DAM (day-ahead market): 1-hr resolution
            RTM_5: real-time market with 5-min resolution
            RTM_15: real-time market with 15-min resolution
    """

    (market, res) = market_info
    base_url = 'http://oasis.caiso.com/oasisapi/SingleZip?'
    if market == 'DAM':
        query_name = 'PRC_LMP'
    else:
        if res == '5':
            query_name = 'PRC_INTVL_LMP'  # 5-min RTM
        else:
            query_name = 'PRC_RTPD_LMP'  # 15-min RTM
    url = base_url + 'resultformat=6&' + 'queryname=' + query_name + '&' \
          'version=1' + '&' \
          'startdatetime=' + start_date + '&' \
          'enddatetime=' + end_date + '&' \
          'market_run_id=' + market + '&' \
          'node=' + node_id

    r = requests.get(url, allow_redirects=True)

    f = ZipFile(BytesIO(r.content))
    match = [s for s in f.namelist() if ".csv" in s][0]
    df = pd.read_csv(f.open(match), low_memory=False)
    return df


def get_year(data):
    return data.year


def get_posit(data, position):
    return data[position]


def convert_to_utc(dateobject, localtz):
    local = pytz.timezone(localtz)
    local_dt = local.localize(dateobject, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt


def cast_date_time(string_datetime):
    time_obj = datetime.strptime(string_datetime, "%Y-%m-%dT%H:%M:%S-00:00")
    return time_obj


def utc_to_time(naive, timezone="Europe/Istanbul"):
    return naive.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(timezone))


def getCAISO_lmp(start_date, end_date, node_id='ALL', market='DAM',
                 tz='US/Pacific'):
    """
    California ISO Interface for retrieving
    locational marginal price (lmp) data over
    a specified period.

    For more details, visit
    http://www.caiso.com/Documents/OASIS-InterfaceSpecification_v5_0_0Redline_Fall2017Release.pdf

    Input:

    tz: time zone
    start_date (yyyymm)
    end_date
    market: Day-Ahead-Market (DAM), or X
    node: ALL

    Output:

    """
    # operating start/end datetime in GMT
    start_time = datetime.strptime(start_date, '%Y%m%d')
    # start_time_utc = convert_to_utc(start_time, tz)

    # to obtain last day 23:00 add one more day
    end_time = datetime.strptime(end_date, '%Y%m%d') + timedelta(days=1)

    df = pd.DataFrame()

    # CAISO allows to retrieve 31 days at once
    # loop to access 310 days at a time
    curr_time = start_time
    ii = 0
    while curr_time + timedelta(days=10) <= end_time:
        next_curr_time = curr_time + timedelta(days=10)

        # Time Format: T07:00-0000
        startdatetime = curr_time.strftime('%Y%m%d') + 'T00:00-0000'
        enddatetime = next_curr_time.strftime('%Y%m%d') + 'T00:00-0000'

        new_df = request_url(startdatetime, enddatetime, market, node_id)
        df = pd.concat([df, new_df])
        curr_time = next_curr_time  # update the time
        ii += 1
        print(ii)

    startdatetime = curr_time.strftime('%Y%m%d') + 'T00:00-0000'
    enddatetime = end_date + 'T23:00-0000'
    new_df = request_url(startdatetime, enddatetime, market, node_id)
    df = pd.concat([df, new_df])
    return df

### inputs ###
node_id = 'CONTADNA_1_N001'
startdate = '20160101'#'20170101'  # local time
enddate = '20170101' #'20180101'   # local time
year = '2016'
##### end-inputs #####

# RTM: 5
# RTPD: 15
# DAM: 60 mins (market_details = ('DAM', ''))
#==============================================================================

market_details = ('RTM', '5')
# market_details = ('RTPD', '15')

df = getCAISO_lmp(startdate, enddate, node_id, market_details, 'US/Pacific')
df = df[df['LMP_TYPE'] == 'LMP']
df.to_csv('CONTADNA_1_N001_LMP_' + market_details[0] + market_details[1] +
          '_' + startdate + '_' + enddate + '.csv')

# # read in the data and manipulate it
fname = 'CONTADNA_1_N001_LMP_' + market_details[0] + market_details[1] + \
       '_' + startdate + '_' + enddate + '.csv'

caiso_df = pd.read_csv(fname)
caiso_df['starttime_gmt'] = caiso_df['INTERVALSTARTTIME_GMT'].apply(cast_date_time).values
caiso_df['endtime_gmt'] = caiso_df['INTERVALENDTIME_GMT'].apply(cast_date_time).values

# convert time to US/Pacific zone
caiso_df['starttime_local'] = caiso_df['starttime_gmt'].apply(utc_to_time, timezone='US/Pacific')
caiso_df['endtime_local'] = caiso_df['endtime_gmt'].apply(utc_to_time, timezone='US/Pacific')

# sort the rows wrt starttime_local
caiso_df = caiso_df.sort_values(by=["starttime_local"])


# Aggregate for given resolution by grouping index
res = '60Min'
caiso_df = caiso_df.set_index('starttime_local')
grouped_caiso = caiso_df.groupby(pd.TimeGrouper(res)).agg(nanmean)

grouped_caiso = grouped_caiso.reset_index()
yy_col = grouped_caiso['starttime_local'].apply(get_year)

indices = np.where(yy_col == int(year))
grouped_caiso = grouped_caiso.iloc[indices]

grouped_caiso.to_csv('CONTADNA_1_N001_LMP_' + market_details[0] +
                     market_details[1] + '_' + year + 'aggregated.csv', 
                     index=False)
