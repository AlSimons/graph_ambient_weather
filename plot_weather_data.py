#! /usr/bin/env python3

"""
Plot selected weather data from the WS2000 backup, as loaded into the database.
"""
import argparse
import datetime
import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import sqlite3


def parse_args(data_types):
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data-type', default='otemp',
                        help="Which data you wish to plot. If specified, must "
                             "be one or two of {}, separated by a comma. "
                             "Default='otemp'".format(
                                 list(data_types.keys())))
    parser.add_argument('-s', '--start-date',
                        help="Date of earliest desired data. "
                             "Must be in the format YYYY-MM-DD."
                             "Default: earliest available")
    parser.add_argument('-e', '--end-date',
                        help="Date of last desired data. "
                             "Must be in the format YYYY-MM-DD."
                             "Default: latest available")
    parser.add_argument('-n', '--num-points', type=int, default=500,
                        help="Number of points to plot. Default=500")
    args = parser.parse_args()
    data_type_list = args.data_type.split(',')
    if len(data_type_list) > 2:
        parser.error("Only two data types may be specified.")
    for dt in data_type_list:
        if dt not in data_types.keys():
            parser.error(f"'{dt}' is not a known data type.\n"
                         f"must be one of: {list(data_types.keys())}")
    return args


database = None
db_conn = None


def init_queries(db):
    """
    For sqlite3.  Parallel routine needed if we add Mariadb support.
    Create the db if needed, and create a connection to it.
    :param db: The name of the database
    :return: None
    """
    global database, db_conn
    database = db
    if not database.endswith('.db'):
        database = database + '.db'
    db_conn = sqlite3.connect(database)


def get_cursor():
    return db_conn.cursor()


def get_data(data_types, all_data_types, start_date, end_date, num_points):
    dts = []
    for dt in data_types:
        dts.append(all_data_types[dt][0])
    columns = ', '.join(dts)
    c = get_cursor()
    params = []
    select_count = 'COUNT(*)'
    select_data = f'date_time, {columns}'
    query = 'SELECT {} FROM wx_data'
    if start_date is not None:
        params.append(start_date + ' 00:00')
        query += ' WHERE date_time >= ?'
    if end_date is not None:
        params.append(end_date + ' 23:59')
        if start_date is None:
            # No WHERE out yet.
            query += ' WHERE'
        else:
            query += ' AND'
        query += ' date_time <= ?'

    # Figure out the spacing to obtain just N evenly spaced records.
    # At this point, the query is getting the number of all records in the
    # requested time
    num_rows = c.execute(query.format(select_count), params).fetchone()[0]

    num_points = min(num_points, num_rows)
    modulus = int(num_rows / num_points)
    params.append(modulus)
    if start_date is None and end_date is None:
        query += ' WHERE'
    else:
        query += ' AND'
    query += ' ROWID % ? = 0'
    c.execute(query.format(select_data), params)
    result = c.fetchall()
    return result


def find_right_date_interval(start, end):
    # What are we doing here?
    # We want as many date ticks as we can get without overwriting. It looks
    # like for our current chart size, the right max number of ticks is
    # somewhere between 70 and 90; I don't yet have enough data to determine it.
    # So we take our entire date range and divide it by 90, to get the
    # number of days between each tick.
    num_days = end - start
    interval = math.ceil(num_days.days / 90)
    if interval == 0:
        interval = 1
    return interval


def plot_it(date_and_data, data_types, start, end, num_points, all_data_types):
    start = "Earliest" if start is None else start
    end = "Latest" if end is None else end

    # Extract the dates from our database return, and turn them into
    # datetime.datetimes.
    dates = [datetime.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S')
             for x in date_and_data]
    num_points = min(num_points, len(dates))

    dts = []
    # Turn our command line column short-names into user-friendly names.
    for dt in data_types:
        dts.append(all_data_types[dt][1])

    plt.figure(figsize=(19, 9), num="{} {} to {} ({} points)".
               format(", ".join(dts), start, end, num_points))
    plt.subplots_adjust(hspace=.5, wspace=.16,
                        top=.97, bottom=.11,
                        left=.06, right=.97)

    for n in range(len(dts)):
        data = [x[1+n] for x in date_and_data]

        # This lets us plot a single column on the full window, but split
        # if 2 requested.
        if len(dts) > 1:
            # split the plot space in half
            plt.subplot(211 + n)

        plt.title("{} {} to {} ({} points)".format(dts[n], start, end,
                                                   num_points))
        plt.xticks(rotation=90)
        # Set x-axis major ticks to weekly interval, on Mondays
        plt.gca().xaxis.set_major_locator(
            mdates.DayLocator(interval=find_right_date_interval(dates[0],
                                                                dates[-1])))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d-%Y'))
        plt.plot(dates, data)
        plt.grid(axis='y')
    plt.show()


def main():
    all_data_types = {
        'apres': ['abs_pressure', "Absolute pressure"],
        'dewpt': ['dew_point', "Dew point"],
        'feels': ['feels_like', "Feels like"],
        'gust': ['gust', "Gust"],
        'itemp': ['indoor_temp', "Indoor temperature"],
        'ihumid': ['indoor_humidity', "Indoor humidity"],
        'otemp': ['outdoor_temp', "Outdoor temperature"],
        'ohumid': ['outdoor_humidity', "Outdoor humidity"],
        'rpres': ['rel_pressure', "Relative pressure"],
        'solrad': ['solar_radiation', "Solar radiation"],
        'uvidx': ['uv_index', "UV index"],
        'wind': ['wind', "Wind"],
        'wdir': ['wind_dir', "Wind direction"],
        'drain': ['daily_rain', "Daily rain"],
        'erain': ['event_rain', "Event rain"],
        'hrain': ['hourly_rain', "Hourly rain"],
        'mrain': ['monthly_rain', "Monthly rain"],
        'yrain': ['yearly_rain', "Yearly rain"],
    }
    args = parse_args(all_data_types)
    init_queries('ws2000')
    data_types = [x.strip() for x in args.data_type.split(',')]
    start = args.start_date
    end = args.end_date
    data = get_data(data_types, all_data_types,
                    start, end, args.num_points)
    plot_it(data, data_types, start, end, args.num_points, all_data_types)


if __name__ == '__main__':
    main()
