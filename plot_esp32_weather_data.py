#! /usr/bin/env python3

"""
Plot temp and humidity from the ESP32 DHT-22. Data are in a sqlite3 DB.
"""
import argparse
import datetime
import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import sqlite3


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start-date',
                        help="Date of earliest desired data. "
                             "Must be in the format YYYY-MM-DD."
                             "Default: earliest available")
    parser.add_argument('-e', '--end-date',
                        help="Date of last desired data. "
                             "Must be in the format YYYY-MM-DD."
                             "Default: latest available")
    parser.add_argument('-n', '--num-points', type=int, default=1000,
                        help="Number of points to plot. Default=1000")
    args = parser.parse_args()
    return args


db_conn = None


def init_queries(db):
    """
    For sqlite3.  Parallel routine needed if we add Mariadb support.
    Create the db if needed, and create a connection to it.
    :param db: The name of the database
    :return: None
    """
    global db_conn
    database = db
    if not database.endswith('.db'):
        database = database + '.db'
    db_conn = sqlite3.connect(database)


def get_cursor():
    return db_conn.cursor()


def get_data( start_date, end_date, num_points):
    columns = 'temp, humidity'
    c = get_cursor()
    params = []
    select_count = 'COUNT(*)'
    select_data = f'date_time, {columns}'
    query = 'SELECT {} FROM readings'  # Eventually has a .format() applied
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
    interval = math.ceil(num_days.days / 128)
    if interval == 0:
        interval = 1
    return interval


def plot_it(date_and_data, num_points):
    # Extract the dates from our database return, and turn them into
    # datetime.datetimes.
    dates = [datetime.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S')
             for x in date_and_data]
    start = datetime.datetime.strftime(dates[0], '%Y-%m-%d')
    end = datetime.datetime.strftime(dates[-1], '%Y-%m-%d')
    num_days = (dates[-1] - dates[0]).days
    num_points = min(num_points, len(dates))

    plt.figure(figsize=(19, 9), num="{} to {} ({} days, {} points)".
               format(start, end, num_days, num_points))
    plt.subplots_adjust(hspace=.5, wspace=.16,
                        top=.97, bottom=.11,
                        left=.06, right=.97)

    dts = ["temp", "humidity"]
    for n in range(len(dts)):
        data = [x[1+n] for x in date_and_data]
        # This lets us plot a single column on the full window, but split
        # if 2 requested.
        plt.subplot(211 + n)

        plt.title("{} {} to {} ({} days, {} points)".format(
            dts[n], start, end, num_days, num_points))
        plt.xticks(rotation=90)
        # Set x-axis major ticks to weekly interval, on Mondays
        plt.gca().xaxis.set_major_locator(
            mdates.DayLocator(interval=find_right_date_interval(dates[0],
                                                                dates[-1])))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d-%Y'))
        plt.plot(dates, data)
        plt.grid(axis='y')
        axes = plt.gca()
        axes.set_xlim(left=dates[0], right=dates[-1])
    plt.show()


def main():
    args = parse_args()
    init_queries('DHT_data.db')
    start = args.start_date
    end = args.end_date
    data = get_data(start, end, args.num_points)
    plot_it(data, args.num_points)


if __name__ == '__main__':
    main()
