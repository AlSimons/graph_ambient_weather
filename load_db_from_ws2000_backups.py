#! /usr/bin/env python3

"""
This is a "docstring". It describes the file at a high level. The Python
infrastructure has doctools, which can read a source file and generate
documentation, class relationship diagrams, etc. from it.  In this way, you
you don't have to (try to) maintain code and documentation separately (which
will invariably become out of sync).

This program loads a database with all the data from a WS2000 backup file.
Initially using sqlite3; may change to MariaDB if the data get large enough.
"""

# Python uses a multitude of libraries to do common tasks.  We use this set.
# All of these are in the base Python distribution; for this program, no
# additional libraries (of which there are zillions) need to be downloaded
# and installed.

# Parsing CSV files is hard.  This library takes care of all the corner cases
# for us. Here we just use the CSV file reader.
from csv import reader

# All sorts of processing for dates and times.
from datetime import datetime

# Globbing is the practice of applying filename wildcards, and returning all
# the matching files / directories.
from glob import glob

# Misc operating system specific operations, such as splitting a file path.
import os

# sqlite3 provides an SQL relational database contained in a single file.
import sqlite3

# Some global (to this file) variables
database = None
db_conn = None


def init_queries(db):
    """
    For sqlite3.  A parallel routine will be needed if we add Mariadb support.
    Create the db if needed, and create a connection to it.
    :param db: The name of the database
    :return: None
    """

    # Mark these names as referring to the global variables. A python routine
    # can READ from a global variable, but if not marked global, writing to a
    # variable will create a new variable in the routine with the same name.
    # Here we want to write to the global variables, so we have to name them
    # in a global statement.
    global database, db_conn
    database = db
    if not database.endswith('.db'):
        database += '.db'
    db_conn = sqlite3.connect(database)


def get_cursor():
    """
    Simple wrapper function to give a friendly name.
    :return: A cursor to the database.
    """
    return db_conn.cursor()


def create_table():
    """
    Create the single table used by the program.
    :return: None.
    :side effect: the database is updated.
    """
    # I'm not going to try to explain relational databases and the SQL language
    # here. There are many excellent tutorials online, including some that
    # focus on using sqlite3.
    c = get_cursor()
    c.execute("DROP TABLE IF EXISTS wx_data")
    c.execute("""CREATE TABLE wx_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_time REAL KEY UNIQUE,
        indoor_temp REAL,
        indoor_humidity REAL,
        outdoor_temp REAL,
        outdoor_humidity REAL,
        dew_point REAL,
        feels_like REAL,
        wind REAL,
        gust REAL,
        wind_dir REAL,
        abs_pressure REAL,
        rel_pressure REAL,
        solar_radiation REAL,
        uv_index REAL,
        hourly_rain REAL,
        event_rain REAL,
        daily_rain REAL,
        weekly_rain REAL,
        monthly_rain REAL,
        yearly_rain REAL)""")

    # Write the table definition out to the database file.
    db_conn.commit()


def get_reader(filepath):
    """
    Returns a CSV reader for a given filepath.
    :param filepath: The full path to the file containing our source data.
    :return: A CSV reader.
    """
    f = open(filepath)
    return reader(f)


def find_latest_data_file():
    """
    Searches in the specified (below) directory tree for the latest file.
    This will need to be changed to be used on another system to navigate
    to the top of the directory tree containing the weather data backups.
    :return: The full path to the desired file.
    """
    # Use the sample backup file from the current directory.
    directory = '.'

    # To use live data, this would probably be a more complex system-specific
    # path. For instance, on the system this was developed on, this is the
    # path used.
    directory = r'c:\Users\simons\Documents\weather_station_data\backups_from_ws2000\*\\'

    #
    # The directory wildcarded in the directory glob pattern is the date,
    # helpfully in the yyyymmdd format so we can use simple lexical ordering
    # to find the most recent (up-to-date) file.
    # If I were making this generally available, this would have to be a
    # command line parameter.

    # The filename pattern used for WS-2000 backups.
    filename_pattern = 'Backup-*.CSV'
    file_list = glob(os.path.join(directory, filename_pattern))
    #
    # Glob doesn't return the filenames in any particular order, so we have
    # to sort them.
    desired_file = sorted(file_list)[-1]
    print(desired_file)
    return desired_file


def add_row(row):
    """
    Adds a row from the source file to the database.  Called once per row of
    data.
    :param row: A single row from the source file.
    :return: None
    :side effect: The database is updated.
    """
    # The cursor is the magic handle for operating on the database.
    c = get_cursor()
    row[0] = format_time(row[0])
    for i in range(1, len(row)):
        row[i] = float(row[i])
    # The fields named below are in the order they appear as columns in
    # the backup.
    #
    # All the "?" in this statement allow for parameter sanitizing,
    # preventing a miscreant from damaging the database. It isn't really
    # needed here, because we are taking data from a known, trusted source (our
    # weather station backup), but it doesn't matter.  ALWAYS, ALWAYS do this.
    # See https://xkcd.com/327/, and the explanation at
    # https://www.explainxkcd.com/wiki/index.php/Little_Bobby_Tables
    # for more detail. (Everyone who ever touches a database should read
    # the above links!)
    c.execute("""
        INSERT INTO wx_data (
            date_time,
            indoor_temp,
            indoor_humidity,
            outdoor_temp,
            outdoor_humidity,
            dew_point,
            feels_like,
            wind,
            gust,
            wind_dir,
            abs_pressure,
            rel_pressure,
            solar_radiation,
            uv_index,
            hourly_rain,
            event_rain,
            daily_rain,
            weekly_rain,
            monthly_rain,
            yearly_rain
        )VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?)
    """, row)


def process_file():
    """
    Find the latest file, get a CSV reader for it, and process each row in
    turn.
    :return: None
    """
    path = find_latest_data_file()
    r = get_reader(path)
    # The first row of the file is column headers, not data. Skip over it
    # using next() on the reader.
    next(r)
    # Now process all the rows in the file.
    for row in r:
        add_row(row)
    # Nothing is written to the database until the changes are committed.
    db_conn.commit()


def format_time(time_string):
    """
    Converts the backups date-time format into the format required by sqlite3.
    :param time_string: The date time in the backup's format
    :return: The date time in the database's required format.
    """
    dt = datetime.strptime(time_string, '%Y/%m/%d %H:%M')
    return datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')


def main():
    """
    Drives the three phases of the program. Create and initialize the database,
    create our table in it, and process the backup file.
    :return: None
    """
    init_queries('ws2000')
    create_table()
    process_file()


if __name__ == '__main__':
    main()
