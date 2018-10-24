# Written by Tony Layton
# Oct 21, 2018
#
import sqlite3
import os
import time
from pprint import pprint
from collections import defaultdict
from wedge_helper import *


# Folder Pointers
read_from_folder = 'test_data_txt/'  # TEST  Read From here
write_to_folder = 'test_data_base/'  # TEST  Write To here
#read_from_folder = 'data_txt/'  # Read From here
#write_to_folder = 'data_base/'  # Write To here


time_dict = defaultdict(list)  # Records the time the input file was opened.
read_db_file = 'Master_WedgeDB.db'
write_db_file = 'Anthony_Layton_WedgeDB.db'


def file_processing_time(the_dict_to_save=time_dict):
    # Purpose:
    #   Saves the time a file is opened as well as closed.
    # Variables:
    #   time_dict
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    # Is used by the following functions to run:
    #   -na-
    file_head = ["Query","Main Event","Sub Event","Date Stamp", "Time Stamp"]
    date_time_stamp = (time.strftime("%Y%m%d %H%M", (time.localtime())))
    name_to_save_report_as = ("Run Reports/{} DB run Query processing TIME.csv".format(date_time_stamp))
    holding_list = []
    for key in sorted(the_dict_to_save.keys()):
        holding_list += [key]
        file_content = the_dict_to_save[key]
        with open(name_to_save_report_as, 'a+', newline='') as csvfile:
            the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            the_row.writerow(file_head)
            the_row.writerows(file_content)
            csvfile.close()
    time.sleep(1)
    print("\tSAVE Successful!\t\tFILE:\t{}".format(name_to_save_report_as))


def time_stamp(Query, Main_Event, Sub_Event, time_dict=time_dict):
    # Purpose:
    #   To add a time stamp to the 'time_dict'
    # Variables:
    #   file_name = name of file being processed
    #   event: is either 'open file' or 'close file'
    #   time_dict = is the dict being written to.
    # Uses the following functions to run:
    #   none
    # Is used by the following functions to run:
    #   process_files()
    date_time_stamp = time.localtime()
    date_stamp = (time.strftime("%Y-%m-%d", date_time_stamp))
    time_stamp = (time.strftime("%H:%M:%S", date_time_stamp))
    write_this = ("{},{},{},{},{}").format(Query, Main_Event, Sub_Event, date_stamp, time_stamp )
    write_this = write_this.split(sep=",")
    time_dict['time'].append(write_this)
    print(write_this)


# Build Query One DB Table
def init_query_one_db_table(cur):
    time_stamp('Query One', 'BUILD DB Structure', 'START')
    cur.execute('''DROP TABLE IF EXISTS Sales_by_Date_by_Hour''')
    cur.execute('''CREATE TABLE Sales_by_Date_by_Hour (
        date TIMESTAMP,
        hour TIMESTAMP,
        spend REAL,
        trans REAL,
        items REAL)''')
    time_stamp('Query One', 'BUILD DB Structure', 'END')


# Populate Query One DB Table
def populate_query_one_db_table(db, file_handle, delimiter, limit=None):
    cur = db.cursor()
    next(file_handle, None)  # for this to work here, we always need header rows.
    # That's my standard, so it should be fine.
    time_stamp('Query One', 'Populate DB', 'START')
    for idx, row in enumerate(file_handle.readlines()):
        row = row.strip().replace('\"', '').split(delimiter)
        cur.execute('''
            INSERT INTO Sales_by_Date_by_Hour (date, hour, spend, trans, items)
            VALUES (?,?,?,?,?)''', row)
        if limit:
            if idx >= limit:
                break
    time_stamp('Query One', 'Populate DB', 'END')
    db.commit()


# Build Query Two DB Table
def init_query_two_db_table(cur):
    time_stamp('Query Two', 'BUILD DB Structure', 'START')
    cur.execute('''DROP TABLE IF EXISTS Sales_by_Owner_by_Year_by_Month''')
    cur.execute('''CREATE TABLE Sales_by_Owner_by_Year_by_Month (
        card_no INTEGER,
        year TIMESTAMP,
        month TIMESTAMP,
        spend REAL,
        trans REAL,
        items REAL)''')
    time_stamp('Query Two', 'BUILD DB Structure', 'END')


# Populate Query two DB Table
def populate_query_two_db_table(db, file_handle, delimiter, limit=None):
    cur = db.cursor()
    next(file_handle, None)  # for this to work here, we always need header rows.
    # That's my standard, so it should be fine.
    time_stamp('Query Two', 'Populate DB', 'START')
    for idx, row in enumerate(file_handle.readlines()):
        row = row.strip().replace('\"', '').split(delimiter)
        cur.execute('''
            INSERT INTO Sales_by_Owner_by_Year_by_Month (card_no, year, month, spend, trans, items)
            VALUES (?,?,?,?,?,?)''', row)
        if limit:
            if idx >= limit:
                break
    time_stamp('Query Two', 'Populate DB', 'END')
    db.commit()


# Build Query Two DB Table
def init_query_three_db_table(cur):
    time_stamp('Query Three', 'BUILD DB Structure', 'START')
    cur.execute('''DROP TABLE IF EXISTS Sales_by_Product_by_Year_by_Month''')
    cur.execute('''CREATE TABLE Sales_by_Product_by_Year_by_Month (
        upc INTEGER,
        description TEXT,
        dept_num INTEGER,
        dept_name TEXT,
        year TIMESTAMP,
        month TIMESTAMP,
        spend REAL,
        trans REAL,
        items REAL)''')
    time_stamp('Query Three', 'BUILD DB Structure', 'END')


# Populate Query two DB Table
def populate_query_three_db_table(db, file_handle, delimiter, limit=None):
    cur = db.cursor()
    next(file_handle, None)  # for this to work here, we always need header rows.
    # That's my standard, so it should be fine.
    time_stamp('Query Three', 'Populate DB', 'START')
    for idx, row in enumerate(file_handle.readlines()):
        row = row.strip().replace('\"', '').split(delimiter)
        cur.execute('''
            INSERT INTO Sales_by_Product_by_Year_by_Month (upc, description, dept_num, 
            dept_name, year, month, spend, trans, items)
            VALUES (?,?,?,?,?,?,?,?,?)''', row)
        if limit:
            if idx >= limit:
                break
    time_stamp('Query Three', 'Populate DB', 'END')
    db.commit()


def query_one():
    # Sales by Date by Hour: By Calendar date and Hour of the day, the total spend
    # the number of transactions, and the count of the number of items.
    # db = sqlite3.connect(':memory:')
    time_stamp_query = 'Query One'
    time_stamp(time_stamp_query, 'DB Read Connection', 'START')
    db = sqlite3.connect(write_to_folder + read_db_file)
    cur = db.cursor()
    cur.fetchall()
    output_one = cur.execute("""SELECT date(datetime) AS date,
    substr(time(datetime),-8,2) as hour,
    sum(total) AS spend,
    count(distinct(date(datetime) || register_no || emp_no || trans_no)) AS trans,
    sum(CASE WHEN (trans_status = 'V' or trans_status = 'R') THEN -1 ELSE 1 END) AS items
    FROM transactions
    WHERE department != 0 and
    department != 15 and 
    trans_status != 'M' and
    trans_status != 'C' and
    trans_status != 'J' and
    (trans_status = '' or 
    trans_status = ' ' or 
    trans_status = 'V' or 
    trans_status = 'R')
    GROUP BY date, hour
    ORDER BY date, hour""")
    # Put Query in Dict
    time_stamp(time_stamp_query, 'Migrate Query to Dict', 'START')
    date_hour = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))  # Spend : Transactions : Items
    for row in output_one:
        date, hour, spend, trans, items = row
        spend = round(spend, 2)
        date_hour[date][hour]['spend'] += spend
        date_hour[date][hour]['trans'] += trans
        date_hour[date][hour]['items'] += items
    time_stamp(time_stamp_query, 'Migrate Query to Dict', 'END')
    time_stamp(time_stamp_query, 'DB Read Connection', 'END')
    db.close()
    # Write Query One to Dict to Txt
    time_stamp(time_stamp_query, 'Migrate Dict to DB Table', 'START')
    time_stamp(time_stamp_query, 'TXT File Read Connection', 'START')
    output_file_name = "Sales_by_Date_by_Hour.txt"
    if output_file_name in os.listdir(write_to_folder):
        os.remove(write_to_folder + output_file_name)
    header = ["Date", "Hour", "Spend", "Transactions", "Items"]
    with open(write_to_folder + output_file_name, 'a+') as o1file:
        o1file.write("\t".join(header) + "\n")
        for date in date_hour:
            for hour in date_hour[date]:
                row = [date, hour,
                       date_hour[date][hour]['spend'],
                       date_hour[date][hour]['trans'],
                       date_hour[date][hour]['items']]
                o1file.write("\t".join([str(item) for item in row]) + "\n")
        o1file.close()
        time_stamp(time_stamp_query, 'TXT File Read Connection', 'END')
    # Write Query One TXT to DB File
    time_stamp(time_stamp_query, 'DB Write Connection', 'START')
    db = sqlite3.connect(write_to_folder + write_db_file)
    cur = db.cursor()
    init_query_one_db_table(cur)
    with open(write_to_folder + output_file_name, 'r') as w1file:
        populate_query_one_db_table(db, w1file, delimiter="\t", limit=None)
    #os.remove(write_to_folder + output_file_name)
    w1file.close()
    time_stamp(time_stamp_query, 'DB Write Connection', 'END')
    time_stamp(time_stamp_query, 'Migrate Dict to DB Table', 'END')
    db.close()
    print("\n\n*** QUERY ONE DONE ***\n\n")


def query_two():
    # Sales by Owner by Year by Month:
    #db = sqlite3.connect(':memory:')
    time_stamp_query = 'Query Two'
    time_stamp(time_stamp_query, 'DB Read Connection', 'START')
    db = sqlite3.connect(write_to_folder + read_db_file)
    cur = db.cursor()
    cur.fetchall()
    output_two = cur.execute("""SELECT card_no,
    substr((datetime),1,4) AS year,
    substr(date(datetime),6,2) AS month,
    sum(total) AS spend,
    count(distinct(date(datetime) || register_no || emp_no || trans_no)) as trans,
    sum(CASE WHEN (trans_status = 'V' or trans_status = 'R') THEN -1 ELSE 1 END) as items
    FROM transactions
    WHERE department != 0 and
    department != 15 and 
    trans_status != 'M' and
    trans_status != 'C' and
    trans_status != 'J' and
    (trans_status = '' or 
    trans_status = ' ' or 
    trans_status = 'V' or 
    trans_status = 'R')
    GROUP BY card_no, year, month
    ORDER BY card_no, year, month""")
    # Put Query Two in Dict
    time_stamp(time_stamp_query, 'Migrate Query to Dict', 'START')
    owner_spend = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float))))
                        # {card_no: {year: {month: Spend, Transactions, Items}}}
    for row in output_two:
        card_no, year, month, spend, trans, items = row
        spend = round(spend, 2)
        owner_spend[card_no][year][month]['spend'] += spend
        owner_spend[card_no][year][month]['trans'] += trans
        owner_spend[card_no][year][month]['items'] += items
    time_stamp(time_stamp_query, 'Migrate Query to Dict', 'END')
    time_stamp(time_stamp_query, 'DB Read Connection', 'END')
    db.close()
    # Write Query Two to Dict to Txt
    time_stamp(time_stamp_query, 'Migrate Dict to DB Table', 'START')
    time_stamp(time_stamp_query, 'TXT File Read Connection', 'START')
    output_file_name = "Sales_by_Owner_by_Year_by_Month.txt"
    if output_file_name in os.listdir(write_to_folder):
        os.remove(write_to_folder + output_file_name)
    header = ["Card_No", "Year", "Month", "Spend", "Transactions", "Items"]
    with open(write_to_folder + output_file_name, 'a+') as o2file:
        o2file.write("\t".join(header) + "\n")
        for card_no in owner_spend:
            for year in owner_spend[card_no]:
                for month in owner_spend[card_no][year]:
                    row = [card_no, year, month,
                           owner_spend[card_no][year][month]['spend'],
                           owner_spend[card_no][year][month]['trans'],
                           owner_spend[card_no][year][month]['items']]
                    o2file.write("\t".join([str(item) for item in row]) + "\n")
        o2file.close()
        time_stamp(time_stamp_query, 'TXT File Read Connection', 'END')
    # Write Query Two TXT to DB File
    time_stamp(time_stamp_query, 'DB Write Connection', 'START')
    db = sqlite3.connect(write_to_folder + write_db_file)
    cur = db.cursor()
    init_query_two_db_table(cur)
    with open(write_to_folder + output_file_name, 'r') as w2file:
        populate_query_two_db_table(db, w2file, delimiter="\t", limit=None)
    w2file.close()
    # os.remove(write_to_folder + output_file_name)
    time_stamp(time_stamp_query, 'DB Write Connection', 'END')
    time_stamp(time_stamp_query, 'Migrate Dict to DB Table', 'END')
    db.close()
    print("\n\n*** QUERY TWO DONE ***\n\n")


def query_three():
    # Sales by Product Description by Year by Month.
    #db = sqlite3.connect(':memory:')
    time_stamp_query = 'Query three'
    time_stamp(time_stamp_query, 'DB Read Connection', 'START')
    db = sqlite3.connect(write_to_folder + read_db_file)
    cur = db.cursor()
    cur.fetchall()
    output_three = cur.execute("""SELECT transactions.upc,
    transactions.description AS description, 
    transactions.department AS dept_num,
    Departments.department_name AS dept_name,
    substr((datetime),1,4) AS year,
    substr(date(transactions.datetime),6,2) AS month,
    sum(total) AS spend,
    count(distinct(date(datetime) || register_no || emp_no || trans_no)) as trans,
    sum(CASE WHEN (transactions.trans_status = 'V' or transactions.trans_status = 'R') THEN -1 ELSE 1 END) as items
    FROM transactions
    INNER JOIN Departments ON transactions.department=Departments.department
    WHERE transactions.department != 0 and
    transactions.department != 15 and 
    transactions.trans_status != 'M' and
    transactions.trans_status != 'C' and
    transactions.trans_status != 'J' and
    (transactions.trans_status = '' or 
    transactions.trans_status = ' ' or 
    transactions.trans_status = 'V' or 
    transactions.trans_status = 'R')
    GROUP BY description, year, month
    ORDER BY description, year, month""")
    # Put Query Two in Dict
    time_stamp(time_stamp_query, 'Migrate Query to Dict', 'START')
    prod_sales = defaultdict(lambda:
                             defaultdict(lambda:
                                         defaultdict(lambda:
                                                     defaultdict(lambda:
                                                                 defaultdict(lambda:
                                                                             defaultdict(lambda: defaultdict(float)))))))
    # {upc: {description: {dept_num: {dept_name: {year: {month: spend, trans, items}}}}}}.
    # [upc][description][dept_num][dept_name][year][month] spend, trans, items
    for row in output_three:
        upc, description, dept_num, dept_name, year, month, spend, trans, items = row
        spend = round(spend, 2)
        prod_sales[upc][description][dept_num][dept_name][year][month]['spend'] += spend
        prod_sales[upc][description][dept_num][dept_name][year][month]['trans'] += trans
        prod_sales[upc][description][dept_num][dept_name][year][month]['items'] += items
    time_stamp(time_stamp_query, 'Migrate Query to Dict', 'END')
    time_stamp(time_stamp_query, 'DB Read Connection', 'END')
    db.close()
    # Write Query Two to Dict to Txt
    time_stamp(time_stamp_query, 'Migrate Dict to DB Table', 'START')
    time_stamp(time_stamp_query, 'TXT File Read Connection', 'START')
    output_file_name = "Sales_by_Product_by_Year_by_Month.txt"
    if output_file_name in os.listdir(write_to_folder):
        os.remove(write_to_folder + output_file_name)
    header = ["Card_No", "Year", "Month", "Spend", "Transactions", "Items"]
    with open(write_to_folder + output_file_name, 'a+') as o3file:
        o3file.write("\t".join(header) + "\n")
        for upc in prod_sales:
            for description in prod_sales[upc]:
                for dept_num in prod_sales[upc][description]:
                    for dept_name in prod_sales[upc][description][dept_num]:
                        for year in prod_sales[upc][description][dept_num][dept_name]:
                            for month in prod_sales[upc][description][dept_num][dept_name][year]:
                                row = [upc, description, dept_num, dept_name, year, month,
                                       prod_sales[upc][description][dept_num][dept_name][year][month]['spend'],
                                       prod_sales[upc][description][dept_num][dept_name][year][month]['trans'],
                                       prod_sales[upc][description][dept_num][dept_name][year][month]['items']]
                                o3file.write("\t".join([str(item) for item in row]) + "\n")
        o3file.close()
        time_stamp(time_stamp_query, 'TXT File Read Connection', 'END')
    # Write Query 'Two' - TXT to DB File
    time_stamp(time_stamp_query, 'DB Write Connection', 'START')
    db = sqlite3.connect(write_to_folder + write_db_file)
    cur = db.cursor()
    init_query_three_db_table(cur)
    with open(write_to_folder + output_file_name, 'r') as w3file:
        populate_query_three_db_table(db, w3file, delimiter="\t", limit=None)
    w3file.close()
    time_stamp(time_stamp_query, 'DB Write Connection', 'END')
    time_stamp(time_stamp_query, 'Migrate Dict to DB Table', 'END')
    db.close()
    print("\n\n*** QUERY THREE DONE ***\n\n")


query_one()
query_two()
query_three()
file_processing_time()