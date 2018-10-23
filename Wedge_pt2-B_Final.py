# Written by Tony Layton
# Oct 21, 2018


import os
from wedge_helper import *
import time
from collections import defaultdict

time_dict = defaultdict(list)  # Records the time the input file was opened.
master_db_name = 'WedgeDB.db'
master_db_folder_file = 'data_base/WedgeDB.db'


# Folder Pointers
#read_from_folder = 'test_data_txt/'  # TEST  Read From here
#write_to_folder = 'test_data_base/'  # TEST  Write To here
read_from_folder = 'data_txt/'  # Read From here
write_to_folder = 'data_base/'  # Write To here


def file_processing_time(the_dict_to_save=time_dict):
    # Purpose:
    #   Saves the time a file is opened as well as closed.
    # Variables:
    #   time_dict
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    # Is used by the following functions to run:
    #   -na-
    file_head = ["File","Event","Date Stamp", "Time Stamp"]
    date_time_stamp = (time.strftime("%Y%m%d %H%M", (time.localtime())))
    name_to_save_report_as = ("Run Reports/{} TXT to DB processing TIME.csv".format(date_time_stamp))
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


def time_stamp(file_name, event, time_dict=time_dict):
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
    write_this = ("{},{},{},{}").format(file_name, event, date_stamp, time_stamp )
    write_this = write_this.split(sep=",")
    time_dict['time'].append(write_this)
    print(write_this)


# Build Data Base:
def build_db():
    txt_file_folder = os.listdir(read_from_folder)
    #db = sqlite3.connect(':memory:')
    db = sqlite3.connect(master_db_folder_file)
    cur = db.cursor()
    init_db(cur)  # From Wedge Helper. Is the function that starts the DB, and creates the "transaction" table.
    for file_name in txt_file_folder:
        time_stamp(file_name, 'Open File')
        with open(read_from_folder + file_name, 'r') as ifile:
            populate_db(db, ifile, delimiter="\t", limit=None)
            ifile.close()
            time_stamp(file_name, 'Close File')
    db.close()
    print("\n\n*** DONE ***\n\n")


build_db()
file_processing_time()