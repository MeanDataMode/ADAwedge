# Written by Tony Layton
# Oct 21, 2018
#
# Builds the text files that hold the data that goes into the SQL tables.
#
import os  # https://docs.python.org/3/library/os.html
import time
from collections import defaultdict
from wedge_helper import *


time_dict = defaultdict(list)       # Records the time the input file was opened.


# Folder Pointers
#read_from_folder = 'test_data_csv/'  # TEST  Read From here
#write_to_folder = 'test_data_txt/'  # TEST  Write To here
read_from_folder = 'data_csv/'  # Read From here
write_to_folder = 'data_txt/'  # Write To here


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
    name_to_save_report_as = ("Run Reports/{} CSV to TXT processing TIME.csv".format(date_time_stamp))
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


def csv_to_txt():
    # Purpose:
    #   will import a csv file that is separated by ',' and
    #   output a .txt file that is separtated by '\t'
    # Uses the following functions to run:
    #   time_stamp()
    csv_file_folder = os.listdir(read_from_folder)
    for file_name in csv_file_folder:
        a = []
        for idx, item in enumerate(file_name):
            a += item
        txt_file_name = ["".join(a[:-4]) + ".txt"]
        # Takes the file name out of the 'list' format.
        for item in txt_file_name:
            txt_file_name_only = item
        # Remove Existing File (if one exists)
        txt_file_name = ("{}{}".format(write_to_folder, txt_file_name_only))
        if txt_file_name_only in os.listdir(write_to_folder):
            os.remove(txt_file_name)
            time.sleep(1.5)
        # Open Input File.
        with open(read_from_folder + file_name, mode='r') as csv_file:
            input_file = csv.reader(csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            time_stamp(file_name, 'Open File')
            csv_file.seek(0)
            # Output File Write.
            with open(txt_file_name, 'a+', newline='\n') as txt_file:
                txt_file.seek(0)
                for idx, line in enumerate(input_file):
                    text_line = "\t".join(line)
                    text_line = (text_line +'\n')
                    txt_file.write(text_line)
                a = file_name  # doing this to no run over chr limit per line in code.
                b = txt_file_name_only  # doing this to no run over chr limit per line in code.
                print("\tFile Reformatting complete!:\n\t\tInput File:\t\t{}\n\t\tOutput File:\t{}".format(a, b))
                txt_file.close()
            time_stamp(file_name, 'Close File')
            print("")
        csv_file.close()
    print("\n\n*** DONE ***\n\n")


csv_to_txt()
file_processing_time()