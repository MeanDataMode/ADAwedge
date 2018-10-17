import os  # https://docs.python.org/3/library/os.html
import io
import csv  # https://docs.python.org/3.4/library/csv.html
from zipfile import ZipFile  # https://docs.python.org/3.4/library/zipfile.html
from collections import defaultdict
import time


# Folder Pointers
file_r_folder = 'test_read_data/' # read from here
file_w_folder = 'test_write_data/' # write to here
# file_r_folder = 'data/'  # read from here
# file_w_folder = 'write_data/'  # write to here


header = ['datetime', 'register_no', 'emp_no', 'trans_no', 'upc',
          'description', 'trans_type', 'trans_subtype', 'trans_status', 'department',
          'quantity', 'Scale', 'cost', 'unitPrice', 'total',
          'regPrice', 'altPrice', 'tax', 'taxexempt', 'foodstamp',
          'wicable', 'discount', 'memDiscount', 'discountable', 'discounttype',
          'voided', 'percentDiscount', 'ItemQtty', 'volDiscType', 'volume',
          'VolSpecial', 'mixMatch', 'matched', 'memType', 'staff',
          'numflag', 'itemstatus', 'tenderstatus', 'charflag', 'varflag',
          'batchHeaderID', 'local', 'organic', 'display', 'receipt',
          'card_no', 'store', 'branch', 'match_id', 'trans_id']

holding_dict = defaultdict(list)  # Holds The Lines that have been parsed, but needing To Be Saved.
time_dict = defaultdict(list)  # Records the time the input file was opened.

written_to_dict = defaultdict(int)  # Counts times the saved files has been opened and saved to.
counter_dict = defaultdict(int)  # Counts times number of rows have been pared and designated to the saved file

errors_dict = defaultdict(list)  # Records the exceptions and errors that occur when parsing.

zip_file_folder = os.listdir(file_r_folder)
delimiters = dict()
for this_zip_file in zip_file_folder:
    with ZipFile(file_r_folder + this_zip_file, 'r') as the_zip_file:
        the_zipped_files = the_zip_file.namelist()
        for the_file_name in the_zipped_files:
            the_input_file = the_zip_file.open(the_file_name, 'r')
            the_input_file = io.TextIOWrapper(the_input_file, encoding="utf-8")
            dialect = csv.Sniffer().sniff(sample=the_input_file.readline(),
                                          delimiters=[",", ";", "\t"])
            delimiters[the_file_name] = dialect.delimiter
            the_input_file.close()  # tidy up


#def pack_dict(input_file):
#    for idx, line in enumerate(input_file):
#        line = (line.strip().split(this_delimiter))
#        line = remove_quotes(line)
#        transaction_dict = {}
#        for item in range(len(line)):
#            transaction_dict[key_head[item]] = line[item]
#        name = ("file name = {}  \nRow# = {}".format(file_name, idx))
#        print("""""")
#        print(name)
#        print(transaction_dict)
#        if idx > 1:
#            break



def clear_w_folder():
    # Purpose:
    #   Clean out the write to folder before we begin
    #
    # Variables:
    #   none
    #
    # Uses the following functions to run:
    #
    #
    # Is used by the following functions to run:
    #
    #
    write_folder = os.listdir(file_w_folder)
    for this_file in write_folder:
        this_file = ("{}{}".format(file_w_folder, this_file))
        os.remove(this_file)
    time.sleep(2)


def run_command_prompt():
    # Purpose:
    #   This is the user interface of this code.
    #   The follow functions are directed by the users responses:
    #       1) where the files will be read from;
    #       2) where the files will write to;
    #       3) DELETE contents of write to folder before writing to folder.
    #
    # Variables:
    #   none
    #
    # Uses the following functions to run:
    #
    #
    # Is used by the following functions to run:
    #
    #
    print("\n\n")
    print("Hello, and welcome!")
    write_folder_len = len(os.listdir(file_w_folder))
    if write_folder_len > 0:
        time.sleep(.25)
        print("Before we get started, I need to take care of a few housekeeping items.")
        time.sleep(.25)
        print("\n\t\tYou have {} files in your 'WRITE TO' folder\n\t\t\tFile Folder:  {}".format(write_folder_len,
                                                                                                  file_w_folder))
        time.sleep(.25)
        while True:
            try:
                to_clear = input("\nDo you want me to DELETE these files? (Y/N)   : ").lower()
                time.sleep(.25)
                if to_clear == 'y' or to_clear == 'yes':
                    while True:
                        confirm = input("CONFIRM you want to DELETE these files permanently? (Y/N)   : ").lower()
                        try:
                            if confirm == 'y' or confirm == 'yes':
                                for i in [3, 2, 1]:
                                    time.sleep(.5)
                                    print("\t\t\t\t{}".format(i))
                                clear_w_folder()
                                time.sleep(.5)
                                print("Your files have been successfully deleted.")
                                break
                            elif confirm == 'n' or confirm == 'no':
                                print("\n\n")
                                break
                            else:
                                print("Sorry, I didn't understand that.")
                                continue
                        except ValueError:
                            print("error")
                elif to_clear == 'n' or to_clear == 'no':
                    print("\n*** Starting Run ***\n\n\n\n\n")
                    break
                else:
                    print("Sorry, I didn't understand that.")
                    continue
            except ValueError:
                print("error")
            print("\nThank You.\n")
            print("\n*** Starting Run ***\n\n\n\n\n")
            time.sleep(2)
            break


def remove_quotes(lists):
    # Function that removes the double quotes
    #
    # Variables:
    #   lists = list of items to remove double quotes from.
    #
    # Uses the following functions to run:
    #   row_irregularity_checker() <also returns variable 'lists' as 'cleaned' to here>
    #
    # Is used by the following functions to run:
    #
    #
    cleaned = []
    for item in lists:
        if item[0:1] == '"' and item[-1:] == '"':
            ab = item[:0] + item[(0 + 1):]
            ab = ab[:-1]
            cleaned.append(ab)
        elif item[0:1] == "'" and item[-1:]:
            ba = item[:0] + item[(0 + 1):]
            ba = ba[:-1]
            cleaned.append(ba)
        else:
            cleaned.append(item)
    return cleaned


def irregular_line_fix(irregular_line):
    # Fixes error that happens when the 'description' has "," within.
    #
    # Variables:
    #   irregular_line = the line that has multiple "," within it.
    #
    # Uses the following functions to run:
    #   prepare_rows() <also returns variable 'irregular_line' to here>
    #
    # Is used by the following functions to run:
    #
    #
    line_length = len(irregular_line)
    y = line_length - 49 + 5
    description_fix = irregular_line[5:y]
    word = ",".join(description_fix)
    word = word.strip('"')
    irregular_line.insert(5, word)
    y = line_length - 49 + 6
    del irregular_line[6:y]
    return irregular_line


def save_this(data_matrix, in_holding_mod):
    # Purpose:
    #   Writes the rows to the saved file.
    #
    # Variables:
    #   data_matrix = the lines to write to file
    #   in_holding_mod = the file number to write to.
    #
    # Uses the following functions to run:
    #
    #
    # Is used by the following functions to run:
    #
    #
    check_first = os.listdir(file_w_folder)
    file_number = format(int(in_holding_mod), "04")
    this_file = ("WedgeFile_{}.csv".format(file_number))
    # save_time = time.localtime()
    # save_time = (time.strftime("%H:%M:%S", save_time))
    # print("\t\t\tSaving To:  {}\t\t\t\tTIME:\t{}".format(this_file, save_time))
    written_to_dict[this_file] += 1
    if this_file in check_first:
        with open(file_w_folder + this_file, 'a+', newline='') as csvfile:
            the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            the_row.writerows(data_matrix)
            csvfile.close()
    else:  # Will Add a header
        with open(file_w_folder + this_file, 'a+', newline='') as csvfile:
            the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            the_row.writerow(header)
            the_row.writerows(data_matrix)
            csvfile.close()


def hold_this(mod, line, write_when):
    # Purpose:
    #
    #
    # Variables:
    #
    #
    # Uses the following functions to run:
    #
    #
    # Is used by the following functions to run:
    #
    #
    if len(holding_dict[mod]) >= write_when:
        # writes the holding cell to file if >=1000
        holding_dict[mod].append(line)
        counter_dict[mod] += 1
        the_file = holding_dict[mod]
        save_this(the_file, mod)
        del holding_dict[mod]
    else:
        counter_dict[mod] += 1
        holding_dict[mod].append(line)  # puts the line in holding cell


def prepare_rows(idx, line, file_name, write_when):
    # Purpose:
    #
    #
    # Variables:
    #
    #
    # Uses the following functions to run:
    #
    #
    # Is used by the following functions to run:
    #
    #
    line_length = len(line)
    quarter = ''
    if line_length != 50:
        this_error = ("{},{},Irregular Line (Before Fix),{}".format(file_name, idx, line))
        this_error = this_error.split(sep=",")
        errors_dict['errors'].append(this_error)
        irregular_line_fix(line)
        this_error = ("{},{},Irregular Line (After Fix),{}".format(file_name, idx, line))
        this_error = this_error.split(sep=",")
        errors_dict['errors'].append(this_error)
    else:
        pass
    if line[45:46] != ['card_no'] and line[45:46] != ['3']:  # Will pass all items that are not card #3.
        cardNo = int(line[45])
        mod = cardNo % 171 + 1
        hold_this(mod, line, write_when)
    elif line[45:46] != ['card_no']:  # This code will designate what to do with 'card_no'== 3
        date = line[0]
        year = str(date[2:4])
        month = int(date[5:7])
        if 1 <= month <= 3:
            quarter = '01'
        elif 4 <= month <= 6:
            quarter = '02'
        elif 7 <= month <= 9:
            quarter = '03'
        elif 10 <= month <= 12:
            quarter = '04'
        else:
            pass
        mod = (year + quarter)
        mod = int(mod)
        hold_this(mod, line, write_when)


def purge_save_holding(holding_dict):
    # Purpose:
    #
    #
    # Variables:
    #
    #
    # Uses the following functions to run:
    #
    #
    # Is used by the following functions to run:
    #
    #
    last_holdings = []
    for key in holding_dict.keys():
        last_holdings += [key]
    for idx, mod in enumerate(last_holdings):
        the_file = holding_dict[mod]
        save_this(the_file, mod)
        del holding_dict[mod]


def row_irregularity_checker():
    # Purpose:
    #
    #
    # Variables:
    #   zip_files = are the files folder names that you would like to pass through this.
    #
    # Uses the following functions to run:
    #
    #
    # Is used by the following functions to run:
    #
    #
    zip_files = os.listdir('test_read_data/')
    for this_zf in zip_files:
        with ZipFile('test_read_data/' + this_zf, 'r') as zf:
            zipped_files = zf.namelist()
            for file_name in zipped_files:
                input_file = zf.open(file_name, 'r')
                input_file = io.TextIOWrapper(input_file, encoding="utf-8")
                this_delimiter = delimiters[file_name]
                time_stamp(file_name, 'Open File in Irregular File Checker')
                for idx, line in enumerate(input_file):
                    line = (line.strip().split(this_delimiter))
                    line = remove_quotes(line)  # Remove "'Double Quotes'"
                    line_length = len(line)
                    if line_length != 50:
                        this_error = ("{},{},Irregular Line (Before Fix),{}".format(file_name, idx, line))
                        this_error = this_error.split(sep=",")
                        errors_dict['errors'].append(this_error)
                        irregular_line_fix(line)
                        this_error = ("{},{},Irregular Line (After Fix),{}".format(file_name, idx, line))
                        this_error = this_error.split(sep=",")
                        errors_dict['errors'].append(this_error)
                    else:
                        pass
                time_stamp(file_name, 'Close File in Irregular File Checker')
                input_file.close()
    print("\n\n*** DONE ***\n\n")


def time_stamp(file_name, event, time_dict = time_dict):
    # Purpose:
    #
    # Variables:
    #
    # Uses the following functions to run:
    #
    # Is used by the following functions to run:
    #
    date_time_stamp = time.localtime()
    date_stamp = (time.strftime("%Y-%m-%d", date_time_stamp))
    time_stamp = (time.strftime("%H:%M:%S", date_time_stamp))
    write_this = ("{},{},{},{}").format(file_name, event, date_stamp, time_stamp )
    write_this = write_this.split(sep=",")
    time_dict['time'].append(write_this)


def process_files(write_when = 1500):
    # Purpose:
    #   The Master Mind that coordinates the many functions of the processing.
    #   Calling this function will initiate the processing
    #
    # Variables:
    #   zip_files = are the files folder names that you would like to pass through this.
    #   write_when = When this number of row is reached, the holding dict will save the rows to file
    #
    # Uses the following functions to run:
    #
    #
    # Is used by the following functions to run:
    #
    #
    zip_files = os.listdir(file_r_folder)
    for this_zf in zip_files:
        with ZipFile(file_r_folder + this_zf, 'r') as zf:
            zipped_files = zf.namelist()
            for file_name in zipped_files:
                input_file = zf.open(file_name, 'r')
                input_file = io.TextIOWrapper(input_file, encoding="utf-8")
                this_delimiter = delimiters[file_name]
                time_stamp(file_name, 'Open File')
                for idx, line in enumerate(input_file):
                    line = (line.strip().split(this_delimiter))
                    line = remove_quotes(line)  # Remove "'Double Quotes'"
                    prepare_rows(idx, line, file_name, write_when)
                    if file_r_folder != 'data/' and idx >= 6000000:
                        break
                    else:
                        pass
                time_stamp(file_name, 'Close File')
                input_file.close()
    purge_save_holding(holding_dict)
    print("\n*** DONE ***")


def run_info():
    # Purpose:
    #   saves the run info to a 'run_info.csv' file
    #
    # Variables:
    #   counter_dict
    #   writtent_to_dict
    #   time_dict
    #
    # Uses the following functions to run:
    #   save_this
    #   process_files() <reports information from this run>
    #
    # Is used by the following functions to run:
    #   none
    #
    print("\t*** FILE NAME *** \t\t\t\t\t*** Total Rows In File ***")
    for key, value in counter_dict.items():
        print("File:  WedgeFile_{}.csv\t\t\t\tTotal Rows:  {}".format(format(int(key), "04"), format(int(value), "03")))
    print("\n\n\t*** FILE NAME *** \t\t\t\t\t*** Total Times File Was Opened & Saved To ***")
    for key, value in written_to_dict.items():
        print("FILE:  {}\t\t\t\tTimes Opened to Save: {}".format(key, value))
    print("\n\n\t*** FILE NAME *** \t\t\t\t\t\t\t\t\t*** START PROCESSING TIME ***\t\t*** END PROCESSING TIME ***")
    for key, value in time_dict.items():
        print("FILE:  {}\t\t\t\t\t\t\t\t{}".format(key, value))


def file_parse_time(the_dict_to_Save = time_dict):
    # This works great!
    os.remove('File_Parse_Time.csv')
    holding1 = []
    for key in time_dict.keys():
        holding1 += [key]
        the_file1 = time_dict[key]
        with open('File_Parse_Time.csv', 'a+', newline='') as csvfile:
            the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            the_row.writerow(["File","Event","Date Stamp", "Time Stamp"])
            the_row.writerows(the_file1)
            csvfile.close()
    del time_dict[key]


def error_info(the_dict_to_Save = errors_dict):
    # Purpose:
    #   saves the exceptions to 'exceptions_and_error_report.csv'
    #
    # Variables:
    #   errors_dict
    #
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    #
    # Is used by the following functions to run:
    #   none
    #
    os.remove('exceptions_and_error_report.csv')
    holding1 = []
    for key in errors_dict.keys():
        holding1 += [key]
        the_file1 = errors_dict[key]
        with open('exceptions_and_error_report.csv', 'a+', newline='') as csvfile:
            the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            the_row.writerow(["File","IDX","Event","Line"])
            the_row.writerows(the_file1)
            csvfile.close()
    del errors_dict[key]


#run_command_prompt()
row_irregularity_checker()
#process_files(15000)
file_parse_time(time_dict)

# This will run the function above
#       zip_files = are the files folder names that you would like to pass through this.
#       write_when = When this number of row is reached, the holding dict will save the rows to file
