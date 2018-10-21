# Written by A.T. Layton
#
# Oct 21, 2018
#
import os  # https://docs.python.org/3/library/os.html
import io
import csv  # https://docs.python.org/3.4/library/csv.html
from zipfile import ZipFile  # https://docs.python.org/3.4/library/zipfile.html
from collections import defaultdict
import time


# Headers for the output files.
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


# Dicts that will be used.
holding_dict = defaultdict(list)    # Holds The Lines that have been processed, but needing To Be Saved.
time_dict = defaultdict(list)       # Records the time the input file was opened.
written_to_dict = defaultdict(int)  # Counts times the saved files has been opened and saved to.
counter_dict = defaultdict(int)     # Counts times number of rows have been pared and designated to the saved file
card_no_dict = defaultdict(lambda: defaultdict(int))  # Counts rows saved per CardNo
errors_dict = defaultdict(list)     # Records the exceptions and errors that occur when processing.
delimiters = dict()


# Folder Pointers
# read_from_folder = 'test_read_data/'  # read from here
# write_to_folder = 'test_write_data/'  # write to here
read_from_folder = 'data/'  # read from here
write_to_folder = 'write_data/'  # write to here


# Gets the delimiters of the files
zip_file_folder = os.listdir(read_from_folder)
for this_zip_file in zip_file_folder:
    with ZipFile(read_from_folder + this_zip_file, 'r') as the_zip_file:
        the_zipped_files = the_zip_file.namelist()
        for the_file_name in the_zipped_files:
            the_input_file = the_zip_file.open(the_file_name, 'r')
            the_input_file = io.TextIOWrapper(the_input_file, encoding="utf-8")
            dialect = csv.Sniffer().sniff(sample=the_input_file.readline(),
                                          delimiters=[",", ";", "\t"])
            delimiters[the_file_name] = dialect.delimiter
            os.SEEK_SET
            the_input_file.close()  # tidy up


# Function is BELOW:  -na-
# Function is ABOVE:  -na-
def run_command_prompt():
    # Purpose:
    #   This is the user interface.
    #   The follow functions are directed by the users responses:
    #       3) DELETE contents of write to folder before writing to folder.
    # Variables:
    #   -na-
    # Uses the following functions to run:
    #   -na-
    # Is used by the following functions to run:
    #   -na-
    write_folder = os.listdir(write_to_folder)
    print("\n\n")
    print("Hello, and welcome!")
    write_folder_len = len(os.listdir(write_to_folder))
    if write_folder_len > 0:
        time.sleep(.25)
        print("Before we get started, I need to take care of a few housekeeping items.")
        time.sleep(.25)
        print("\n\t\tYou have  {}  files in your  'WRITE TO'  file folder\n\t\t*** File Folder:   '{}'".format(write_folder_len,
                                                                                                               write_to_folder))
        time.sleep(.25)
        while True:
            try:
                to_clear = input("\nDo you want me to  DELETE  these files? (Y/N)   : ").lower()
                time.sleep(.25)
                if to_clear == 'y' or to_clear == 'yes':
                    while True:
                        confirm = input("CONFIRM you want to  DELETE  these files permanently? (Y/N)   : ").lower()
                        try:
                            if confirm == 'y' or confirm == 'yes':
                                for this_file in write_folder:
                                    this_file = ("{}{}".format(write_to_folder, this_file))
                                    os.remove(this_file)
                                for i in [3, 2, 1]:
                                    time.sleep(1)
                                    print("\n\t\t\t\t\t\t\t{}".format(i))
                                time.sleep(1)
                                print("\nYour files have been successfully deleted.")
                                time.sleep(2)
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
                    print("\n\t\t\t*** Starting File Processing ***\n\n\n\n\n")
                    break
                else:
                    print("Sorry, I didn't understand that.")
                    continue
            except ValueError:
                print("error")
            print("\nThank You!\n")
            break
    print("\n\t\t\t*** Starting File Processing ***\n\n\n\n\n")
    time.sleep(2)


# Function is BELOW:  -na-
# Function is ABOVE:  process_files()
def remove_quotes(line):
    # Function that removes the double quotes
    #   This removes double quotes only at the start and the end of an item string, within a line.
    # Variables:
    #   lists = list of items to remove double quotes from.
    # Uses the following functions to run:
    #   -na-
    # Is used by the following functions to run:
    #   row_irregularity_checker()
    #   process_files()
    cleaned = []
    for item in line:
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


# Function is BELOW:  -na-
# Function is ABOVE:  prepare_rows()
def irregular_line_fix(irregular_line):
    # Fixes error that happens when the 'description' has "," within.
    #   Fixes lines that have more than 50 items in it.
    # Variables:
    #   irregular_line = the line that has multiple "," within it.
    # Uses the following functions to run:
    #   -na-
    # Is used by the following functions to run:
    #   prepare_rows
    line_length = len(irregular_line)
    y = line_length - 49 + 5
    description_fix = irregular_line[5:y]
    word = ",".join(description_fix)
    word = word.strip('"')
    irregular_line.insert(5, word)
    y = line_length - 49 + 6
    del irregular_line[6:y]
    return irregular_line


# Function is BELOW:  -na-
# Function is ABOVE:  hold_this(), purge_save_holding()
def save_this(data_matrix, in_holding_mod, write_to_folder):
    # Purpose:
    #   Writes the rows to the saved file.
    # Variables:
    #   data_matrix = the lines to write to file
    #   in_holding_mod = the file number to write to.
    #   write_to_folder = folder to write to
    # Uses the following functions to run:
    #   -na-
    # Is used by the following functions to run:
    #   hold_this(), purge_save_holding()
    check_first = os.listdir(write_to_folder)
    # file_number = (format(int(in_holding_mod), "04"))
    this_file = ("WedgeFile_{}.csv".format(format(int(in_holding_mod), "04")))
    written_to_dict[this_file] += 1
    if this_file in check_first:  # will NOT add a header
        with open(write_to_folder + this_file, 'a+', newline='') as csvfile:
            the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            the_row.writerows(data_matrix)
            csvfile.close()
    else:  # will ADD a header
        with open(write_to_folder + this_file, 'a+', newline='') as csvfile:
            the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            the_row.writerow(header)
            the_row.writerows(data_matrix)
            csvfile.close()


# Function is BELOW:  save this()
# Function is ABOVE:  prepare_rows()
def hold_this(mod, line, write_when, write_to_folder, card_no):
    # Purpose:
    #   Adds the line as an item within a dictionary of other files that will be written to the
    #   same output file. When the number of lines of that dictionary reaches
    #   a designated number, this function will then call the 'save_this' function to save
    #   the group of lines to the designated file.
    # Variables:
    #   mod = used as the dictionary key, and the unique ID for the output file name.
    #   line = is the line to be added to the dictionary and later saved to an output file.
    #   write_when = is a number, once reached, the 'save_this' function will be called ot save
    #                the lines to the output file.
    #   write_to_folder = folder to write to
    # Uses the following functions to run:
    #   save_this()
    # Is used by the following functions to run:
    #   prepare_rows()
    if len(holding_dict[mod]) >= write_when:  # Will SAVE to ouput file, will only add to holding dict.
        holding_dict[mod].append(line)
        counter_dict[("WedgeFile_{}.csv".format(format(int(mod),"04")))] += 1
        card_no_dict[("WedgeFile_{}.csv".format(format(int(mod), "04")))][card_no] += 1
        the_file = holding_dict[mod]
        save_this(the_file, mod, write_to_folder)
        del holding_dict[mod]
    else:  # Will NOT save to ouput file, will only add to holding dict.
        counter_dict[("WedgeFile_{}.csv".format(format(int(mod),"04")))] += 1
        card_no_dict[("WedgeFile_{}.csv".format(format(int(mod), "04")))][card_no] += 1
        holding_dict[mod].append(line)


# Function is BELOW:  hold_this(), irregular_line_fix()
# Function is ABOVE:  process_files()
def prepare_rows(idx, line, file_name, write_when, write_to_folder):
    # Purpose:
    #   Prepares the lines to be written to the dict.  Card 3 and the other cards have a specific way
    #   the output file name is designated. This function ensures proper output file naming.
    # Variables:
    #   file_name = the name of the contributing input file.
    #   write_when = is a number, once reached, the 'save_this' function will be called ot save
    #                the lines to the output file.
    #   idx = the item within the line
    #   line = the line being passed through it.
    #   write_to_folder = folder to write to
    # Uses the following functions to run:
    #   hold_this()
    # Is used by the following functions to run:
    #   process_files()
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
        cardno = int(line[45])
        mod = cardno % 171 + 1
        hold_this(mod, line, write_when, write_to_folder, cardno)
    elif line[45:46] != ['card_no']:  # This code will designate what to do with 'card_no'== 3
        cardno = int(line[45])
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
        hold_this(mod, line, write_when, write_to_folder, cardno)


# Function is ABOVE:  process_files()
# Function is BELOW:  save_this()
def purge_save_holding(write_to_folder, holding_dict=holding_dict):
    # Purpose:
    #   This function will save items within the dictionary 'holding_dict'
    #   at the end of processing all files.
    # Variables:
    #   holding_dict = the location of all lines needing writing to the designated output file.
    #   write_to_folder = folder to write to
    # Uses the following functions to run:
    #   save_this()
    # Is used by the following functions to run:
    #   process_files()
    last_holdings = []
    for key in holding_dict.keys():
        last_holdings += [key]
    for idx, mod in enumerate(last_holdings):
        the_file = holding_dict[mod]
        save_this(the_file, mod, write_to_folder)
        del holding_dict[mod]


# Function is BELOW:  -na-
# Function is ABOVE:  process_files()
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


# Function is BELOW:  time_stamp(), remove_quotes()
# Function is ABOVE:  process_files()
def row_irregularity_checker():
    # Purpose:
    #   To do a test run by going through each of the files, lines, and items in an effort
    #   to catch any errors that may cause problems (ex:  rows with more than 50 columns,
    #   lines not split properly... ect.)
    # Variables:
    #   zip_files[] = are the files folder names that you would like to pass through this.
    #   delimiters[] = used to designate what to split on
    # Uses the following functions to run:
    #   time_stamp(),
    #   remove_quotes()
    # Is used by the following functions to run:
    #   file_contain_nrows(), file_open_counter(), error_info()
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
    print("\n\n\t\t\t*** DONE ***\n\n")


# Function is BELOW:  time_stamp(), remove_quotes(), prepare_rows(), purge_save_holding()
# Function is ABOVE:  file_contain_nrows(), file_open_counter(), file_processing_time(), error_info()
def process_files(write_when=1500, break_at=60000000):
    # Purpose:
    #   The Master Mind that coordinates the many functions of the processing.
    #   Calling this function will initiate the processing
    # Variables:
    #   zip_files[] = are the files folder names that you would like to pass through this.
    #   delimiters[] = used to designate what to split on
    #   write_when = When this number of row is reached, the holding dict will save the rows to file
    #   write_to_folder = folder to write to
    # Uses the following functions to run:
    #   time_stamp()
    #   remove_quotes()
    #   prepare_rows()
    #   purge_save_holding()
    # Is used by the following functions to run:
    #   file_contain_nrows(), file_open_counter(), file_processing_time(), error_info()
    zip_files = os.listdir(read_from_folder)
    for this_zf in zip_files:
        with ZipFile(read_from_folder + this_zf, 'r') as zf:
            zipped_files = zf.namelist()
            for file_name in zipped_files:
                input_file = zf.open(file_name, 'r')
                input_file = io.TextIOWrapper(input_file, encoding="utf-8")
                this_delimiter = delimiters[file_name]
                input_file.seek(0)
                print("")
                time_stamp(file_name, 'Open File')
                for idx, line in enumerate(input_file):
                    line = (line.strip().split(this_delimiter))
                    line = remove_quotes(line)  # Remove "'Double Quotes'"
                    prepare_rows(idx, line, file_name, write_when, write_to_folder)
                    if read_from_folder != 'data/' and idx >= break_at:
                        break
                    else:
                        pass
                time_stamp(file_name, 'Close File')
                input_file.close()
    purge_save_holding(write_to_folder, holding_dict=holding_dict)
    print("\n\t\t\t*** FILE PROCESSING COMPLETE ***\n\n")
    file_contain_nrows()
    file_open_counter()
    file_processing_time()
    rows_per_card_no()
    error_info()


# Function is BELOW:  file_processing_time(),error_info()
# Function is ABOVE:  -na-
def save_list_report(the_dict_to_save, name_to_save_report_as, file_head):
    # Purpose:
    #   saves list dicts into a csv.
    # Variables:
    #   the_dict_to_save = the dict to save.
    #   name_to_save_report_as = the name of the saved output file.
    #   file_head = The Header in the file
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    # Is used by the following functions to run:
    #   -na-
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


# Function is BELOW:  file_contain_nrows(), file_contain_nrows
# Function is ABOVE:  -na-
def save_counting_report(the_dict_to_save, name_to_save_report_as, file_head):
    # Purpose:
    #   saves counting dicts into a csv.
    # Variables:
    #   the_dict_to_save = the dict to save.
    #   name_to_save_report_as = the name of the saved output file.
    #   file_head = The Header in the file
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    # Is used by the following functions to run:
    #   -na-
    final_count_dict = defaultdict(list)
    for key, value in sorted(the_dict_to_save.items()):
        this_count = ("{},{}".format(key, value))
        this_count = this_count.split(sep=",")
        final_count_dict['counter'].append(this_count)
        file_content = final_count_dict['counter']
    with open(name_to_save_report_as, 'a+', newline='') as csvfile:
        the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        the_row.writerow(file_head)
        the_row.writerows(file_content)
        csvfile.close()
    print("\tSAVE Successful!\t\tFILE:\t{}".format(name_to_save_report_as))


# Function is BELOW:  process_files()
# Function is ABOVE:  -na-
def rows_per_card_no(the_dict_to_save=card_no_dict):
    # Purpose:
    #   saves the number of times an output file was opened and saved to.
    #   Can be useful when trying to decide how many lines to write at one.
    # Variables:
    #   written_to_dict
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    # Is used by the following functions to run:
    #   -na-
    date_time_stamp = (time.strftime("%Y%m%d %H%M", (time.localtime())))
    name_to_save_report_as = ("Run Reports/{} Rows per CardNo.csv".format(date_time_stamp))
    final_count_dict = defaultdict(list)
    for key1, value1 in sorted(the_dict_to_save.items()):
        for key2, value2 in sorted(value1.items()):
            this_count = ("{},{},{}".format(key1, key2, value2))
            this_count = this_count.split(sep=",")
            final_count_dict['counter'].append(this_count)
            the_file = final_count_dict['counter']
    with open(name_to_save_report_as, 'a+', newline='') as csvfile:
        the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        the_row.writerow(["File","Card No.","Rows in File Associated with Card No."])
        the_row.writerows(the_file)
        csvfile.close()
    print("\tSAVE Successful!\t\tFILE:\t{}".format(name_to_save_report_as))


# Function is BELOW:  process_files()
# Function is ABOVE:  save_counting_report()
def file_open_counter(the_dict_to_save=written_to_dict):
    # Purpose:
    #   saves the number of times an output file was opened and saved to.
    #   Can be useful when trying to decide how many lines to write at one.
    # Variables:
    #   written_to_dict
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    # Is used by the following functions to run:
    #   -na-
    file_head = ["File","Times File Opened and Saved To"]
    date_time_stamp = (time.strftime("%Y%m%d %H%M", (time.localtime())))
    name_to_save_report_as = ("Run Reports/{} File Open Counter.csv".format(date_time_stamp))
    save_counting_report(the_dict_to_save, name_to_save_report_as, file_head)


# Function is BELOW:  process_files()
# Function is ABOVE:  save_counting_report
def file_contain_nrows(the_dict_to_save=counter_dict):
    # Purpose:
    #   saves the number of rows within a file.
    # Variables:
    #   counter_dict
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    # Is used by the following functions to run:
    #   -na-
    file_head = ["File","Count of Rows in File"]
    date_time_stamp = (time.strftime("%Y%m%d %H%M", (time.localtime())))
    name_to_save_report_as = ("Run Reports/{} File Row Counts.csv".format(date_time_stamp))
    save_counting_report(the_dict_to_save, name_to_save_report_as, file_head)


# Function is BELOW:  process_files()
# Function is ABOVE:  save_list_report
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
    name_to_save_report_as = ("Run Reports/{} File Processing Times.csv".format(date_time_stamp))
    save_list_report(the_dict_to_save, name_to_save_report_as, file_head)


# Function is BELOW:  process_files()
# Function is ABOVE:  save_list_report
def error_info(the_dict_to_save=errors_dict):
    # Purpose:
    #   saves the exceptions to 'exceptions_and_error_report.csv'
    #   If no errors are saved to dict, then no file will output
    # Variables:
    #   errors_dict
    # Uses the following functions to run:
    #   process_files() <reports errors from this run>
    # Is used by the following functions to run:
    #   -na-
    file_head = ["File", "Event", "Date Stamp", "Time Stamp"]
    date_time_stamp = (time.strftime("%Y%m%d %H%M", (time.localtime())))
    name_to_save_report_as = ("Run Reports/{} Exceptions and Error Report.csv".format(date_time_stamp))
    save_list_report(the_dict_to_save, name_to_save_report_as, file_head)


run_command_prompt()
process_files(write_when=20000, break_at=6000000000)
