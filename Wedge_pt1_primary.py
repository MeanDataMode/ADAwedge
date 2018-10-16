import os  # https://docs.python.org/3/library/os.html
import io
import csv  # https://docs.python.org/3.4/library/csv.html
from zipfile import ZipFile  # https://docs.python.org/3.4/library/zipfile.html
from collections import defaultdict
import time


# Folder Pointers
# file_r_folder = 'test_read_data/' # read from here
# file_w_folder = 'test_write_data/' # write to here
file_r_folder = 'data/'  # read from here
file_w_folder = 'write_data/'  # write to here


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

holding_dict = defaultdict(list)
time_dict = defaultdict(list)
written_to_dict = defaultdict(int)
errors_dict = defaultdict(list)
counter_dict = defaultdict(int)


zip_files = os.listdir(file_r_folder)
delimiters = dict()
for this_zf in zip_files:
    with ZipFile(file_r_folder + this_zf, 'r') as zf:
        zipped_files = zf.namelist()
        for file_name in zipped_files:
            input_file = zf.open(file_name, 'r')
            input_file = io.TextIOWrapper(input_file, encoding="utf-8")
            dialect = csv.Sniffer().sniff(sample=input_file.readline(),
                                          delimiters=[",", ";", "\t"])
        delimiters[file_name] = dialect.delimiter
        input_file.close()  # tidy up


def clear_w_folder():
    # Clean out the write to folder before we begin
    write_folder = os.listdir(file_w_folder)
    for this_file in write_folder:
        this_file = ("{}{}".format(file_w_folder, this_file))
        os.remove(this_file)
    time.sleep(2)


def run_command_prompt():
    print("\n\n")
    print("Hello, and welcome!")
    write_folder_len = len(os.listdir(file_w_folder))
    if write_folder_len > 0:
        time.sleep(.25)
        print("Before we get started, I need to take care of a few housekeeping items.")
        time.sleep(.25)
        print("It looks like you have {} files in the 'WRITE TO ' location:\t\t*** {} ***".format(write_folder_len,
                                                                                                  file_w_folder))
        time.sleep(.25)
        while True:
            try:
                to_clear = input("\nDo you want me to REMOVE these files? (Y/N)   : ").lower()
                time.sleep(.25)
                if to_clear == 'y' or to_clear == 'yes':
                    while True:
                        confirm = input("Are you sure you want to DELETE these files permanently? (Y/N)   : ").lower()
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
                    print("\n\n")
                    break
                else:
                    print("Sorry, I didn't understand that.")
                    continue
            except ValueError:
                print("error")
            print("\nThank You.\n")
            print("\n*** Starting Run ***\n\n\n\n\n\n\n")
            time.sleep(2)
            break


def remove_quotes(lists):
    # Function that removes the double quotes
    # lists = list of items to remove double quotes from.
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
    # irregular_line = the line that has multiple "," within it.
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
    # Functions that writes the rows to the saved file.
    # data_matrix = the lines to write to file
    # in_holding_mod = the file number to write to.
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
    else: # Will Add a header
        with open(file_w_folder + this_file, 'a+', newline='') as csvfile:
            the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            the_row.writerow(header)
            the_row.writerows(data_matrix)
            csvfile.close()


def hold_this(mod, line, write_when):
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


def prepare_rows(idx, line, input_file, this_delimiter, write_when):
    line_length = len(line)
    quarter = ''
    if line_length != 50:
        irregular_line_fix(line)
        this_error = [("*** IRREGULAR LINE ***\t\tFILE:  {}\t\tIDX:  {}".format(file_name, idx))]
        errors_dict[input_file].append(this_error)
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
            this_error = [("*** ERROR ON QUARTER ***\nFILE:  {}\nIDX:  {}".format(input_file, idx))]
            errors_dict[input_file].append(this_error)
        mod = (year + quarter)
        mod = int(mod)
        hold_this(mod, line, write_when)


def purge_save_holding(holding_dict):
    last_holdings = []
    for key in holding_dict.keys():
        last_holdings += [key]
    for idx, mod in enumerate(last_holdings):
        the_file = holding_dict[mod]
        save_this(the_file, mod)
        del holding_dict[mod]


def row_irregularity_checker():
    # zip_files = are the files folder names that you would like to pass through this.
    start_time = time.localtime()
    start_time = (time.strftime("%Y-%m-%d %H:%M:%S", start_time))
    time_dict.update({'Overall\t\t\t\t\t\t': {'Start': start_time}})
    zip_files = os.listdir('test_read_data/')
    for this_zf in zip_files:
        with ZipFile('test_read_data/' + this_zf, 'r') as zf:
            zipped_files = zf.namelist()
            for file_name in zipped_files:
                input_file = zf.open(file_name, 'r')
                input_file = io.TextIOWrapper(input_file, encoding="utf-8")
                this_delimiter = delimiters[file_name]
                file_start_time = time.localtime()
                file_start_time = (time.strftime("%Y-%m-%d %H:%M:%S", file_start_time))
                print("\nFILE:\t{}\n\t\t\t\t\t\t\t\t\tSTART TIME:\t{}".format(file_name, file_start_time))
                for idx, line in enumerate(input_file):
                    line = (line.strip().split(this_delimiter))
                    line = remove_quotes(line)  # Remove "'Double Quotes'"
                    line_length = len(line)
                    if line_length != 50:
                        print(len(line))
                        print(line)
                        this_error = [("*** IRREGULAR LINE BEFORE ***\t\tFILE:  {}\t\tIDX:  {}\n{}".format(file_name, idx,line))]
                        errors_dict[input_file].append(this_error)
                        irregular_line_fix(line)
                        print("")
                        print(len(line))
                        print(line)
                        this_error = [("*** IRREGULAR LINE AFTER ***\t\tFILE:  {}\t\tIDX:  {}\n{}".format(file_name, idx,line))]
                        errors_dict[input_file].append(this_error)
                    else:
                        pass
                input_file.close()
            file_end_time = time.localtime()
            file_end_time = (time.strftime("%Y-%m-%d %H:%M:%S", file_end_time))
            time_dict.update({file_name: {'Start': file_start_time, 'End': file_end_time}})
            print("\t\t\t\t\t\t\t\t\tEND TIME:\t{}".format(file_end_time))
    end_time = time.localtime()
    print("\n\n*** DONE ***\n\n")
    end_time = (time.strftime("%Y-%m-%d %H:%M:%S", end_time))
    time_dict.update({'Overall\t\t\t\t\t\t': {'Start': start_time, 'End': end_time}})


def file_start_time_stamp(file_name):
    this_time = time.localtime()
    this_time = (time.strftime("%Y-%m-%d %H:%M:%S", this_time))
    time_dict.update({file_name: {'Time Start': this_time}})
    print("File: {}\tStart Time: {}".format(file_name, this_time))


def process_files(zip_files = zip_files, write_when = 15000):
    # zip_files = are the files folder names that you would like to pass through this.
    # write_when = When this number of row is reached, the holding dict will save the rows to file
    # row_stop = use None for production, and any other number to stop after that number of rows.
    # Function that finds what the delimiters are for each file
    for this_zf in zip_files:
        with ZipFile(file_r_folder + this_zf, 'r') as zf:
            zipped_files = zf.namelist()
            for file_name in zipped_files:
                input_file = zf.open(file_name, 'r')
                input_file = io.TextIOWrapper(input_file, encoding = "utf-8")
                this_delimiter = delimiters[file_name]
                file_start_time_stamp(file_name)
                for idx, line in enumerate(input_file):
                    line = (line.strip().split(this_delimiter))
                    line = remove_quotes(line)  # Remove "'Double Quotes'"
                    prepare_rows(idx, line, input_file, this_delimiter, write_when)
                    if file_r_folder != 'data/' and idx >= 20000:
                        break
                    else:
                        pass
                input_file.close()
    purge_save_holding(holding_dict)
    print("\n*** DONE ***")


def run_info():
    print("\t*** FILE NAME *** \t\t\t\t\t*** Total Rows In File ***")
    for key, value in counter_dict.items():
        print("File:  WedgeFile_{}.csv\t\t\t\tTotal Rows:  {}".format(format(int(key), "04"), format(int(value), "03")))
    print("\n\n\t*** FILE NAME *** \t\t\t\t\t*** Total Times File Was Opened & Saved To ***")
    for key, value in written_to_dict.items():
        print("FILE:  {}\t\t\t\tTimes Opened to Save: {}".format(key, value))
    print("\n\n\t*** FILE NAME *** \t\t\t\t\t\t\t\t\t*** START PROCESSING TIME ***\t\t*** END PROCESSING TIME ***")
    for key, value in time_dict.items():
        print("FILE:  {}\t\t\t\t\t\t\t\t{}".format(key, value))


def error_info():
    print("\n\n\t*** *** ERROR REPORT *** ***")
    for key, value in errors_dict.items():
        print("File:  {}\t\t\t\tERROR:  {}".format(key, value))
    for key, value in errors_dict.items():
        the_report = holding_dict[key]
        with open('errors_report.csv', 'a+', newline = '') as csvfile:
            the_row = csv.writer(csvfile, delimiter = ',', quoting = csv.QUOTE_MINIMAL)
            the_row.writerows(the_report)
            csvfile.close()


run_command_prompt()
process_files(zip_files, 15000)
run_info()
error_info()
# This will run the function above
#       zip_files = are the files folder names that you would like to pass through this.
#       write_when = When this number of row is reached, the holding dict will save the rows to file
