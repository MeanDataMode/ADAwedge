import os  # https://docs.python.org/3/library/os.html
import io
import csv  # https://docs.python.org/3.4/library/csv.html
from zipfile import ZipFile  # https://docs.python.org/3.4/library/zipfile.html
from collections import defaultdict
import time


# Folder Pointers
file_r_folder = 'test_read_data/'  # read from here
file_w_folder = 'test_write_data/'  # write to here
#file_r_folder = 'data/'  # read from here
#file_w_folder = 'write_data/'  # write to here


header = ['datetime', 'register_no', 'emp_no', 'trans_no', 'upc', 'description', 'trans_type', 'trans_subtype',
            'trans_status', 'department', 'quantity', 'Scale', 'cost', 'unitPrice', 'total', 'regPrice', 'altPrice',
            'tax', 'taxexempt', 'foodstamp', 'wicable', 'discount', 'memDiscount', 'discountable', 'discounttype',
            'voided', 'percentDiscount', 'ItemQtty', 'volDiscType', 'volume', 'VolSpecial', 'mixMatch', 'matched',
            'memType', 'staff', 'numflag', 'itemstatus', 'tenderstatus', 'charflag', 'varflag', 'batchHeaderID',
            'local', 'organic', 'display', 'receipt', 'card_no', 'store', 'branch', 'match_id', 'trans_id']


holding_dict = defaultdict(list)
time_dict = defaultdict(list)
written_to_dict = defaultdict(int)
errors_dict = defaultdict(list)
counter_dict = defaultdict(int)


# Clean out the write to folder before we begin
# write_folder = os.listdir(file_w_folder)
# for this_file in write_folder:
#    this_file = ("{}{}".format(file_w_folder,this_file))
#    os.remove(this_file)
# time.sleep(5)


# finds what the delimiters are for each file
delimiters = dict()
zip_files = os.listdir(file_r_folder)
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
    description_fix = irregular_line[5:8]
    word = ",".join(description_fix)
    word = word.strip('"')
    irregular_line.insert(5, word)
    del irregular_line[6:9]
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
    else: #Will Add a header
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
    if line_length != 50:
        irregular_line_fix(line)
        this_error = [("*** IRREGULAR LINE ***\t\tFILE:  {}\t\tIDX:  {}".format(file_name, idx))]
        errors_dict[input_file].append(this_error)
    else:
        this_error = [("*** IRREGULAR LINE ***\t\tFILE:  {}\t\tIDX:  {}".format(file_name, idx))]
        errors_dict[input_file].append(this_error)
    if line[45:46] != ['card_no']:  # Will pass all items that are not card #3.
        if line[45:46] != ['3']:  # This will sort, place in holding, and save the row.
            cardNo = int(line[45])
            mod = cardNo % 200 + 1
            hold_this(mod, line, write_when)
        else:  # This code will designate what to do with 'card_no'== 3
            date = line[0]
            year = str(date[2:4])
            month = int(date[5:7])
            if month >= 1 and month <= 3:
                quarter = '01'
            elif month >= 4 and month <= 6:
                quarter = '02'
            elif month >= 7 and month <= 9:
                quarter = '03'
            elif month >= 10 and month <= 12:
                quarter = '04'
            else:
                this_error = [("*** ERROR ON QUARTER ***\nFILE:  {}\nIDX:  {}".format(input_file, idx))]
                errors_dict[input_file].append(this_error)
            mod = (year + quarter)
            mod = int(mod)
            hold_this(mod, line, write_when)
    else:  # This will not save the file as it is a HEADER row.
        pass


def purge_save_holding(holding_dict):
    last_holdings = []
    for key in holding_dict.keys():
        last_holdings += [key]
    for idx, mod in enumerate(last_holdings):
        the_file = holding_dict[mod]
        save_this(the_file, mod)
        del holding_dict[mod]


def process_files(zip_files = zip_files, write_when = 15000):
    # zip_files = are the files folder names that you would like to pass through this.
    # write_when = When this number of row is reached, the holding dict will save the rows to file
    # row_stop = use None for production, and any other number to stop after that number of rows.
    # Function that finds what the delimiters are for each file
    start_time = time.localtime()
    start_time = (time.strftime("%Y-%m-%d %H:%M:%S", start_time))
    time_dict.update({'Overall\t\t\t\t\t\t': {'Start': start_time}})
    for this_zf in zip_files:
        with ZipFile(file_r_folder + this_zf, 'r') as zf:
            zipped_files = zf.namelist()
            for file_name in zipped_files:
                input_file = zf.open(file_name, 'r')
                input_file = io.TextIOWrapper(input_file, encoding="utf-8")
                this_delimiter = delimiters[file_name]
                file_start_time = time.localtime()
                file_start_time = (time.strftime("%Y-%m-%d %H:%M:%S", file_start_time))
                print("")
                print("FILE:\t{}\n\t\t\t\t\t\t\t\t\tSTART TIME:\t{}".format(file_name, file_start_time))
                for idx, line in enumerate(input_file):
                    line = (line.strip().split(this_delimiter))
                    line = remove_quotes(line)  # Remove "'Double Quotes'"
                    prepare_rows(idx, line, input_file, this_delimiter, write_when)
                    if idx >= 100:
                        break
                    else:
                        pass
                input_file.close()
            file_end_time = time.localtime()
            file_end_time = (time.strftime("%Y-%m-%d %H:%M:%S", file_end_time))
            time_dict.update({file_name: {'Start': file_start_time, 'End': file_end_time}})
            print("\t\t\t\t\t\t\t\t\tEND TIME:\t{}".format(file_end_time))
    purge_save_holding(holding_dict)
    end_time = time.localtime()
    print("")
    print("")
    print("*** DONE ***")
    print("")
    print("")
    end_time = (time.strftime("%Y-%m-%d %H:%M:%S", end_time))
    time_dict.update({'Overall\t\t\t\t\t\t': {'Start': start_time, 'End': end_time}})


process_files(zip_files, 15000)
# This will run the function above
#       zip_files = are the files folder names that you would like to pass through this.
#       write_when = When this number of row is reached, the holding dict will save the rows to file


def run_info():
    print("\t*** FILE NAME *** \t\t\t\t\t*** Total Rows In File ***")
    for key, value in counter_dict.items():
        print("File:  WedgeFile_{}.csv\t\t\t\tTotal Rows:  {}".format(format(int(key), "04"), format(int(value), "03")))
    print("")
    print("")
    print("\t*** FILE NAME *** \t\t\t\t\t*** Total Times File Was Opened & Saved To ***")
    for key, value in written_to_dict.items():
        print("FILE:  {}\t\t\t\tTimes Opened to Save: {}".format(key,value))
    print("")
    print("")
    print("\t*** FILE NAME *** \t\t\t\t\t\t\t\t\t*** START PROCESSING TIME ***\t\t*** END PROCESSING TIME ***")
    for key, value in time_dict.items():
        print("FILE:  {}\t\t\t\t\t\t\t\t{}".format(key,value))
run_info()


print("")
print("")
print("\t*** *** ERROR REPORT *** ***")
for key, value in errors_dict.items():
    print("File:  {}\t\t\t\tERROR:  {}".format(key, value))



#################### GOOD TO ADD TO PRIMARY #############################
for key, value in errors_dict.items():
    the_report = holding_dict[key]
    with open('errors_report.csv', 'a+', newline='') as csvfile:
        the_row = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        the_row.writerows(the_report)
        csvfile.close()
