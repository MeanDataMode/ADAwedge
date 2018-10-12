import os
import io
import csv
from zipfile import ZipFile

zip_files = os.listdir("data/")

#zip_files = zip_files[0] # Will limit the following code to only the 1st file.  RM for production.
## Find the delimiters  # This is the one to keep.
import csv
delimiters = dict()
# Start by reading in all the files again.
for this_zf in zip_files :
    with ZipFile("data/" + this_zf,'r') as zf :
        zipped_files = zf.namelist()
        for file_name in zipped_files :
            input_file = zf.open(file_name,'r')
            input_file = io.TextIOWrapper(input_file,encoding="utf-8")
            dialect = csv.Sniffer().sniff(sample=input_file.readline(),
                                      delimiters=[",",";","\t"])
        delimiters[file_name] = dialect.delimiter
        input_file.close() # tidy up


#print(delimiters)
# Function to remove quotes
def remove_quotes(lists):
    cleaned = []
    for item in lists:
        if item[0:1] == '"' and item[-1:] == '"':
            ab = item[:0] + item[(0+1):]
            ab = ab[:-1]
            cleaned.append(ab)
        elif item[0:1] == "'" and item[-1:]:
            ba = item[:0] + item[(0+1):]
            ba = ba[:-1]
            cleaned.append(ba)
        else:
            cleaned.append(item)
    return cleaned


# This Function Packs the Dictonary
# This function puts the 'for file_name in zipped_files:'
#                              'for idx, line in enumerate(input_file):'
key_head = ['datetime', 'register_no', 'emp_no', 'trans_no', 'upc', 'description', 'trans_type', 'trans_subtype',
            'trans_status', 'department', 'quantity', 'Scale', 'cost', 'unitPrice', 'total', 'regPrice', 'altPrice',
            'tax', 'taxexempt', 'foodstamp', 'wicable', 'discount', 'memDiscount', 'discountable', 'discounttype',
            'voided', 'percentDiscount', 'ItemQtty', 'volDiscType', 'volume', 'VolSpecial', 'mixMatch', 'matched',
            'memType', 'staff', 'numflag', 'itemstatus', 'tenderstatus', 'charflag', 'varflag', 'batchHeaderID', 'local',
            'organic', 'display', 'receipt', 'card_no', 'store', 'branch', 'match_id', 'trans_id']
def pack_dict(input_file):
    for idx, line in enumerate(input_file):
        line = (line.strip().split(this_delimiter))
        line = remove_quotes(line)
        transaction_dict = {}
        for item in range(len(line)):
            transaction_dict[key_head[item]] = line[item]
        name = ("file name = {}  \nRow# = {}".format(file_name,idx))
        print("""""")
        print(name)
        print(transaction_dict)
        if idx > 1:
            break

# This is a function that brings in all files in location "data/": and unzips the file
# uses the following fuctions within:  pack_dict()
from zipfile import ZipFile
zip_files = os.listdir("data/")

a = []

def bring_all(zip_files):
    for this_zf in zip_files :
        with ZipFile("data/" + this_zf,'r') as zf :
            zipped_files = zf.namelist()
            for file_name in zipped_files :
                input_file = zf.open(file_name,'r')
                input_file = io.TextIOWrapper(input_file,encoding="utf-8")
                this_delimiter = delimiters[file_name]
                ##print(delimiters[file_name])
                #pack_list(input_file)
                print(input_file[0])
                for idx, line in enumerate(input_file):
                    line = (line.strip().split(this_delimiter))
                    line = remove_quotes(line) #will remove "'Double Quotes'"
                    # Will pull only the list
                    if line[45:46] != ['card_no']: #Will pass all items that are not card #3.
                        if line[45:46] != ['3']: #This will sort, place in holding, and save the row.
                            ##print("")
                            ##print("THIS WILL Print AND SAVE")
                            ##print(line[45])
                            ##print("File: {} - idx: {}".format(input_file,idx))
                            a.extend(line) # NOT SURE IF THIS IS WHAT I WANT TO DO HERE, TO UNDO REMOVE ALL ##
                            ##print(line)
                            # NOW MOVE FILE


                            # The .extend were I want to combine file?
                            # Fuck


                            # GOAL:
                            # The goal is to count number of rows in holding to be saved and when holding rows >= 1,000 then save to file
                            # plan:
                            #     Identify customer number
                            #        based on customer number assign to holding place
                            #           Check to see if holding place exists.
                            #              if doesnt exist, CREATE
                            #              else: see how many rows are in holding place
                            #               if rows >=1,000    :SAVE
                            #               else rows < 1,000  :place in holding


                        else: #This code will designate what to do with 'card_no'== 3
                            ##print("")
                            ##print("This Is Card 3")
                            ##print(line[45:46])
                            ##print("File: {} - idx: {}".format(input_file,idx))
                            pass
                    else: #This will not save the file as it is a HEADER row.
                        ##print("")
                        ##print("THIS IS A HEADER")
                        ##print(line[45:46])
                        ##print("File: {} - idx: {}".format(input_file,idx))
                        pass
                    if idx <= 1:
                        break
                input_file.close()

#Import All Files
from zipfile import ZipFile
zip_files = os.listdir("data/")
bring_all(zip_files)
