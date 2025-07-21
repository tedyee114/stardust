'''All User input goes into function called GUI
    From there, the piece at the bottom that starts with if __name__ == "__main__": 
calls a series of other functions (which start with "def _____"). Some of these
functions are stored in other scripts for simplicity, so they need to be imported
before they can be used.
    First it calls a function called "list_files_function" with unput arguments 
    "bucket" and "folder". This function creates a CSV file (comma separated 
    values, basically an excel sheet) of all files in the specified location in S3
    Next the function called "p1_function" is used to send each of the files in 
that CSV file through Textract (the AI software). However, Textract directly 
return anything when we run it asynchronously (using multiple computers at once,
instead of one at a time). What it does give us is a location of where the output
data is stored for 7 days after creation, called a JobID. p1_function then creates
a bunch CSV files (we do it in batches because Textract gets overwhelmed when it 
all is sent at once) listing the filename and the JobID of where it's extracted data is stored.
    Finally, a function called "p2_function" retrieves the information from the 
JobID locations and saves it as a bunch JSON (JavaScript Object Notation) files 
(they get split up so the files aren't huge)'''
import os				            #allows for filepath access
import csv                          #allows writing and reading of CSV (comma separated value) files
import datetime			            #shows start and end time, duration
import guibackend as gb             #script that makes the GUI
import functs	                    #script functions to get list of files in s3
                                        #returns csv of filenames with paths
import part1_files_to_queue as p1	#script to send files to Textract and return JobIDs
                                        #returns many CSV of filenames+paths and JobIDs into outputpath
import part2_textract_to_json as p2	#script to extract JSON data at given JobIDs
                                        #returns many JSON files into outputpath
import uploadtos3               	#script to extract JSON data at given JobIDs
                                        #returns many JSON files into outputpath
import tkinter as tk                #imports the library used to make GUIs
from tkinter import ttk             #imports another piece of that library

def guibackend():                   #this doesn't work yet
    a,b = gb.whole_process()
    print (a,b)

def GUI():
    # super cool stuff you wouldn't understand
    bucket          = "bellinghamtest"                                          #the bucket where files are stored, enclosed in quotes, default = "bellinghamtest"
    folder          = "walpole"                                                 #the folder of the project you're working on, enclosed in quotes, no default
    csv_base_name   = "walpole_20240603"                                        #what to name the outputs, enclosed in quoates, typically the town name followed by the date, i.e. "walpole_20240603"
    file_num        = 0                                                         #leave this at 0, it tells whow many files to skip, which we rarely want
    max_files       = 600                                                       #when to stop processing input files, theoretically just 99999999, but lowered to avoid timeouts, defualt = 600
    batch_size      = 10                                                        #self-explanatory, applies to p1, the input files, default = 10
    keywords        = ["lead", "ld", "lear", "lero"]                            #not functional, used to tell programmers about known town-specific typos, example = ["lead", "ld", "lear", "lero"]
    typos           = {"owmer":"owner"}                                         #not functional, used to tell programmers about known town-specific typos, example = {"owmer":"owner"}
    new_file_cutoff = 20                                                        #used for p2, I really don't know, default = 20
    start_inc       = 0                                                         #the number should suffixes start at, i.e. 50 -> output_file_50.json, defualt = 0
    
    
    preferences = "Start Time: " + str(datetime.datetime.now())
    preferences += "Bucket: " + str(bucket) + "\n"                               #this whole block just saves all inputted settings as a .txt file (by making for reviewing purposes
    preferences += "Folder: " + str(folder) + "\n"                                  #it does this by sticking all the settings together as a textstring variable called preferences, which it writes as an output file a the end
    preferences += "CSV Base Name: " + str(csv_base_name) + "\n"
    preferences += "File Num: " + str(file_num) + "\n"
    preferences += "Max Files: " + str(max_files) + "\n"
    preferences += "Batch Size: " + str(batch_size) + "\n"
    preferences += "Keywords: " + str(keywords) + "\n"
    preferences += "Typos: " + str(typos) + "\n"
    preferences += "New File Cutoff (p2): " + str(new_file_cutoff) + "\n"
    preferences += "Start Inc (p2): " + str(start_inc) + "\n"
    os.makedirs(folder, exist_ok=True)
    f = open(f"{folder}/{folder}_preferences.txt", "a")
    f.write(preferences)
    f.close()
    
    return bucket, folder, csv_base_name, file_num, max_files, batch_size, keywords, typos, new_file_cutoff, start_inc  #this is the output of the function called GUI
  
def list_files_function(bucket, folder):
    if not os.path.exists(folder):                                              #create a project folder if one doesn't already exist
        os.makedirs(folder)
    subfolder = folder+"/CSV_JobID_Maps"                                        #create a subfolder called "CSV_JobID_Maps" if one doesn't already exist
    if not os.path.exists(subfolder):
        os.makedirs(subfolder)
    s3_filename = f"{folder}/list_of_{folder}_files_in_S3.csv"                  #defines the output location
    functs.export_s3_files(bucket = bucket, folder_name = folder, s3_filename = s3_filename)    #retrieve a list of files at the specified location and save it ther
    return s3_filename

def p1_function(bucket, folder, s3_csv, csv_base_name, max_files = None, file_num = 1, batch_size = 100):
    """
    bucket (str) : s3 bucket name
    folder (str) : s3 folder name -- process files in this folder!
    s3_csv (str) : List of all files in the specified bucket & folder. This CSV must be an output from list_s3_files.py
    csv_base_name (str) : Basename for output CSV_JobIDMaps, which will be incremented per batch
    max_files (str) : TOTAL maximum number of files to process before breaking out of this function
    file_num (int) (default: 1): Start at this # as increment on the csv_base_name. Recommend using a different file_num 
    batch_size (int) : Numer of files to include PER FUNCTION CALL (and thus, per output CSV). Can specify None if you just want to do everything as one
    
    Use this to process all unprocessed files in s3 (up to max_files), using chunks of some # of files at a time (separate CSVs, etc)
    """
    max_files_arg = int(max_files) if max_files else None
    safety = 0
    total_files_processed = 0
    
    s3_files = []
    with open(s3_csv, "r") as s3_file:
        s3_files = [r.strip() for r in s3_file.read().splitlines()[1:]]
    
    # Get list of files that have been processed, either in p1 or p2 or both, for this folder
    processed = p1.get_processed_csv(folder)
    
    # List of files that haven't been processed yet
    files_to_process = [file for file in s3_files if file not in processed and os.path.join(folder, file) not in processed]
    
    
    while True:
        safety += 1
        if safety >= 1000: # Just in case the breaks on total_files_processed / max_files prove 
            print("BROKE ON SAFETY = 1000")
            break
        
        # Each run will only include a # of files up to specified batch_size
        # Returns the # of files that were processed and a list of the files that were processed in this run
        processed_ct, files_complete = p1.s3_csv_to_queue(folder, bucket=bucket, s3_csv_file = s3_csv, max_files = batch_size or max_files, out_csv_file = f"{folder}/CSV_JobID_Maps/{csv_base_name +'_' if not csv_base_name.endswith('_') else csv_base_name}{file_num}.csv", spec_s3_files=s3_files, spec_files_to_process=files_to_process)
        files_to_process = [f for f in files_to_process if f not in files_complete and os.path.join(folder, f) not in files_complete] # Remove completed files from the list of 'files_to_process', so they're not supplied to the next run of the function
        
        # If 0 files were processed in last run, same will happen next time --> Break out of loop
        if processed_ct == 0:
            break
        
        # Update count of how many total files we've processed
        total_files_processed += processed_ct
        # If we've now processed all the # of files we want to processed, break out of loop
        if max_files and total_files_processed >= max_files:
            break 
        
        file_num += 1

def p2_function(folder, csv_base_name, new_file_cutoff = 20, start_inc = 1):
    """
    folder (str) : Folder name
    csv_base_name (str) : csv_base_name used in p1 -- will process all CSV files starting with this string
    new_file_cutoff (int) (default: 20): Max. number of files to include in a single JSON; low numbers will result in MANY .json files, but will have significantly lower processing time than with larger values
    start_inc (int) (default: 1): Number from which to start incrementing until we find a .json file with enough space for new files. If numerous JSONs have been output already (ON THIS SAME DATE), set this to the highest incremented # in use
    """
    # if not os.path.exists(folder+"JSON_Outputs"):
    #     os.makedirs(folder+"/JSON_Outputs")
    csv_files = [f for f in os.listdir("CSV_JobIDMaps") if f.startswith(csv_base_name) and "Complete.csv" not in f]
    completed = []
    ct = 0
    
    for csv_file in csv_files:
        print(f"\n*** Processing {csv_file} ***")
        ct += 1
        if ct >= 10000:
            break
        
        #start_inc = 1
        
        # Update start_inc to whatever was used at the end of this most-recent batch
        j_obj, start_inc = p2.process_files(
            folder, 
            csv_file = csv_file, 
            new_file_cutoff = 20, 
            start_inc = start_inc,)
        
        if j_obj is not None and j_obj != "-broken-":
            completed.append(csv_file)
        else:
            break

    return completed

if __name__ == "__main__":
				                
    uploadtos3.main()
    #start GUI
    # guibackend()
    bucket, folder, csv_base_name, file_num, max_files, batch_size, keywords, typos, new_file_cutoff, start_inc = GUI()
    
    #   when button "run" clicked:
    starttime = datetime.datetime.now()
    
    s3_csv = list_files_function(bucket, folder)
    print("List of all files in given bucket+folder saved to: "+ s3_csv)
    print("List Creation Elapsed Time: ", datetime.datetime.now() - starttime)
    
    
    p1_function(
        folder = folder,
        bucket = bucket,
        s3_csv = s3_csv,
        csv_base_name = csv_base_name,
        file_num = file_num,
        max_files = max_files,
        batch_size=batch_size)
    print("List of all JobIDs saved to: "+ csv_base_name)
    print("P1 Elapsed Time: ", datetime.datetime.now() - starttime)
    
    
    p2_function(
        folder          = folder,
        csv_base_name   = csv_base_name,
        new_file_cutoff = new_file_cutoff,
        start_inc       = start_inc)
    print("P2 Elapsed Time: ", datetime.datetime.now() - starttime)
    print("DONE###########################################################################################################")