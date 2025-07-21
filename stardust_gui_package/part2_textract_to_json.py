import webbrowser, os
import re
import json
from collections import defaultdict
import boto3                #the aws AI that reads the handwriting off of images
import io
from io import BytesIO
import sys
from pprint import pprint
import time
import os
import json
from functs import current_time, csv_namer, json_namer
import functs
import botocore

# Create a session that grabs the Access Key & Secret Access Key, so it doesn't time out
session = boto3.Session(profile_name='ducky2')

def __init__():
    """Makes this an importable module"""
    pass

class ExpiredTokenError(Exception):
    """Just used to be able to throw & catch the specific issue, as needed"""
    def __init__(self, message):
        super().__init__(message)


def get_processed_csv(folder_name):
    """Returns a list of files that have been documented in {folder_name}/JSON_Outputs/_[folder_name]_Part2Complete.csv"""
    import csv
    
    csv_path = f"{folder_name}/JSON_Outputs/_{folder_name}_Part2Complete.csv"
    
    if not os.path.exists(csv_path):
        return []
    
    with open(csv_path, "r") as csv_contents:
        return [r.strip().replace(',', '') for r in csv_contents.read().splitlines()]


def add_processed_json(file_name, folder_name):
    """Add file_name to the CSV file at {folder_name}/JSON_Outputs/_[folder_name]_Part2Complete.csv"""
    if not os.path.exists(f"{folder_name}/JSON_Outputs/_{folder_name}_Part2Complete.csv"):
        with open(f"{folder_name}/JSON_Outputs/_{folder_name}_Part2Complete.csv", "w") as file:
            file.write("File Path")
            
    with open(f"{folder_name}/JSON_Outputs/_{folder_name}_Part2Complete.csv", "a") as file:
        file.write("\n" + file_name + "\n" + os.path.basename(file_name).replace(",", ""))

def get_files_in_json(json_file):
    """Returns a list of files listed in json_file"""
    with open(json_file, "r") as file_contents:
        blocks_out = [j for j in json.loads(file_contents.read())]
        return list(set([j["FileName"] for j in blocks_out if "FileName" in j]))

functs.JSON_FILE_LENGTHS = {}

def increment_json_file(folder_name, out_json = None, start_inc = 1, new_file_cutoff = 50):
    """
    folder_name (str) : The 'prefix' for the file; used to create a specific output file name, if needed
    out_json (str) (optional) : Specify a specific file path for the JSON file; will still be incremented as needed!
    start_inc (int) (default: 1) : The first number to which the function will try to write
    new_file_cutoff (int) (default: 50) : If a file at [folder_name]_[inc] has this many unique files in it already, will increment to the next number
    """
    blocks_out = []
    
    inc = start_inc or 1
    
    out_json = out_json or json_namer(folder_name)
    
    og_out_json = str(out_json)
    
    out_json = og_out_json.replace(".json", f'_{inc}.json') if inc else str(og_out_json)
    if "JSON_Outputs" not in out_json:
        out_json = os.path.join(folder_name, "JSON_Outputs", out_json)
    
    while os.path.exists(out_json):
        #print(f'-checking {out_json}-')
        
        # If we don't know how many files are in a file at this increment, open the files & get the info & add it
        if inc not in functs.JSON_FILE_LENGTHS:
            json_files = get_files_in_json(out_json)
            functs.JSON_FILE_LENGTHS[inc] = int(len(set(json_files)))
        
        if functs.JSON_FILE_LENGTHS[inc] >= new_file_cutoff:
            inc += 1
            out_json = og_out_json.replace(".json",f"_{inc}.json")
        else:
            if inc != start_inc:
                print(f"-- Incrementing to {out_json} --")
            return out_json, inc
    return out_json, inc # return, if none of the out_jsons ever existed!

def process_to_json(jobId, file_name, folder_name, out_json = None, overwrite = False, new_file_cutoff = 20, start_inc = 0, use_analyze = False):
    """
    jobId (str) : JobId number to be passed to textract.get_document_text_detection()
    file_name (str) : The file name of the file being processed (associate with this JobID)
    folder_name (str) : The folder currently being processed
    out_json (str) (optional) : Output json file to use in place of one built from the folder_name and current date
    overwrite (bool) (default: False): Clear contents from existing JSON with same output name
    new_file_cutoff (int) (default: 20) : Try to put up to this many source-files in a single .json file before starting a new one (lower values help with memory and thus, processing speed)
    start_inc (int) (default: 0) : This number is incremented when new_file_cutoff is reached, to create a new file. If highest number already in use is high, then supplying this value can slightly reduce processing time spent trying to find the next available number
    use_analyze (bool) (default: False) : When true, runs "get_document_analysis" instead of "get_document_text_detection" (significantly more expensive, but may have added benefits at some times)
    
    
    Writes the results of textract.get_document_text_detection() as JSON
    """
    curr_inc = start_inc
    
    region_name = 'us-east-1'
    
    maxResults = 1000
    paginationToken = None
    finished = False

    page_number = 0
    
    blocks_out = []
    
    # Remove folder from file name, if present
    file_name = file_name.replace(folder_name + "/", "")
    
    out_json = out_json or json_namer(folder_name)
    if "JSON_Outputs" not in out_json:
        out_json = os.path.join(folder_name, "JSON_Outputs", out_json)
    
    if not os.path.exists(os.path.dirname(out_json)):
        os.mkdir(os.path.dirname(out_json))
    
    # If we don't know how many files are listed in this incremented JSON, or if it's hit the cutoff where it needs a new file, process that
    if f"_{curr_inc}" not in out_json or  curr_inc not in functs.JSON_FILE_LENGTHS or functs.JSON_FILE_LENGTHS[curr_inc] > new_file_cutoff:
        #print(f"\t- Consider incrementing from {curr_inc} -")
        out_json, curr_inc = increment_json_file(folder_name, out_json, start_inc = curr_inc, new_file_cutoff=new_file_cutoff)
    
    if f"_{curr_inc}" not in out_json:
        out_json = out_json.replace(".json", f'_{curr_inc}.json')
        
    # Load the current contents of the JSON file, if we're not trying to overwrite it
    if overwrite == False and os.path.exists(out_json):
        with open(out_json, "r") as json_file:
            blocks_out = [j for j in json.loads(json_file.read()) if ["FileName"] != file_name]
        
    textract = session.client('textract', region_name=region_name)
    while not finished:
        page_number += 1
        response = None
        
        if paginationToken is None:
            # For the first page, no NextToken
            response = textract.get_document_text_detection(JobId=jobId, MaxResults=maxResults) if not use_analyze else textract.get_document_analysis(JobId=jobId, MaxResults=maxResults)
        else:
            # For subsequent pages, supply paginationToke as NextToken
            response = textract.get_document_text_detection(JobId=jobId, MaxResults=maxResults, NextToken=paginationToken) if not use_analyze else textract.get_document_analysis(JobId=jobId, MaxResults=maxResults, NextToken=paginationToken)

        start_time = time.time()
        previous_print_time = time.time()

        while "JobStatus" in response and response["JobStatus"] == "IN_PROGRESS":
            time.sleep(1) # Wait a second (literally) before trying to get the job again
            response = textract.get_document_text_detection(JobId=jobId, MaxResults=maxResults) if not use_analyze else textract.get_document_analysis(JobId=jobId, MaxResults=maxResults)
            
            time_since_printed =  time.time() - previous_print_time
            
            if time_since_printed >= 10: # Print 'still waiting' type message every 10 seconds
                previous_print_time = time.time()
                print(f"\t{current_time()} Waiting for results (elapsed: {round(time.time() - start_time)})")

        blocks = response['Blocks']
        for block in blocks:
            # Add JobID, file name, and page number to the info for each block
            # and add the block to the combined list of blocks_out, which holds existing info AND this new info
            # (doing json.dumps on this combined list gives us increased confidence that the format will be good)
            block["JobID"] = jobId
            block["FileName"] = file_name
            blocks_out.append(block)

        # dumps for combination of existing file contents (if not overwriting) and new blocks
        json_object = json.dumps(blocks_out, indent=4)
        with open(out_json, "w") as f:
            f.write(json_object)
            
        functs.JSON_FILE_LENGTHS.setdefault(curr_inc, 0)
        functs.JSON_FILE_LENGTHS[curr_inc] += 1
        
        if 'NextToken' in response:
            # Token to be supplied for the next page of the document
            paginationToken = response['NextToken']
            json_object = None
            blocks = []
        else:
            finished = True
            add_processed_json(file_name, folder_name = folder_name)
            return json_object, curr_inc # Return the json object and the curr_inc -- so next calls can use the curr_inc and not have to look so hard for where to start
    

def csv_to_jobs(folder_name, csv_file = None, csv_base_name = None):
    """
    folder_name (str) : The folder to process; if csv_file is not specified, and csv_base_name is not specified, will return contents of all CSV job ID maps with this folder as the basename
    csv_file (str) (optional): Specific CSV to process. If not specified, will do ALL the CSV files associated with this folder (or this csv_base_name, if specified)
    csv_base_name (str) (optional): If csv_file is not specified, will return contents of all CSV job ID maps with this csv_base_name
    """
    import csv
    
    all_csvs = [f for f in os.listdir("CSV_JobIDMaps") if ".csv" in f and "Complete.csv" not in f]
    pref_base = csv_base_name or folder_name
    csv_files = [csv_file] if csv_file else [os.path.join("CSV_JobIDMaps", f) for f in all_csvs if f.startswith(pref_base)]
    
    csv_contents = []
    for csv_path in csv_files:
        if not os.path.exists(csv_path):
            if os.path.exists(os.path.join("CSV_JobIDMaps", csv_path)):
                csv_path = os.path.join("CSV_JobIDMaps", csv_path)
            else:
                raise FileNotFoundError(f"The identified file path {csv_path} does not exist")
        
        with open(csv_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            #if any(len(r) != 2 for r in csv_reader):
            #    print([r for r in csv_reader if len(r) != 2])
            #    raise Exception("Bad rows in csv")
            csv_contents.extend([[r[0], " ".join(r[1:]).replace(',', '')] for r in csv_reader][1:])
    
    return {jobId : docName for jobId, docName in csv_contents}

def process_files(folder_name, csv_file = None, csv_base_name = None, out_json = None, overwrite = False, new_file_cutoff = 20, start_inc = 0, ignore_processed = True, use_analyze = False, max_files = 0):
    """
    folder_name (str) : The folder we are processing
    csv_file (str) : A specific CSV file from which to get the Job ID Map; if not specified, will process ALL files matchin csv_base_name or folder_name
    csv_base_name (str) : Base name of CSV files to process -- if csv_file is specified, not used; if csv_file not specified, will process all CSV job ID maps with this base_name
    out_json (str) (optional) : A specific file name for the output json file; will default to to folder_name plus today's date, if not specified
    overwrite (bool) (default: False) : If True, will delete and recreate the JSON file with specified name; if False, it will replace records in the JSON file for this specific file, but will leave the rest of the file intact
    start_inc (int) (default: 0) : The increment we should attempt to start with when creating the out JSON file name. If this number is known, it should be supplied; otherwise, the process will have to check the contents of every file from 0 until it finds one with space available
    ignore_processed (bool) (default: True) : If False, files will be RE-PROCESSED even if they've already been processed previously
    use_analyze (bool) (default: False) : When true, runs "get_document_analysis" instead of "get_document_text_detection" (significantly more expensive, but may have added benefits at some times)
    """
    start_time2 = time.time()
    curr_inc = start_inc or 0
    
    processed = get_processed_csv(folder_name)
    print(f"Processed files: {len(processed)}")
    
    # Get contents listing job ID to file names, either for a specific csv_file, or for all files using the csv_base_name, or for all files starting with the folder_name
    job_file_map = csv_to_jobs(folder_name, csv_file, csv_base_name)
    print(f"Jobs in job_file_map: {len(job_file_map)}")
    
    # If we're ignoring processed files, makes list from only the files that haven't been processed; if not ignoring files, just converts job_file_map dict to list
    files_to_process = [[j, f] for j, f in job_file_map.items() if f.replace(',', '') not in processed and os.path.join(folder_name, f.replace(',', '')) not in processed] if ignore_processed else [list(x) for x in job_file_map.items()]
    print(f"Files to process: {len(files_to_process)}")
    
    if not files_to_process:
        return "-complete-", curr_inc
    
    max_files = max_files or len(files_to_process)
    j_obj = None 
    for i, (jobId, filename) in enumerate(files_to_process[:max_files]):
        if ignore_processed and (filename.replace(',', '') in processed or os.path.join(folder_name, filename.replace(',', '')) in processed or os.path.basename(filename).replace(',', '') in processed):
            print(f"Already processed {filename}")
            j_obj = None
            continue
        
        print(f"({current_time()}) ({i+1}/{len(files_to_process)}) Processing jobId: {jobId} / File name; {filename}")

        output_filename = "output_file.json"
        output_file_path = os.path.join(folder_name, "JSON_Outputs", output_filename)

        j_obj, curr_inc = process_to_json(jobId, filename, folder_name=folder_name, out_json = output_file_path, overwrite=overwrite, new_file_cutoff = new_file_cutoff, start_inc = curr_inc, use_analyze = use_analyze)

        if j_obj is None:
            return j_obj, curr_inc
        if j_obj != "-broken-": # If there was a problem with the function call, we don't add this file to the list of Processed files
            add_processed_json(filename, folder_name = folder_name)
        
    end_time2 = time.time()
    print("---Part 2 Elapsed %s seconds---\n" % (end_time2 - start_time2))
    
    # Returns the last-used increment, so the next batch can use that as the start_inc and cut down on processing time
    return j_obj, curr_inc

if __name__ == "__main__":
    pass
    