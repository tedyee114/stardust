import time
import os
from datetime import datetime
import boto3

def __init__():
    """Makes this an importable module"""
    pass

def current_time():
    return time.strftime("%H:%M:%S", time.localtime())
    
def csv_namer(folder_name, spec_date = None):
    today = spec_date or datetime.now().date().strftime("%Y%m%d")
    
    return os.path.join("CSV_JobIDMaps", f'{folder_name}_{today}.csv'.replace("/","__"))
    
def json_namer(folder_name):
    today = datetime.now().date().strftime("%Y%m%d")
    return os.path.join("JSON_Outputs", folder_name, f'{folder_name}_{today}.json'.replace("/","__"))
    

def export_s3_files(folder_name, bucket, s3_filename = "S3Files.csv"):
    roleArn = 'arn:aws:iam::905418264059:role/TextractRole'
    region_name = 'us-east-1'
    conn = boto3.client('s3')  # Assuming boto.cfg setup, assume AWS S3

    paginator = conn.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=folder_name)
    
    all_files = set()

    for pg, response in enumerate(page_iterator, start = 1):
        print(f"-Page {pg}-")
        for key in response.get('Contents', []):
            document = key['Key']
            if os.path.isdir(document):
                continue
            #if document.count("/") == 1:
            else:
                all_files.add(document)
            
    print(f"Total files: {len(all_files)}")
            
    with open(s3_filename, "w") as csv_file:
        csv_file.write("\n".join(list(all_files)))
        
    return all_files
    
JSON_FILE_LENGTHS = {}