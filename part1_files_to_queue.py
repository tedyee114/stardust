import boto3
import botocore
import time
import os
from datetime import datetime
from functs import current_time, csv_namer
import csv
import random

def __init__():
    """Makes this an importable module"""
    pass

# Create a session that grabs the Access Key & Secret Access Key, so it doesn't time out
# session = boto3.Session(profile_name='ducky2')

class ExpiredTokenError(Exception):
    def __init__(self, message):
        super().__init__(message)


def get_processed_csv(folder_name):
    """Returns a list of files that have been file_named in [folder_name]/_CSV_JobID_Maps_Part1Complete.csv"""
    import csv
    
    csv_path = f"{folder_name}/CSV_JobID_Maps/{folder_name}_Part1Complete.csv"
    
    done =[]
    if not os.path.exists(csv_path):
        done.extend([])
    else:
        with open(csv_path) as csv_contents:
            done.extend([r.strip() for r in csv_contents.read().splitlines()])
    
    # If file has been processed in p2 but somehow not documented as processed in p1, we still don't need to rerun it for p1
    csv_path = f"JSON_Outputs/_{folder_name}_Part2Complete.csv"
    if not os.path.exists(csv_path):
        done.extend([])
    else:
        with open(csv_path) as csv_contents:
            done.extend([r.strip().replace(",", "") for r in csv_contents.read().splitlines()])
    return done

def add_processed_csv(file_name, job_id, folder_name, job_map_csv = None):
    """Add file_name to the CSV file at {folder_name}/CSV_JobID_Maps_Part1Complete.csv"""
    if not os.path.exists(f"{folder_name}/CSV_JobID_Maps/{folder_name}_Part1Complete.csv"):
        with open(f"{folder_name}/CSV_JobID_Maps/{folder_name}_Part1Complete.csv", "w") as file:
            file.write("File Path")
            
    with open(f"{folder_name}/CSV_JobID_Maps/{folder_name}_Part1Complete.csv", "a") as file:
        file.write("\n" + file_name)
        
    csv_path = job_map_csv or csv_namer(folder_name)
    if not csv_path.endswith(".csv"):
        csv_path += ".csv"
        
    if "CSV_JobID_Maps" not in csv_path:
        csv_path = os.path.join("CSV_JobID_Maps", csv_path)
    
    if not os.path.exists(csv_path):
        with open(csv_path, "w") as file:
            file.write("JobId,SourceFile")
            
    with open(csv_path, "a") as file:
        file.write(f"\n{job_id},{file_name.replace(',', '')}")

def jobs_to_csv(job_map, folder_name, csv_file = None):
    """
    job_map (dict) : Dictionary of JobID : File Name
    folder_name (str) : The folder that was processed to create the job_map. This will be used in naming the output csv file ([folder_name]_[today's date])
    csv_file (str) (optional) : Supply if we want to use a specific output csv file, rather than creating a name from the folder_name and today's date
    """
    
    csv_path = csv_file or csv_namer(folder_name)
    if not csv_path.endswith(".csv"):
        csv_path += ".csv"
        
    if "CSV_JobID_Maps" not in csv_path:
        csv_path = os.path.join("CSV_JobID_Maps", csv_path)
    
    with open(csv_path, "w") as file:
        file.write("JobId,SourceFile")
        file.write("\n" + "\n".join(f"{k},{v.replace(',', '')}" for k, v in job_map.items()))


class DocumentProcessor:
    sqsQueueUrl = ''
    snsTopicArn = ''
    processType = ''

    def __init__(self, role, bucket, file_name, region, folder_name):
        self.roleArn = role
        self.bucket = bucket
        self.file_name = file_name
        self.region_name = region
        self.jobId = None
        self.folder_name = folder_name
        

        self.textract = session.client('textract', region_name=self.region_name)
        self.sqs = self.get_sqs_client()
        self.sns = self.get_sns_client()

    def get_sns_client(self):
        return session.client('sns', region_name=self.region_name)
    
    def get_sqs_client(self):
        return session.client('sqs', region_name=self.region_name)


    def ProcessDocument(self, out_csv_file = None):
        """
        Start the textract file_name text detection and set own jobId to the JobId assigned therein
        
        Also returns the JobId as result of the function
        """
        self.sqs = self.get_sqs_client()
        self.sns = self.get_sns_client()

        jobFound = False

        response = self.textract.start_document_text_detection(
            DocumentLocation={
                'S3Object': 
                    {
                        'Bucket': self.bucket, 
                        'Name': self.file_name
                    }
                },
            NotificationChannel={
                'RoleArn': self.roleArn, 
                'SNSTopicArn': self.snsTopicArn
            }
        )
        
        
        self.jobId = response['JobId']  # Store JobId
        add_processed_csv(self.file_name, job_id = self.jobId, folder_name = self.folder_name, job_map_csv = out_csv_file)

        return response['JobId']

    def CreateTopicandQueue(self):
        millis = str(int(round(time.time() * 1000))) # Time at start of processing, in miliseconds

        # Create SNS topic
        snsTopicName = "AmazonTextractTopic" + millis

        try:
            self.sns = self.get_sns_client()
            topicResponse = self.sns.create_topic(Name=snsTopicName)
            self.snsTopicArn = topicResponse['TopicArn']
    
            # create SQS queue
            sqsQueueName = "AmazonTextractQueue" + millis
            self.sqs = self.get_sqs_client()
        
        
            self.sqs.create_queue(QueueName=sqsQueueName)
            self.sqsQueueUrl = self.sqs.get_queue_url(QueueName=sqsQueueName)['QueueUrl']
            attribs = self.sqs.get_queue_attributes(QueueUrl=self.sqsQueueUrl,
                                                AttributeNames=['QueueArn'])['Attributes']

            sqsQueueArn = attribs['QueueArn']
    
            # Subscribe SQS queue to SNS topic
            self.sns.subscribe(TopicArn=self.snsTopicArn, Protocol='sqs', Endpoint=sqsQueueArn)
    
            # Authorize SNS to write SQS queue
            policy = """{{
          "Version":"2012-10-17",
          "Statement":[
            {{
              "Sid":"MyPolicy",
              "Effect":"Allow",
              "Principal" : {{"AWS" : "*"}},
              "Action":"SQS:SendMessage",
              "Resource": "{}",
              "Condition":{{
                "ArnEquals":{{
                  "aws:SourceArn": "{}"
                }}
              }}
            }}
          ]
        }}""".format(sqsQueueArn, self.snsTopicArn)
    
            response = self.sqs.set_queue_attributes(
                QueueUrl=self.sqsQueueUrl,
                Attributes={
                    'Policy': policy
                })
        except Exception as e:
            raise ExpiredTokenError(f"Security token has expired. {e}")


def s3_csv_to_queue(folder_name, bucket, s3_csv_file, out_csv_file = None, max_files = None, spec_s3_files = None, spec_files_to_process = None) -> int:
    """
    folder_name (str) : The folder to process
    bucket (str) : The bucket containing the folder-to-process
    s3_csv_file (str) : The CSV file listing all files in the folder-to-process -- this is an output from functs.export_s3_files()
    out_csv_file (str) (optional) : A specific csv file path to use for the output, instead of one formed from the folder name and today's date
    max_files (int) (optional) : Stop processing after the first [max_files] files
    spec_s3_files (list) (optional) : Supplied list of all files in this folder (result from list_s3_files.py). REQUIRED WHEN spec_files_to_prcess is supplied
    spec_files_to_process (list) (optional) : Specify files that still need to be processed. If this is NOT specified, the function will read the s3_csv_file and then run function get_processed_csv(). If supplied, can skip that processing (may help with memory issues, but runs the risk of processing the same file multiple times)
    
    Create an analyzer and call ProcessDocument() for each file in the specified folder_name
    
    Returns the number of files that were processed
    """
    start_time1 = time.time()
    
    roleArn = 'arn:aws:iam::905418264059:role/TextractRole'
    region_name = 'us-east-1'

    if spec_files_to_process and not spec_s3_files:
        raise ValueError("spec_s3_files is required when spec_files_to_process is supplied")

    job_file_map = {}
    
    already_processed_ct = 0
    
    processed_ct = 0
    completed = [] # Files that have been processed in this run
    s3_files = []
    if not spec_files_to_process:
        # Get list of files in s3 (from output of list_s3_files.py)
        with open(s3_csv_file, "r") as s3_file:
            s3_files = [r.strip().replace(',', '') for r in s3_file.read().splitlines()[1:]]
        
        # Get list of files that have been processed, either in p1 or p2 or both, for this folder
        processed = get_processed_csv(folder_name)
        
        # List of files that haven't been processed yet
        files_to_process = [file for file in s3_files if file.replace(',', '') not in processed and os.path.join(folder_name, file.replace(',', '')) not in processed]
    else:
        files_to_process = spec_files_to_process
        processed = [f for f in spec_s3_files if f.replace(',', '') not in files_to_process and os.path.join(folder_name, f.replace(',', '')) not in files_to_process]
        
    
    
    # If max_files was not specified, set it to the the # of files there are to be processed
    max_files = int(max_files) if max_files else len(files_to_process)
    
    # Print how many TOTAL files remain to be processed, vs how many have been processed already
    print(f"*** FILES TO PROCESS: {len(files_to_process)} (vs {len([file for file in s3_files if file.replace(',', '') in processed or os.path.join(folder_name, file.replace(',', '')) in processed])} already processed)")
    
    # Will run on all files_to_process, starting from the first item up to either the specified max_files or ... all the files
    for i, file_name in enumerate(files_to_process[:max_files+1]):
        
        # If we've already done more files than we're planning to do... stop it!
        if max_files and i > max_files:
            break
        
        # If this file has already been processed, skip it (shouldn't happen, but maybe could if duplicates or something?)
        if file_name in processed or any(file_name.replace(',', '') in fname.replace(',', '') for fname in processed):
            already_processed_ct += 1
            print(f"Files already processed: {already_processed_ct}")
            continue
        
        # Don't try to process folders
        if file_name.endswith("/"):
            continue
        
        # Count how many files we've processed in this run
        processed_ct += 1
        # If this file would push us ABOVE the max number of files to process ... quit
        if processed_ct > max_files:
            continue
        
        # Create the DocumentProcessor to start the processing
        analyzer = DocumentProcessor(role=roleArn, bucket=bucket, file_name=file_name, region=region_name, folder_name=folder_name)
        try:
            analyzer.CreateTopicandQueue()
        except ExpiredTokenError:
            print(f"ExpiredTokenError. Processed up through item # {processed_ct - 1}")
        
        try:
            job_id = analyzer.ProcessDocument(out_csv_file)
            job_file_map[job_id] = file_name # Add job_id to dictionary with value = file_name name 
            completed.append(file_name)
            # Print a status update w/ timestamp every 10 files
            if processed_ct % 10 == 0:
                print(f'({current_time()}) ({processed_ct}/{max_files}) Processing Job Id {job_id} complete')
                
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'Badfile_nameException':
                print(f"Badfile_nameException (Bad file_name): {file_name}")
            else:
                print(f"Error processing file_name: {file_name}, Error: {e}")
                
        
    end_time1 = time.time()
    print("---Part 1 Elapsed %s seconds---" % (end_time1 - start_time1))
    return processed_ct, completed # Return the # of files we processed
if __name__ == "__main__":
    pass
    