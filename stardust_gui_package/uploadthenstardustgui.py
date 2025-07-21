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
import boto3
import botocore
# import csv                          #allows writing and reading of CSV (comma separated value) files
# import datetime			            #shows start and end time, duration
# import guibackend as gb             #script that makes the GUI
# import functs	                    #script functions to get list of files in s3
#                                         #returns csv of filenames with paths
# import part1_files_to_queue as p1	#script to send files to Textract and return JobIDs
#                                         #returns many CSV of filenames+paths and JobIDs into outputpath
# import part2_textract_to_json as p2	#script to extract JSON data at given JobIDs
#                                         #returns many JSON files into outputpath
import tkinter as tk                #imports the library used to make GUIs
from tkinter import PhotoImage, ttk, messagebox             #imports another piece of that library



class UploadToS3:
    def __init__(self, master):
        self.master = master
        self.master.title("AWS S3 Explorer")
        self.master.configure(bg="#1F252F")


        self.aws_access_key_label = tk.Label(master, text="AWS Access Key ID:", bg="#1F252F", fg="white")
        self.aws_access_key_label.grid(row=0, column=0, padx=10, pady=5, sticky="e")
        self.aws_access_key_entry = tk.Entry(master, width=40)
        self.aws_access_key_entry.grid(row=0, column=1, padx=10, pady=5)

        self.aws_secret_key_label = tk.Label(master, text="AWS Secret Access Key:", bg="#1F252F", fg="white")
        self.aws_secret_key_label.grid(row=1, column=0, padx=10, pady=5, sticky="e")
        self.aws_secret_key_entry = tk.Entry(master, width=40, show="*")
        self.aws_secret_key_entry.grid(row=1, column=1, padx=10, pady=5)

        self.configure_button = tk.Button(master, text="Retrieve Data from AWS", command=self.configure_aws, bg="#FAA12A")
        self.configure_button.grid(row=2, column=1, padx=10, pady=10)

        self.bucket_listbox = tk.Listbox(master, width=50)
        self.bucket_listbox.grid(row=3, column=0, padx=10, pady=10, columnspan=2, sticky="nsew")
        self.bucket_listbox.bind('<<ListboxSelect>>', self.populate_bucket_contents)

        self.file_listbox = tk.Listbox(master, width=50)
        self.file_listbox.grid(row=4, column=0, padx=10, pady=10, columnspan=2, sticky="nsew")

        self.upload_multiple_button = tk.Button(master, text="Upload File(s)", command=self.upload_multiple_files, bg="#FAA12A")
        self.upload_multiple_button.grid(row=5, column=0, padx=10, pady=10)

        self.upload_folder_button = tk.Button(master, text="Upload Folder", command=self.upload_folder, bg="#FAA12A")
        self.upload_folder_button.grid(row=5, column=1, padx=10, pady=10)

        self.delete_button = tk.Button(master, text="Delete Selected", command=self.delete_file, bg="#FAA12A")
        self.delete_button.grid(row=5, column=2, padx=10, pady=10)
        
        self.start_processing_button = tk.Button(master, text="Continue to Processing Screen", command=self.start_processing, bg="#FAA12A")
        self.start_processing_button.grid(row=6, column=1, padx=10, pady=10)

        self.s3 = None

        self.aws_credentials_valid = True  # Variable to track if AWS credentials are valid
        self.startprocessing = False

    def configure_aws(self):
        access_key = self.aws_access_key_entry.get()
        secret_key = self.aws_secret_key_entry.get()
        if access_key and secret_key:
            try:
                self.s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
                self.populate_buckets()
                self.access_key_value = access_key  # Storing access key as an instance variable
                self.secret_key_value = secret_key  # Storing secret key as an instance variable
                self.aws_credentials_valid = True
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'InvalidAccessKeyId':
                    self.aws_credentials_valid = False
                    messagebox.showerror("Error", "Invalid AWS Access Key ID or Secret Access Key.")
                else:
                    messagebox.showerror("Error", f"An error occurred: {e}")
        else:
            messagebox.showerror("Error", "Please enter both AWS Access Key ID and Secret Access Key.")
        
        if self.aws_credentials_valid == True:
            print("Valid Credentials Found, Variable Updated")
        else:
            print("Invalid Credentials Found, Variable NOT Updated")
            
    def populate_buckets(self):
        buckets = self.s3.list_buckets()['Buckets']
        self.bucket_listbox.delete(0, tk.END)
        for bucket in buckets:
            self.bucket_listbox.insert(tk.END, bucket['Name'])

    def populate_bucket_contents(self, event):
        if not self.s3:
            return
        selected_bucket = self.bucket_listbox.get(self.bucket_listbox.curselection())
        self.file_listbox.delete(0, tk.END)
        objects = self.s3.list_objects_v2(Bucket=selected_bucket)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                self.file_listbox.insert(tk.END, obj['Key'])

    def upload_files(self, files):
        selected_bucket = self.bucket_listbox.get(self.bucket_listbox.curselection())
        try:
            for file_path in files:
                file_name = os.path.basename(file_path)
                with open(file_path, 'rb') as f:
                    # Upload each file to the selected bucket
                    self.s3.upload_fileobj(f, selected_bucket, file_name)
            if len(files) == 1:
                messagebox.showinfo("Success", f"File '{file_name}' uploaded successfully.")
            else:
                messagebox.showinfo("Success", f"All files uploaded successfully.")
            self.populate_bucket_contents(event=None)  # Refresh file list after upload
        except Exception as e:
            messagebox.showerror("Error", f"Failed to upload file/files: {e}")

    def upload_single_file(self):
        if not self.s3:
            messagebox.showerror("Error", "AWS not configured. Please configure AWS first.")
            return

        file_path = filedialog.askopenfilename()
        if file_path:
            self.upload_files([file_path])

    def upload_multiple_files(self):
        if not self.s3:
            messagebox.showerror("Error", "AWS not configured. Please configure AWS first.")
            return

        files = filedialog.askopenfilenames()
        if files:
            self.upload_files(files)

    def upload_folder(self):
        if not self.s3:
            messagebox.showerror("Error", "AWS not configured. Please configure AWS first.")
            return

        folder_path = filedialog.askdirectory()
        if folder_path:
            files = []
            for carrot, dirs, filenames in os.walk(folder_path):
                for filename in filenames:
                    files.append(os.path.join(carrot, filename))
            self.upload_files(files)

    def delete_file(self):
        if not self.s3:
            messagebox.showerror("Error", "AWS not configured. Please configure AWS first.")
            return

        selected_bucket = self.bucket_listbox.get(self.bucket_listbox.curselection())
        selected_file = self.file_listbox.get(self.file_listbox.curselection())
        if selected_file:
            try:
                self.s3.delete_object(Bucket=selected_bucket, Key=selected_file)
                messagebox.showinfo("Success", f"File '{selected_file}' deleted successfully.")
                self.populate_bucket_contents(event=None)  # Refresh file list after deletion
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete file: {e}")
                
    def start_processing(self):
        self.startprocessing = True
        print("User has selected to start processing now", self.startprocessing)
        self.master.destroy()  # Destroy the upload window after start processing window
                
    def get_credentials(self):
        print("get_credentials function started")
        return self.startprocessing, self.access_key_value, self.secret_key_value

def fileupload():
    carrot = tk.Tk()
    app = UploadToS3(carrot)
    carrot.mainloop()
    
    startprocessing, access_key, secret_key = app.get_credentials()
    print("First Page User Input: ", startprocessing, access_key, secret_key)
    return startprocessing, access_key, secret_key

def guibackend():
    # Create main window
    root = tk.Tk()
    root.title("Textract to JSON Portal")
    img = PhotoImage(file='C:\\Users\\tedye\\Desktop\\cloud9\\Logo.PNG')
    root.iconphoto(False, img)

    # Set background color
    root.configure(bg="#00A3E0")

    # Set text color
    text_color = "#004976"

    # Output List
    text_entries = []

    # Function to handle button click for all processes
    def whole_process():
        outputs = []
        for entry in text_entries:
            outputs.append(entry.get())
        return outputs

    # Function to handle button click for s3_files_list
    def filelist():
        outputs = []
        for entry in text_entries:
            outputs.append(entry.get())
        print("button clicked")
        return outputs

    # Function to handle button click for p1
    def p1():
        outputs = []
        for entry in text_entries:
            outputs.append(entry.get())
        print(outputs)
        return outputs

    # Function to handle button click for p2
    def p2():
        outputs = []
        for entry in text_entries:
            outputs.append(entry.get())
        return outputs

    # Top left: 3 text entry boxes with default values and titles
    titles1 = ["Please Enter the S3 Bucket Name",
                "Please Enter the Folder Name",
                "Please Enter a base output filename"]
    text_defaults = ["bellinghamtest",
                        "walpole",
                        "walpole_202040605"]
    for i, (title, default_value) in enumerate(zip(titles1, text_defaults)):
        # Add title labels
        title_label = ttk.Label(root, text=title, foreground=text_color)
        title_label.grid(row=i+1, column=0, padx=5, pady=5, sticky="w")

        # Add text entry boxes
        entry = tk.Entry(root, fg=text_color)
        entry.insert(0, default_value)
        entry.grid(row=i+1, column=1, padx=5, pady=5)
        text_entries.append(entry)

    # Middle Column: 5 number entry boxes with titles and default Values
    number_titles = ["Number of Files to Skip (file_num)",
                        "Stop Limit (max_files)",
                        "Batch Size (batch_size)",
                        "New File Cutoff (new_file_cutoff)",
                        "Start Increment (start_inc)"]
    number_defaults = [0, 600, 10, 20, 0]
    for i, (title, default_value) in enumerate(zip(number_titles, number_defaults)):
        title_label = ttk.Label(root, text=title, foreground=text_color)
        title_label.grid(row=i+1, column=2, padx=5, pady=5, sticky="w")
        entry = tk.Entry(root, fg=text_color)
        entry.insert(0, default_value)
        entry.grid(row=i+1, column=3, padx=5, pady=5)
        text_entries.append(entry)

    # Right side: 10 text boxes with titles and default values
    titles2 = ["L (Lead)", "C (Copper)", "G (Galvanized)", "PVC", "HDPE",
                "DI (Ductile Iron)", "CI-L (Lined Cast Iron)",
                "CI-U (Unlined Cast Iron, unless noted as lined)", "B (Brass)",
                "UNK (Unknown)"]
    title_defaults = ["LD", "COPP", 3, 4, 5, 6, 7, 8, 9, 10]
    labels_and_entries = []  # Store labels and entries for alignment
    for i, (title, default_value) in enumerate(zip(titles2, title_defaults)):
        label = ttk.Label(root, text=title, foreground=text_color)
        label.grid(row=i, column=4, padx=5, pady=5, sticky="w")
        entry = tk.Entry(root, fg=text_color)
        entry.insert(0, default_value)
        entry.grid(row=i, column=5, padx=5, pady=5)
        labels_and_entries.append(label)
        labels_and_entries.append(entry)

    # Bottom left: Static text box
    static_text = tk.Text(root, height=5, width=30, fg=text_color)
    static_text.insert(tk.END, '"lead", "ld", "lear", "lero", "owmer":"owner"')
    static_text.grid(row=11, column=0, padx=5, pady=5)

    # Bottom right: Image
    image_path = "C:\\Users\\tedye\\Desktop\\cloud9\\Capture.PNG"
    image = tk.PhotoImage(file=image_path)
    label = tk.Label(root, image=image)
    label.grid(row=11, column=2, columnspan=3, padx=5, pady=5)

    # Submit Buttons
    # Color: #FAA12A
    button = tk.Button(root, text="Run Whole Process (suggested)", command=whole_process, bg="#FAA12A")
    button.grid(row=12, column=0, padx=5, pady=5)

    button = tk.Button(root, text="Run only s3_files_list", command=filelist, bg="#FAA12A")
    button.grid(row=12, column=1, padx=5, pady=5)

    button = tk.Button(root, text="Run only p1", command=p1, bg="#FAA12A")
    button.grid(row=12, column=

    2, padx=5, pady=5)

    button = tk.Button(root, text="Run only p2", command=p2, bg="#FAA12A")
    button.grid(row=12, column=3, padx=5, pady=5)

    root.mainloop()
    
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
    
    
#     preferences = "Start Time: " + str(datetime.datetime.now())
#     preferences += "Bucket: " + str(bucket) + "\n"                               #this whole block just saves all inputted settings as a .txt file (by making for reviewing purposes
#     preferences += "Folder: " + str(folder) + "\n"                                  #it does this by sticking all the settings together as a textstring variable called preferences, which it writes as an output file a the end
#     preferences += "CSV Base Name: " + str(csv_base_name) + "\n"
#     preferences += "File Num: " + str(file_num) + "\n"
#     preferences += "Max Files: " + str(max_files) + "\n"
#     preferences += "Batch Size: " + str(batch_size) + "\n"
#     preferences += "Keywords: " + str(keywords) + "\n"
#     preferences += "Typos: " + str(typos) + "\n"
#     preferences += "New File Cutoff (p2): " + str(new_file_cutoff) + "\n"
#     preferences += "Start Inc (p2): " + str(start_inc) + "\n"
#     os.makedirs(folder, exist_ok=True)
#     f = open(f"{folder}/{folder}_preferences.txt", "a")
#     f.write(preferences)
#     f.close()
    
#     return bucket, folder, csv_base_name, file_num, max_files, batch_size, keywords, typos, new_file_cutoff, start_inc  #this is the output of the function called GUI
  
# def list_files_function(bucket, folder):
#     if not os.path.exists(folder):                                              #create a project folder if one doesn't already exist
#         os.makedirs(folder)
#     subfolder = folder+"/CSV_JobID_Maps"                                        #create a subfolder called "CSV_JobID_Maps" if one doesn't already exist
#     if not os.path.exists(subfolder):
#         os.makedirs(subfolder)
#     s3_filename = f"{folder}/list_of_{folder}_files_in_S3.csv"                  #defines the output location
#     functs.export_s3_files(bucket = bucket, folder_name = folder, s3_filename = s3_filename)    #retrieve a list of files at the specified location and save it ther
#     return s3_filename

# def p1_function(access_key, secret_key, bucket, folder, s3_csv, csv_base_name, max_files = None, file_num = 1, batch_size = 100):
#     """
#     bucket (str) : s3 bucket name
#     folder (str) : s3 folder name -- process files in this folder!
#     s3_csv (str) : List of all files in the specified bucket & folder. This CSV must be an output from list_s3_files.py
#     csv_base_name (str) : Basename for output CSV_JobIDMaps, which will be incremented per batch
#     max_files (str) : TOTAL maximum number of files to process before breaking out of this function
#     file_num (int) (default: 1): Start at this # as increment on the csv_base_name. Recommend using a different file_num 
#     batch_size (int) : Numer of files to include PER FUNCTION CALL (and thus, per output CSV). Can specify None if you just want to do everything as one
    
#     Use this to process all unprocessed files in s3 (up to max_files), using chunks of some # of files at a time (separate CSVs, etc)
#     """
#     max_files_arg = int(max_files) if max_files else None
#     safety = 0
#     total_files_processed = 0
    
#     s3_files = []
#     with open(s3_csv, "r") as s3_file:
#         s3_files = [r.strip() for r in s3_file.read().splitlines()[1:]]
    
#     # Get list of files that have been processed, either in p1 or p2 or both, for this folder
#     processed = p1.get_processed_csv(folder)
    
#     # List of files that haven't been processed yet
#     files_to_process = [file for file in s3_files if file not in processed and os.path.join(folder, file) not in processed]
    
    
#     while True:
#         safety += 1
#         if safety >= 1000: # Just in case the breaks on total_files_processed / max_files prove 
#             print("BROKE ON SAFETY = 1000")
#             break
        
#         # Each run will only include a # of files up to specified batch_size
#         # Returns the # of files that were processed and a list of the files that were processed in this run
#         processed_ct, files_complete = p1.s3_csv_to_queue(folder, bucket=bucket, s3_csv_file = s3_csv, max_files = batch_size or max_files, out_csv_file = f"{folder}/CSV_JobID_Maps/{csv_base_name +'_' if not csv_base_name.endswith('_') else csv_base_name}{file_num}.csv", spec_s3_files=s3_files, spec_files_to_process=files_to_process)
#         files_to_process = [f for f in files_to_process if f not in files_complete and os.path.join(folder, f) not in files_complete] # Remove completed files from the list of 'files_to_process', so they're not supplied to the next run of the function
        
#         # If 0 files were processed in last run, same will happen next time --> Break out of loop
#         if processed_ct == 0:
#             break
        
#         # Update count of how many total files we've processed
#         total_files_processed += processed_ct
#         # If we've now processed all the # of files we want to processed, break out of loop
#         if max_files and total_files_processed >= max_files:
#             break 
        
#         file_num += 1

# def p2_function(folder, csv_base_name, new_file_cutoff = 20, start_inc = 1):
#     """
#     folder (str) : Folder name
#     csv_base_name (str) : csv_base_name used in p1 -- will process all CSV files starting with this string
#     new_file_cutoff (int) (default: 20): Max. number of files to include in a single JSON; low numbers will result in MANY .json files, but will have significantly lower processing time than with larger values
#     start_inc (int) (default: 1): Number from which to start incrementing until we find a .json file with enough space for new files. If numerous JSONs have been output already (ON THIS SAME DATE), set this to the highest incremented # in use
#     """
#     # if not os.path.exists(folder+"JSON_Outputs"):
#     #     os.makedirs(folder+"/JSON_Outputs")
#     csv_files = [f for f in os.listdir("CSV_JobIDMaps") if f.startswith(csv_base_name) and "Complete.csv" not in f]
#     completed = []
#     ct = 0
    
#     for csv_file in csv_files:
#         print(f"\n*** Processing {csv_file} ***")
#         ct += 1
#         if ct >= 10000:
#             break
        
#         #start_inc = 1
        
#         # Update start_inc to whatever was used at the end of this most-recent batch
#         j_obj, start_inc = p2.process_files(
#             folder, 
#             csv_file = csv_file, 
#             new_file_cutoff = 20, 
#             start_inc = start_inc,)
        
#         if j_obj is not None and j_obj != "-broken-":
#             completed.append(csv_file)
#         else:
#             break

#     return completed

if __name__ == "__main__":
				                
    startprocessing, access_key, secret_key = fileupload()
    
    
    # if startprocessing == True:
    #     # start GUI
    guibackend()
    #     bucket, folder, csv_base_name, file_num, max_files, batch_size, keywords, typos, new_file_cutoff, start_inc = GUI()
        
    #     #   when button "run" clicked:
    #     starttime = datetime.datetime.now()
        
    #     s3_csv = list_files_function(bucket, folder)
    #     print("List of all files in given bucket+folder saved to: "+ s3_csv)
    #     print("List Creation Elapsed Time: ", datetime.datetime.now() - starttime)
        
    #     #need to update p1 so that it takes the access and secret keys like below instead of using default profile
    #     # client = boto3.client(
    #     #     'textract',
    #     #     aws_access_key_id=access_key,
    #     #     aws_secret_access_key=secret_key,
    #     #     region_name='us-east-2')
        
        
    #     p1_function(
    #         access_key = access_key,
    #         secret_key = secret_key,
    #         folder = folder,
    #         bucket = bucket,
    #         s3_csv = s3_csv,
    #         csv_base_name = csv_base_name,
    #         file_num = file_num,
    #         max_files = max_files,
    #         batch_size=batch_size)
    #     print("List of all JobIDs saved to: "+ csv_base_name)
    #     print("P1 Elapsed Time: ", datetime.datetime.now() - starttime)
        
        
    #     p2_function(
    #         folder          = folder,
    #         csv_base_name   = csv_base_name,
    #         new_file_cutoff = new_file_cutoff,
    #         start_inc       = start_inc)
    #     print("P2 Elapsed Time: ", datetime.datetime.now() - starttime)
    #     print("DONE###########################################################################################################")