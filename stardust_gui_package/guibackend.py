
import tkinter as tk
from tkinter import PhotoImage, ttk
# Create main window
root = tk.Tk()
root.title("Textract to JSON Portal")
img = PhotoImage(file='C:\\Users\\tedye\\Documents\\COOP\\Portfolio\\Wright Pierce\\cloud9\\logo.PNG')
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
image_path = "C:\\Users\\tedye\\Documents\\COOP\\Portfolio\\Wright Pierce\\cloud9\\Capture.PNG"
image = tk.PhotoImage(file=image_path)
# label = tk.Label(root, image=image)
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