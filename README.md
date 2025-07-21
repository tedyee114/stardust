##Created by Ted Yee 6/14/2024
##README file for stardust package
Please note that no versions in this package are complete as of 6/14/2024

A portal to upload documents to S3 for processing, then call other proprietary scripts to create a Lead Service Line Inventory

Begin with the Quesition: Is this run on a machine supporting python and tkinter?

Answer 1: This machine supports tkinter and python (most computers)
1a: use uploadthenstardustgui.py for a complete GUI that carries out the below functions
1b: use stardustgui to skip the upload page
2: import functs for use in multiple functions
3: use uploadtos3.py to open the upload page
4: use guibackend.py to create a gui
5: use part1_files-to_queue.py to send all images/PDFs to Textract and retrieve a list of JobIDs (which expire 7 days after creation)
6: use part2_textract_to_json.py to retrieve all data at those JobIDs
7: user parses json using logic from post_processor.py to turn json files into formatted spreadsheet

Answer 2: This machine supports python, but not tkinter (Cloud9 or other virtual machines without monitors)
1: the files in this package will not run, use stardust.py outside of this folder/package

Answer 3: This machine does not support python
1a: download if possible
1b: you cannot run these files