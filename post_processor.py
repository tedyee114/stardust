import json
import re
import pandas as pd
import os
import pyperhelper as pph

def __init__():
	pass

EXPECTED_WORDS = '''
curb
valve
meter
street
avenue
no.
number
saddle
copper
di
badger
capped
lead
owner
location
city
owner's
name
acct.
size
pvc
service
line
adapter
reducer
'''.splitlines()

def missing_letters(from_string, in_string):
	return [from_letter for from_letter in from_string if from_letter.lower() not in in_string.lower() or from_string.lower().count(from_letter.lower()) != in_string.lower().count(from_letter.lower())]

def word_cleanup(string):
	original_string = str(string)

	string = string.lower()
	string = re.sub(r"\b/\b","1",string)
	# It actually is GOOD that this allows replacements that have 0 differences, because it will solve typos/swapped letters/ etc!
	close_match = [e for e in EXPECTED_WORDS if string not in EXPECTED_WORDS and len(missing_letters(string, e)) in (1, 0) and len(missing_letters(e, string)) in (1, 0)]
	
	if close_match:
		print(string, close_match)
		string = string.replace(string, close_match[0])

	return string

def text_cleanup(string):
	return " ".join(word_cleanup(s) for s in string.split())

def fix_one_slashes(string):
	sub = r"(/)(?=(?:[ \-]+)\d(/|-|x|\\)\d)"
	string = re.sub(sub, "1", string)

	sub = r"(^|\s)(/)(?= ?\")"
	string = re.sub(sub, r"\g<1>1", string)

	sub = r"(-|'|\s|\b)314(?=\b)"
	string = re.sub(sub, r"\g<1>3/4", string)

	sub = r'(?<!\d)/(?!\d)'
	string = re.sub(sub, "1", string)

	sub = r"^:?( ?-? ?3/4)"
	string = re.sub(sub, r"1 \1", string)

	return string

def fix_lead(string):
	sub = r'(\b|\s)(lea(s|r|o))(\b|\s)'
	return re.sub(sub, r"\1lead", string, flags=re.IGNORECASE)

def fix_typos(string):
	known = {
		'coppen' : 'copper',
		'coppan' : 'copper',
		'carb' : 'curb',
		'coppea' : 'copper',
		'corren' : 'copper',
		'value' : 'valve',
		'nalue' : 'valve',
		'gatenalve' : 'gate valve',
		'gate vaiuc' : 'gate valve',
		'a dapter' : 'adapter',
		'a idapter' : 'adapter',
	}

	string = pph.regex.dict_replace(string, known)
	string = re.sub(r"(copp[a-zA-Z]+)", "copper", string, flags = re.IGNORECASE)

	return "\n".join(line[0].upper() + line[1:].lower().replace("smh","SMH") for line in string.splitlines())

################################
### TED'S SCRIPTS ##############
################################

#NOTE TO READERS!!!! Begin by collapsing (click the triangle next to the row number) the ASSUMPTIONS section and any row beginning with "def" so you can read the general comments first

#These few lines import libraries which let us use different parts of the computer
import csv                                                                      #this allows us to read, create, and edit csv (comma separated value) files, which can easily become excel sheets
import time                                                                     #this allows us to read the time from the computer's clock in GMT, it's only used in this code to name the output files with their creation time
import re                                                                       #this allows us to compare text strings to certain patterns. Think matching "12/31/1999" to the pattern MM/DD/YYYY

#subfunctions                                                                   #these chunks of code beginnineg with "def" are called functions (note they can be expanded or collapsed by clicking the triangle next to the row number). In the main part of the code, they "are called". Imagine having minions that can only do very limited specific tasks, but by telling them to do their jobs in a certain order, the whole process gets done
def extract_year(date):
	###########################################################
	#Some dates begin with "UNK. MONTH" and it messes with the other code. This fixes them
	pattern = r'UNK\. MONTH/\d{2}/(?P<year>\d{4})$'
	pattern1 = r'UNK\. MONTH/\d{1}/(?P<year>\d{4})$'
	pattern2 = r"([A-Z0-9]+?)/([A-Z0-9]+?)/(?P<year>\d{4})"
	pattern3 = r"([A-Z0-9]+?)/(?P<year>\d{4})"
	
	# Search for the pattern in the string
	match = re.search(pattern, date)
	match1 = re.search(pattern1, date)
	match2 = re.search(pattern2, date)
	match3 = re.search(pattern3, date)

	if match:
		# Extract the matched year
		date = match.groupdict()["year"]
		return date
		
	if match1:
		# Extract the matched year
		date = match1.groupdict()["year"]
		return date
		
	if match2:
		# Extract the matched year
		date = match2.groupdict()["year"]
		return date
		
	if match3:
		# Extract the matched year
		date = match3.groupdict()["year"]
		return date
	###########################################################


	# Find the index of the last '/'
	last_slash_index = date.rfind('/')

	# Check if there's only one '/'
	if '/' not in date:
		# If there's no '/', assume it's a year in YY or YYYY format
		year = date
	else:
		# Extract characters after the last '/'
		year = date.split("/")[-1]
	
	if len(year) == 4 and year.isnumeric():
		return year


	# If the year is YY format, convert it to YYYY
	if len(year) == 2:
		if int(year) > 23:
			year = '19' + year
		else:
			year = '20' + year
		return str(year)
	elif len(date) <=3 or date.count('/') == 1:
		# If not digits and there's only one '/', it's in the format 'string/string'
		date = 0
		return date
	else:
		# If there's more than one '/', it's in the format 'string/string/string'
		return date

def wordend_exception(text, ending, excepted):
	"""
	text (str) : Text to search
	ending (str) : The end-of-word to search for
	excepted (str) : Ignore words ending with this
	"""
	pattern = r'\b\w*{}\b'.format(ending)                                       #defines a template meaning all words ending in [ending]
	matches = re.findall(pattern, text)                                         #creates a list of all words matching the template
	filtered_matches = [word for word in matches if not word.endswith(excepted)]#removes words ending in [excepted]
	return bool(filtered_matches)                                               #If any filtered matches are found, return True; otherwise, return False

def pipe_info(cityowner, row):
	chunk = ''
	data_out = {
		"Material" : "",
		"Lead" : "",
		"Size" : "",
		"Year" : "",
	}
	if cityowner in str(row[6].lower()):                                        #searches entity column (G) for either 'cit' (minimum token for city) or 'own'(minimum token for owner)
		datablock = str(row[8].lower())                                         #makes everything lowercase for searching
		#F searches for material keywords in column I###########################################################

		if datablock:                                                           #checks data box is not empty

			reg_map = {
				"L" : r"\b(lead|lo|ld|lear)\b",
				"C" : r"(\bc\b|\bcu\b|\bk\b|copp|coipen|coffer)",
				"G" : r"(galv|iron)",
				"PVC" : r"(pvc|plastic|\bpl\b|\bpoly\b)",
				"HDPE" : r'\b(hd)?pe\b',
				"DI" : r"(ductile|d\.?i\.?p\.?)",
				"CI" : r"(c\.?i\.?|lined)",
				"B" : r"(brass|\bbr\b|\bb\b)",
			}

			found_materials = [mat for mat, pattern in reg_map.items() if re.search(pattern, datablock)]
			if "G" not in found_materials and re.search(r"(?<!in)g\b", datablock):
				found_materials.append("G")
			
			data_out["Material"] = ";".join(sorted(found_materials))
			chunk += data_out["Material"] + ","
		
		#G searches for 'lead' and and another material keyword in column I#####################################
		if cityowner == 'cit':
			if 'L' in data_out["Material"] and len(data_out["Material"])>2:
				data_out["Lead"] = "MAYBE- OTHER MATERIAL DETECTED WITH LEAD"
			chunk += data_out["Lead"] + ","
				
		#H searches for keywords or quotation marks in column I################################################
		datablock = re.sub(r"(i|l|/)(?= ?\")", "1", datablock)
		
		size = re.findall(r"(3/4|\b\d 1/2|1/2|\b\d ?\")", datablock)
		if size:
			data_out["Size"] = ";".join(size)
		chunk += data_out["Size"] + ","

		#I takes year from input column H#########################################
		data_out["Year"] = extract_year(str(row[7]))
		chunk += data_out["Year"] + ","

	elif 'cit' in str(row[6].lower()):                                 #skip the 3 following private columns if its a public row
		chunk = ",,,"
	elif 'own' in str(row[6].lower()):                                 #skip the 4 preceeding public columns if its a public row
		chunk = ",,,,"

	return chunk

def columnheaders():                                                            #this subfunction simply creates the first row of the csv. it contains excel formulas for easy manual editing
	headers=''                                                                  #creating a blank text string that will be separated by | into the cells of header row. This is done because commas in the excel header formulas are otherwise interpreted as new cell delimiters
	# Column A, INDEX 0
	headers+= 'FILENAME'
	# Column B, INDEX 1
	headers+= '|LINK TO FILE'
	# Column C, INDEX 2
	headers+= '|AUTO-GENERATED FLAGS'
	# Column D, INDEX 3
	headers+= '|=CONCAT("MANUALLY CHECKED?                    ",100*ROUND(COUNTA(D2:D9999)/COUNTA(A2:A9999),4),"%")'
	# Column E, INDEX 4
	headers+= '|LOCATIONAL IDENTIFIER'
	# Column F, INDEX 5
	headers+= '|STREET NUMBER'
	# Column G, INDEX 6
	headers+= '|STREET NAME'
	# Column H, INDEX 7
	headers+= '|DO STREET NAMES MATCH INVENTORY?'
	# Column I, INDEX 8
	headers+= '|LOT NUMBER (IF EXISTS)'
	# Column J, INDEX 9
	headers+= '|WORDS GOOSENECK OR PIGTAIL FOUND IN TEXT?'
	# Column K, INDEX 10
	headers+= '|=CONCAT("GOOSENECK MATERIAL                    ",100*ROUND(((COUNTBLANK(K2:K9999)-COUNTBLANK(A2:A9999))+(COUNTIF(K2:K9999,"*;*")))/COUNTA(A2:A9999),4),"% ARE BLANK OR HAVE SEMICOLON")'
	# Column L, INDEX 11
	headers+= '|=CONCAT("PUBLIC MATERIAL                    ",100*ROUND(((COUNTBLANK(M2:M9999)-COUNTBLANK(A2:A9999))+(COUNTIF(M2:M9999,"*;*")))/COUNTA(A2:A9999),4),"% ARE BLANK OR HAVE SEMICOLON")'
	# Column M, INDEX 12
	headers+= '|WAS PUBLIC SERVICE LINE MATERIAL EVER PREVIOUSLY LEAD?'
	# Column N, INDEX 13
	headers+= '|=CONCAT("PUBLIC SIZE                        ",100*ROUND(((COUNTBLANK(N2:N9999)-COUNTBLANK(A2:A9999))+(COUNTIF(N2:N9999,"*;*")))/COUNTA(A2:A9999),4),"% ARE BLANK OR HAVE SEMICOLON")'
	# Column O, INDEX 14
	headers+= '|=CONCAT("PUBLIC DATE                        ",100*ROUND(((COUNTBLANK(O2:O9999)-COUNTBLANK(A2:A9999))+(COUNTIF(O2:O9999,"*;*")))/COUNTA(A2:A9999),4),"% ARE BLANK OR HAVE SEMICOLON")'
	# Column P, INDEX 15
	headers+= '|=CONCAT("PRIVATE MATERIAL                   ",100*ROUND(((COUNTBLANK(P2:P9999)-COUNTBLANK(A2:A9999))+(COUNTIF(P2:P9999,"*;*")))/COUNTA(A2:A9999),4),"% ARE BLANK OR HAVE SEMICOLON")'
	# Column Q, INDEX 16
	headers+= '|=CONCAT("PRIVATE SIZE                       ",100*ROUND(((COUNTBLANK(Q2:Q9999)-COUNTBLANK(A2:A9999))+(COUNTIF(Q2:Q9999,"*;*")))/COUNTA(A2:A9999),4),"% ARE BLANK OR HAVE SEMICOLON")'
	# Column R, INDEX 17
	headers+= '|=CONCAT("PRIVATE DATE                       ",100*ROUND(((COUNTBLANK(R2:R9999)-COUNTBLANK(A2:A9999))+(COUNTIF(R2:R9999,"*;*")))/COUNTA(A2:A9999),4),"% ARE BLANK OR HAVE SEMICOLON")'
	# Column S, INDEX 18
	headers+= '|BIGGEST ISSUE NOTED (USE DATA VALIDATION FOR SORTING)'
	# Column T, INDEX 19
	headers+= '|NOTES'
	# Column U, INDEX 20
	headers+= '|NOTES2'
	# Column V, INDEX 21
	headers+= '|=CONCAT("DO MATERIALS MATCH                 ",100*ROUND(COUNTIF(W2:W9999, FALSE),4),"% ARE FALSE (insert equations and drag down")'
	# Column W, INDEX 22
	headers+= '|=CONCAT("DO SIZES MATCH                     ",100*ROUND(COUNTIF(X2:X9999, FALSE),4),"% ARE FALSE (insert equations and drag down")'
	# Column X, INDEX 23
	headers+= '|=CONCAT("DO DATES MATCH                     ",100*ROUND(COUNTIF(V2:V9999, FALSE),4),"% ARE >1YR APART (insert equations and drag down")'
	# Column Y, INDEX 24
	headers+= '|COMBINED MIN CONFIDENCE'
	# Column Z, INDEX 25
	headers+= '|COMBINED AVG CONFIDENCE'
	# Column AA, INDEX 26
	headers+= '|COMBINED MAX CONFIDENCE'
	# Raw data begins in Column AB, INDEX 27
	oldheaders = '|SOURCE|FILE|PAGE|ADDRESS|SERVICE NUMBER|METER NUMBER|ENTITY|DATE|DATA|LEAD?|MIN CONFIDENCE| AVG CONFIDENCE|MAX CONFIDENCE|||'
	# Input headers repeat up  5 times for when there are multiple  pieces of raw data per file
	headers += oldheaders*5 +'\n'                                           
	
	return headers

def newfile(raw_data, row, previous_row, bufferinfo, totalmin, totalavg, totalmax):
	if len(totalmin)>1:                                                         #calculates average confidence scores amongst data pieces combined by address
		totalmin = sum(totalmin)/len(totalmin)
	if len(totalavg)>1:
			totalavg = sum(totalavg)/len(totalavg)
	if len(totalmax)>1:
		totalmax = sum(totalmax)/len(totalmax)

	#previous row's bufferinfo is committed only when a new address is found and the previous one has no more rows, confidence scores and raw_data are also appended to the string that constitutes the row in the array variable (also a string, just with newline delimiters)
	bufferinfo.append(totalmin)
	bufferinfo.append(totalavg)
	bufferinfo.append(totalmax)
	rawdatalist = raw_data.split(",")
	bufferinfo.extend(rawdatalist)
	
	with open(output_file, "a", newline='') as fout:                                   #takes the textstring called "array" and writes it to the output file using commas to split it into cells
			writer = csv.writer(fout)
			writer.writerow(bufferinfo)  # Splitting not needed herefout.writerow(bufferinfo)
			
	
	raw_data 	= ''                                                            #clears the textstring variable called raw_data that is a buffer for the raw data only
	bufferinfo 	= ["","","","","","","","","","","","","","","","","","","","","","","",""]                                                             #clears the list that remembers the most recently extracted info
	totalmin 	= []                                                            #clears the lists that remember the confidence scores for averaging at the end of the file
	totalavg 	= []
	totalmax 	= []

	if row[1]:                                                         
		bufferinfo[0]= row[1]         						                    #Column A will be filename (original Column B/index 1) if it existed
		bufferinfo[1]= '=HYPERLINK(CONCAT("filepath to folder",A3),A3)'         #Column B will be an excel sheet fomula that creates a hyperlink to the file                                                
	if row[3]:			
		bufferinfo[4]= row[3]							                        #Column E will be address (original Column D/index 3) if exists
		bufferinfo[5]= '=LEFT(E3, FIND(" ", E3) - 1)'					        #Column F will be street number (anything found before the first space) (these can be changed if files are organized into folder by street)
		bufferinfo[6]= '=RIGHT(E3, LEN(E3) - FIND(" ", E3))'					#Column G will be street name   (anything found after the first space) (these can be changed if files are organized into folder by street)
		bufferinfo[7]= '=VLOOKUP(E3,"list of streetnames",1,FALSE)'     		#Column H will be lookup the extracted streetname to see if it makes sense
	else:
		bufferinfo[4]="No Address found in JSON"
	if row[8]:
		if "goose" in str(row[8].lower()) or "pig" in str(row[8].lower()):      #Column J searches for keywords "goose" and "pig" in original data (Column I/index 8)
			bufferinfo[9]= "Y"
	if row:
		chunk = ''                                                              #creates empty string that will be completed with info about the house
		chunk += pipe_info("cit", row)                                          #calls the extractor subfunction and creates a textstring variable out of the data
		chunk += pipe_info("own", row)                                          #Columns P,Q,R to be filled out with private data or be blank
		if 'cit' not in str(row[6].lower()) and 'own' not in str(row[6].lower()):   #ERROR MESSAGE for when Textract did not pick it up as owner ofr city
			chunk += "NEITHER CITY NOR OWNER VALUE DETECTED"
		chunklist = chunk.split(",")
		bufferinfo [11] = chunklist [0]                                         #Columns L,M,N,O,P,Q,R to be filled out with private data or be blank
		bufferinfo [12] = chunklist [1]
		bufferinfo [13] = chunklist [2]
		bufferinfo [14] = chunklist [3]
		bufferinfo [15] = chunklist [4]
		bufferinfo [16] = chunklist [5]
		bufferinfo [17] = chunklist [6]
																				#Columns S,T,U to be left blank for manual entry
		bufferinfo[21] = '=L3=R3'                                               #Column V to be excel formula to help check if sizes match
		bufferinfo[22] = '=M3=Q3'                                               #Column V to be excel formula to help check if sizes match
		bufferinfo[23] = '=N3=R3'                                               #Column V to be excel formula to help check if sizes match


	return raw_data, bufferinfo, totalmin, totalavg, totalmax

def samefile(raw_data, row, previous_row, bufferinfo, totalmin, totalavg, totalmax):
	chunk = ''                                                                              #creates empty string that will be completed with info about the house

	if 'cit' in str(row[6].lower()):                                                        #determines public or private side by searching entity column (G) for  'cit'
		chunklist = pipe_info("cit", row).split(',')                                        #extracts information into a 4 element list (initially textstring, then split)
		# print(chunklist)
		
		if chunklist[2] > bufferinfo[14] or bufferinfo[14] == "":                           #if the extracted year is the first public found or newer than the previous, use this section
			print('1 NEWER PRIVATE INFO FOUND')                                               
			for i in range(3):
				if "UNK" in str(chunklist[0+i].lower()) or chunklist[0+i] == "":            #if newer material is 'UNK' or blank, skip instead of overwriting
					pass
				else:
					bufferinfo[11+i] = chunklist[0+i]
					
		if chunklist[2] == bufferinfo[14] or int(chunklist[2].isnumeric()) == False:        #if an equal private YEAR (month and day ignored) is found, concatenate
			print('2 EQUAL YEAR PRIVATE INFO FOUND')
			for i in range(3):
				if "UNK" in str(chunklist[0+i].lower()) or chunklist[0+i] == "":            #if newer info is 'UNK' or blank, skip instead of overwriting
					pass
				else:
					if "UNK" in str(bufferinfo[11+i].lower()) or bufferinfo[11+i] == "":    #if older info  material is 'UNK' or blank, overwrite it
						bufferinfo[11+i] = chunklist[0+i]
					else:
						bufferinfo[11+i] = "&" + chunklist[0+i]                             #if neither are blank, concatenate them with & symbol between
						if str(chunklist[0+1]) == "City No Date":
							bufferinfo[17] = ""

	if 'own' in str(row[6].lower()):                                                        #determines public or private side by searching entity column (G) for  'own'
		chunklist = pipe_info("own", row).split(',')                                        #extracts information into a 4 element list (initially textstring, then split)
		# print(chunklist)
		
		if chunklist[2] > bufferinfo[14] or bufferinfo[14] == "":                           #if a newer private YEAR (month and day ignored) is found
			print('3 NEWER PUBLIC INFO  FOUND')
			for i in range(3):
				if "UNK" in str(chunklist[0+i].lower()) or chunklist[0+i] == "":            #if newer material is 'UNK' or blank, skip instead of overwriting
					pass
				else:
					bufferinfo[15+i] = chunklist[0+i]

		if chunklist[2] == bufferinfo[14] or int(chunklist[2].isnumeric()) == False:        #if an equal private YEAR (month and day ignored) is found, concatenate
			print('4 EQUAL YEAR PRIVATE INFO FOUND')
			for i in range(3):
				if "UNK" in str(chunklist[0+i].lower()) or chunklist[0+i] == "":            #if newer info is 'UNK' or blank, skip instead of overwriting
					pass
				else:
					if "UNK" in str(bufferinfo[15+i].lower()) or bufferinfo[15+i] == "":    #if older info  material is 'UNK' or blank, overwrite it
						bufferinfo[15+i] = chunklist[0+i]
					else:
						bufferinfo[15+i] = "&" + chunklist[0+i]                             #if neither are blank, concatenate them with & symbol between
						if str(chunklist[0+1]) == "Owner No Date":
							bufferinfo[17] = ""
					
	if 'cit' not in str(row[6].lower()) and 'own' not in str(row[6].lower()):               #ERROR MESSAGE FOR WHEN NOTHING FOUND BY pipe_info() subfunction above
		bufferinfo += "NEITHER CITY NOR OWNER VALUE DETECTED"

	return raw_data, bufferinfo, totalmin, totalavg, totalmax


#the main function. this one is the heavy lifter, it's the one that calls all of the functions that I grouped together and named 'subfunctions'. it's like the one that tells the minions what to do in what order (see note about subfunctions above)
def process_csv(input_file, output_file):
	with open(input_file, 'r') as csv_input, open(output_file, "at", newline='') as csv_output:
		reader = csv.reader(csv_input, quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL, skipinitialspace=True)

		headerstring = columnheaders()                                          #creates a text string variable and sets it equal to the output of the subfunction named "columnheaders"
		with open(output_file, "at", newline='') as csv_output:                 #tells the code to append text ("at") to the output file
			writer = csv.writer(csv_output)                                     #creates a command called writer that means "write to the csv"
			writer.writerow(headerstring.split('|'))                            #adds the textstring called headerstring to the csv, splitting cells by | symbol

		# array = []                                                          #creates empty textstring that holds all the information until commit to csv
		previous_row = ''                                                       #creates empty textstring memory of the last set of input data processed
		raw_data = ''                                                           #creates empty textstring memory of the last set of raw data processed
		bufferinfo 	= ["0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30"]                                                            #clears the list that remembers the most recently extracted info                     #creates nonempty textstring buffer (memory of the current extracted information), which is only commited after all rows of input data from the current address have been processed
		raw_data = ''                                                           #creates empty textstring to slowly accumulate the raw data values which are appended to the right side of the extracted info
		totalmin = []                                                           #creates empty list of confidence scores to be calculated into an average later
		totalavg = []                                                           #creates empty list of confidence scores to be calculated into an average later
		totalmax = []                                                           #creates empty list of confidence scores to be calculated into an average later
		
		firstline = True                                                        #used to skip the first line (headers), see comment after next
		for row in reader:                                                      #For every row in input csv
			if firstline:                                                       #after skipping the line the first time, it changes the value to false causing it to ignore this block and not skip the rest of the rows
				firstline = False
				continue

			if row:                                                             #check that the row is not empty
				if previous_row and previous_row[1] != row[1]:                  #if the input filename value IS NOT the same as the last one, use the newfile subfunction to assign values to the variables at the start of the line
				   raw_data, bufferinfo, totalmin, totalavg, totalmax = newfile(raw_data, row, previous_row, bufferinfo, totalmin, totalavg, totalmax) 
				if previous_row and previous_row[1] == row[1]:                  #if the input filename value IS the same as the last one, use the newfile subfunction to assign values to the variables at the start of the line
				   raw_data, bufferinfo, totalmin, totalavg, totalmax = samefile(raw_data, row, previous_row, bufferinfo, totalmin, totalavg, totalmax) 
				
				raw_data +=str(row[0:13])+ ',,,'                                #adds all raw data into a textstring which is placed in the columns to the right once the last info is extracted from a file
				if row[10]:                                                     #if confidence scores are found: make it a string, remove brackets, apostrophes and spaces, make it a number, and put it in the storage list
					totalmin.append(float(str(row[10]).strip("' ][")))
				if row[11]:
					totalavg.append(float(str(row[11]).strip("' ][")))
				if row[12]:
					totalmax.append(float(str(row[12]).strip("' ][")))
				previous_row = row                                              #Update 'previous row' index for next comparison
		

		with open(output_file, "a", newline='') as fout:                                   #takes the textstring called "array" and writes it to the output file using commas to split it into cells
			writer = csv.writer(fout)
			writer.writerow(bufferinfo)  # Splitting not needed herefout.writerow(bufferinfo)


#technically, this is the one that controls all the minions (note how it doesn't have a "def" at it's beginning), however, it only has 4 lines: defining filepaths, telling proces_csv to do it's job, and printing done. it might seem redundant to have a person whose only job is to tell the next person to go, but it's common practice to have it all kicked off by a master starter like this
if __name__ == "__main__":                                                      
	input_file = 'excel_processor/ATTEMPT3.csv'
	output_file = "excel_processor/finalform-" + time.strftime("%Y_%m_%d-%I_%M_%S_%p") + ".csv"
	process_csv(input_file, output_file)
	print("DoNe")