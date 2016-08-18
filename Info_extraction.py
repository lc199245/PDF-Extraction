####### Message Parsing Script
####### Language: Python
####### Author @Chang Liu @Aleksander Cianciara
####### Date July 12th 2016
####### Info_extraction.py
####### The main logics are here including reading in all the message files, the file type check, msg parsing and output.


#######
# import packages pandas, re, glob, time 
#######
import pandas as pd;
import re;
import glob;
import sys;
import time;
from Message_Manager import *;    

#######
# reload system endocing to be 'utf-8'
#######
reload(sys);
sys.setdefaultencoding('utf-8');


#set up the common drive directory of all the useful messages
file_path_txt = 'Z:\Princeton\Global Data\Financial Data\Common\\ntShare\smsg_test\\raw_msgs\\completed\\*';
all_file_list = glob.glob(file_path_txt); #this will return all the files' full path in that directory

print len(all_file_list); #the number of messages we have 

count_total = len(all_file_list);

#counters for each type of txt messages
count_MA = 0;
count_CS = 0;
count_BF = 0;
count_MS = 0;
count_C = 0;
count_B = 0;
count_IS = 0;

##################################################################################
##################################################################################
##################################################################################

#counters for htm messages
count_AM_htm = 0;


#excel writers declared for output 
#######
# we create a single excel workbook for each different kind of messages
# and for each single message, we create a new sheet in that workbook for its output result
#######
writer_MA = pd.ExcelWriter('MA_results.xlsx', engine = 'xlsxwriter');
writer_CS = pd.ExcelWriter('CS_results.xlsx', engine = 'xlsxwriter');
writer_MS = pd.ExcelWriter('MS_results.xlsx', engine = 'xlsxwriter');
writer_B = pd.ExcelWriter('B_results.xlsx', engine = 'xlsxwriter');
writer_C = pd.ExcelWriter('C_results.xlsx', engine = 'xlsxwriter');
writer_IS = pd.ExcelWriter('IS_results.xlsx', engine = 'xlsxwriter');
##################################################################################
##################################################################################
##################################################################################

writer_AM_htm = pd.ExcelWriter('AM_htm_results.xlsx', engine = 'xlsxwriter');


#######
# progress bar variables declared here
#######
progress_bar = 0;
progress_bar_showed = 10;
start_time = time.time();


# we create a list to store the paths of files that did not get processed by our script
rest_list = [];


#######
# the following loop goes through the whole file-path-list
# two steps of the logics:
# 1. check the file type: txt or htm (based on file suffix)
# 2. check the message type (based on the reading, we define some key features for the messages,  
# 	 e.g. MA, CS, MS, IS, AM_htm
#######

for i in range(0, len(all_file_list)):

	file_path_txt = all_file_list[i];
	temp_file = open(file_path_txt,'rb'); 
	temp_body = temp_file.read(); # open each message and get the content


	progress_bar = progress_bar + 1;


	######
	# The variavle msg_flag here is to prevent the case that a message get processed more than one time
	# Because some of the messages may fit more than 1 condtion.
	# For each message, after being processed, i will set the msg_flag to be 1. So it won't get into other branches
	# And in the result file, there will be only 1 sheet for 1 message.
	######
	msg_flag = 0; 

	#progress bar logic, also recording running time
	if((progress_bar%(len(all_file_list)/10)==0)):
		print str(progress_bar_showed) + '%';
		progress_bar_showed = progress_bar_showed + 10;
		print "--- %s seconds ---" % (time.time() - start_time);
		start_time = time.time();

	#check file type, a txt file or a htm(html) file
	if file_path_txt[-3:] == 'txt':
		#Condition for txt MA alert
		if len(temp_body) >= 5 and (temp_body[0] == 'M' and temp_body[1] == 'A') and msg_flag == 0:
			count_MA = count_MA + 1; #increment counter
			sheet_number = file_path_txt[-22:-14]; #use the workitemid as the sheet number in the result excel workbook
		  	temp_df = MA_message_process(file_path_txt);
			temp_df.to_excel(writer_MA,sheet_number, index = False, header=['Field','Value']); #output the result to a sheet in the excel workbook
			msg_flag = 1; # change msg_flag indicating this message has been processed

		#Cresit Suisse txt Message
		if len(temp_body) >= 5 and ('CREDIT SUISSE') in temp_body and msg_flag == 0:
			count_CS = count_CS + 1;
			sheet_number = file_path_txt[-22:-14];
			temp_df = CS_message_process(temp_body);
			temp_df.to_excel(writer_CS,sheet_number,index = False, header=['Field','Value']);
			msg_flag = 1;

		#Morgan Stanley txt Message
		if len(temp_body) >= 5 and 'MORGAN STANLEY' in temp_body and msg_flag == 0:
			count_MS = count_MS + 1;		
			sheet_number = file_path_txt[-22:-14];
			temp_df = CS_message_process(temp_body);
			temp_df.to_excel(writer_MS,sheet_number,index = False, header=['Field','Value']);
			msg_flag = 1;


		#txt messages started with 'Company:' at the beginning
		if len(temp_body) >= 5 and 'Company:' in temp_body and msg_flag == 0:
			count_C = count_C + 1;	
			sheet_number = file_path_txt[-22:-14];
			temp_df = C_message_process(temp_body);
			temp_df.to_excel(writer_C,sheet_number,index=False,header=['Field', 'Value']);
			msg_flag = 1;		

		#txt messages started with 'Borrower:' at the beginning or contains 'Borrower:' at the beginning of a new line
		if len(temp_body) >= 5 and ('Borrower' == temp_body[0:8]  or ('\nBorrower:' in temp_body)) and msg_flag == 0:
			count_B = count_B + 1;	
			sheet_number = file_path_txt[-22:-14];
			temp_df = B_message_process(temp_body);
			temp_df.to_excel(writer_B,sheet_number,index = False, header=['Field','Value']);
			msg_flag = 1;
		
		#txt messages started with 'Issuer:' at the beginning or contains 'Issuer:' at the beginning of a new line
		if len(temp_body) >= 5 and (('Issuer' == temp_body[0:6]) or '\nIssuer' in temp_body ) and msg_flag == 0:
			count_IS = count_IS + 1;
			sheet_number = file_path_txt[-22:-14];
			temp_df = IS_message_process(temp_body);
			temp_df.to_excel(writer_IS,sheet_number,index = False, header=['Field','Value']);
			msg_flag = 1;

	#for htm(html) files		
	if file_path_txt[-3:] == 'htm':
		#######
		# actually for html files, there are only two kinds of messages.
		# 1. The useful infomation are hiding in tags: div, li or p
		#    in another to way say, in pure plain text content of that html file
		# 2. The useful infomation are hiding in tables: tbody, tr, td
		#######
		if (('Debt Syndicate' in temp_body) or ('(Bloomberg)' in temp_body) or ('FINAL PRICE' in temp_body) or ('Final Price' in temp_body) or ('Attached Message:' in temp_body) or ('Borrower:' in temp_body)) and ('<tbody>' not in temp_body) and (msg_flag == 0):
			count_AM_htm = count_AM_htm + 1;
			msg_flag = 1;
			temp_df = HTML_AM_message_process(file_path_txt);
			sheet_number = file_path_txt[-22:-14];
			temp_df.to_excel(writer_AM_htm,sheet_number, index = False, header=['Field','Value']);
			msg_flag = 1;


	if msg_flag == 0:
		rest_list.append(file_path_txt);



#close the file IO stream and save the changes to the result excel workbooks
writer_MA.save();
writer_CS.save();
writer_MS.save();
writer_B.save();
writer_C.save();
writer_IS.save();

##################################################################################
##################################################################################
writer_AM_htm.save();


#######
# This part is for displaying some key infomation of the process
# 1. display the counters for each different kind of messages
# 2. calculate and display percentages of each different kind of messages
# 3. display how many files in total did the script process
# 4. display how many files the script did not process and print out the workitemid list for them
#######		
percent_1 = 100 * float(count_MA) / float(count_total);
percent_2 = 100 * float(count_CS) / float(count_total);
percent_3 = 100 * float(count_MS) / float(count_total);
percent_4 = 100 * float(count_C) / float(count_total);
percent_5 = 100 * float(count_B) / float(count_total);
percent_6 = 100 * float(count_IS) / float(count_total);
##################################################################################
##################################################################################
percent_7 = 100 * float(count_AM_htm) / float(count_total);

percent_total = percent_1 + percent_2 + percent_3 + percent_4 + percent_5 + percent_6 + percent_7 ;


print str(count_MA) + ' messages are MA alerts. ' + str(percent_1) + '%' ;
print str(count_CS) + ' messages are Credit Suisse messages. '+ str(percent_2) + '%';
print str(count_MS) + ' messages are MS messages. '+ str(percent_3) + '%';
print str(count_C) + ' messages are C messages. '+ str(percent_4) + '%';
print str(count_B) + ' messages are B messages. '+ str(percent_5) + '%';
print str(count_IS) + ' messages are Issuer messages. '+ str(percent_6) + '%';
##################################################################################
##################################################################################
print str(count_AM_htm) + ' messages are Attached Message HTML messages. '+ str(percent_7) + '%';
print 'Total Processed: ' + str(percent_total) + '%';

print '----------------------------------------------------------';
print str(len(rest_list)) + ' messages remained unprocessed:';
for i in range(0, len(rest_list)):
	print rest_list[i][-22:-14];
print '----------------------------------------------------------';



