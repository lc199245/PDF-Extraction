####### Message Parsing Script
####### Language: Python
####### Author @Chang Liu @Aleksander Cianciara
####### Date July 12th 2016
####### Message_Manager.py
####### The main logics are here including reading in all the message files, the file type check, msg parsing and output.


#packages imported are:
import pandas as pd;
import textwrap;
from bs4 import BeautifulSoup;
import html;
import re;
from HTMLParser import HTMLParser;

#method for MA Alert txt messages
def MA_message_process(file_path):
	temp_df = pd.read_table(file_path, sep = ':');
	for i in range(0,len(temp_df)):
		temp_df.iloc[i][1] = temp_df.iloc[i][1].strip();
		temp_df.iloc[i][0] = temp_df.iloc[i][0].strip();
	return temp_df;			


#method for Credit Suisse txt messages
def CS_message_process(message_body):
	BODY_WIDTH = 65; 
	#the length for textwrap to know how many characters should contain in a line
	#different message type may have different length

	body = message_body;
	lines = body.split('\n');
	new_body = '';
	for line in lines:
		if line:
			new_body += '\n';
			new_body += '\n'.join(textwrap.wrap(line, BODY_WIDTH));
	
	new_lines = new_body.split('\n');
	total_output = [];
	total_output_index = -1;
	for line in new_lines:
		if '*****' not in line and line:
			if len(re.sub(r'\s+', '', line.split('|')[0])) > 0:
				total_output.append(line)
				total_output_index += 1
			else:
				total_output[total_output_index] += line
	#######
	# Three steps for the process
	# 1. Split the total message body by '\n' and put them all into a list
	# 2. Split each line by a certain deliminater like ':', '|'
	# 3. Other modifications related to certain lines
	# other methods have similar concepts to this
	#######
	result_set = [];
	for i in range(0, len(total_output)):
		total_output[i] = total_output[i].replace('|','');
		temp_str = total_output[i].split(': ',1);
		for j in range(0, len(temp_str)):
			temp_str[j] = temp_str[j].strip();
			temp_str[j] = re.sub(r'\s+',' ',temp_str[j]);
		result_set.append(temp_str);
	result_set_final=[];
	for item in result_set:
		temp_str = item;
		if not ('---' in temp_str[0] or ('From' == temp_str[0]) or ('To' == temp_str[0]) or ('At' == temp_str[0]) or (len(temp_str) < 2)):
			result_set_final.append(temp_str);
	temp_df = pd.DataFrame(result_set_final);
	return temp_df;


#method for 'Borrower' messages
def B_message_process(message_body):
	body = message_body;
	lines = body.split('\n');

	new_body = '';
	for line in lines:
		if line and ('*****' not in line) and ('***' not in line) and ('---' not in line) and (':' in line or 'Margin' in line):
			new_body += '\n';
			new_body += '\n'.join(textwrap.wrap(line, len(line)));
	new_lines = new_body.split('\n');
	new_lines_final=[];
	for item in new_lines:
		temp_item = item;
		if temp_item !='' and '       ' == item[0:7]:
			new_lines_final.append(['',temp_item]);
		if temp_item != '' and '       ' != item[0:7]: 
			temp_str_list = temp_item.split(':',1);
			if len(temp_str_list) != 2:
				temp_str_list.append('');
				new_lines_final.append(temp_str_list)
			else:
				new_lines_final.append(temp_str_list);
	


	# Here we put some logic to check the result sets that we have.
	# Some cases may not be correctly parsed
	# Here is where we make up for those cases

	#1.0 single 'Margin/' line
	Margin_flag = 0;    
	Margin_flag_index = 0;
	#2.0 wrong 'Tenor' position
	Tenor_flag = 0;
	Tenor_flag_index = 0;
	for i in range(0, len(new_lines_final)):
		if new_lines_final[i][0]=='Margin/' and new_lines_final[i][1]=='':
			Margin_flag_index = i;
			Margin_flag = 1;
		if 'Tenor' in new_lines_final[i][1]:
			# print 'Tenor problem!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
			Tenor_flag_index = i;
			Tenor_flag = 1;

		new_lines_final[i][0] = new_lines_final[i][0].strip();
		new_lines_final[i][1] = new_lines_final[i][1].strip();
		new_lines_final[i][1] = re.sub(r'\s+',' ',new_lines_final[i][1]);


    #1.1 fix the Margin problem
	if Margin_flag == 1:
		temp_Margin_str = ''.join([new_lines_final[Margin_flag_index][0], new_lines_final[Margin_flag_index+1][0]]);
		new_lines_final[Margin_flag_index + 1][0] = temp_Margin_str;
		del new_lines_final[Margin_flag_index];

	#2.1 fix the Tenor problem 
	if Tenor_flag == 1:
		temp_Tenor_strlist = new_lines_final[Tenor_flag_index][1].split(':',1);
		temp_Tenor_strlist[0] = temp_Tenor_strlist[0].strip();
		temp_Tenor_strlist[1] = temp_Tenor_strlist[1].strip();
		new_lines_final[Tenor_flag_index] = temp_Tenor_strlist;
	

	temp_df = pd.DataFrame(new_lines_final);
	return temp_df;


#method for 'Company' message
def C_message_process(message_body):
	BODY_WIDTH = 75;
	body = message_body;
	lines = body.split('\n');

	new_body = '';
	for line in lines:
		if line and '*****' not in line :
			new_body += '\n';
			new_body += '\n'.join(textwrap.wrap(line,BODY_WIDTH));
	new_lines = new_body.split('\n');

	new_lines_final=[];
	for item in new_lines:
		temp_item = item;
		if temp_item !='' and '       ' == item[0:7]:
			new_lines_final.append(['',temp_item]);
		if temp_item != '' and '       ' != item[0:7]: 
			temp_str_list = temp_item.split(':',1);
			if len(temp_str_list) != 2:
				temp_str_list.append('');
				new_lines_final.append(temp_str_list)
			else:
				new_lines_final.append(temp_str_list);


	# Here we put some logic to check the result sets that we have.
	# Some cases may not be correctly parsed
	# Here is where we make up for those cases
	
	# 1.0 Bookrunners
	br_flag_index = 0;
	br_flag = 0;

	# 2.0 Business
	bu_flag_index = 0;
	bu_flag = 0;


	for i in range(0, len(new_lines_final)):
		if new_lines_final[i][0] == 'Bookrunners':
			br_flag = 1;
			br_flag_index = i;
		if new_lines_final[i][0] == 'Business':
			bu_flag = 1;
			bu_flag_index = i;

	# 1.1 fix the Bookrunners problem
	if br_flag == 1:
		if new_lines_final[br_flag_index][1][-1] == '/':
			temp_str = ''.join([new_lines_final[br_flag_index][1], new_lines_final[br_flag_index + 1][0]]);
			new_lines_final[br_flag_index][1] = temp_str;
			del new_lines_final[br_flag_index + 1];

	# 2.1 fix the business problem / actually I just want to make it looks more organized
	if bu_flag == 1:
		if new_lines_final[bu_flag_index + 1][0] == 'including':
			temp_str = ''.join([new_lines_final[bu_flag_index][1],' ', new_lines_final[bu_flag_index + 1][0], ':']);
			new_lines_final[bu_flag_index][1] = temp_str;
			del new_lines_final[bu_flag_index + 1];
			


	for i in range(0, len(new_lines_final)):
		new_lines_final[i][0] = new_lines_final[i][0].strip();
		new_lines_final[i][1] = new_lines_final[i][1].strip();
	
	temp_df = pd.DataFrame(new_lines_final);
	return temp_df;			

#method for 'Issuer' messages
def IS_message_process(message_body):
	BODY_WIDTH = 120;
	body = message_body;
	
	lines = body.split('\n');
	
	lines = [item for item in lines if (':   ' in item or '   :' in item or '          ' == item[0:10])]; # im a genius!

	for i in range(0,len(lines)):
		lines[i] = re.sub(r'\s+',' ',lines[i]);

	lines = [item for item in lines if (item != ' ')];

	lines = [item for item in lines if ('---' not in item and '***' not in item)];    # im PRETTY SURE im a genius!

	new_lines_final = [];

	for i in range(0,len(lines)):
		lines[i] = lines[i].replace('?','');
		temp_str_list = lines[i].split(':',1);
		for j in range(0,len(temp_str_list)):
			temp_str_list[j] = temp_str_list[j].strip();

		if len(temp_str_list) < 2:
			
			temp_str_list.insert(0,'');

		new_lines_final.append(temp_str_list);

	new_lines_final = [item for item in new_lines_final if (item != [''])];


	temp_df = pd.DataFrame(new_lines_final);
	return temp_df;			


#method for check whether a line in an html file contains useful information or not
def IS_HTML_Useful(info_item):
	if ('{' in info_item) or (';}' in info_item):
		return False;

	if ('/*' in info_item) or ('*/' in info_item):
		return False;

	if '***' in info_item:
		return False;
	
	if len(info_item) < 4:
		return False;

	if '><' in info_item:
		return False;

	if 'From' == info_item[0:4]:
		return False;

	if '@bloomberg.net' in info_item:
		return False;

	if 'mailto' in info_item:
		return False;

	if 'converted from rtf' in info_item:
		return False;

	if ('Subject:' in info_item) or ('At:' in info_item) or ('To:' in info_item) or ('RE:' in info_item) or ('Re:' in info_item):
		return False; 

	if ('BLOOMBERG/' in info_item) or ('rte-version' in info_item) or ('INC.' in info_item):
		return False;

	if '______' in info_item:
		return False;
	return True;


#method for doing modifications for each line of a html file
#Things like replacing certain characters can be done here
#if we find out we need to do some other modifications, we can always 
#add the statements here instead of changing too much in the engine file(info_extraction.py)	
def HTML_String_manage(temp_html_str):
	return temp_html_str.replace('|','').strip();

#method to return all the plain text content of a html file
def visible(element):
	if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
		return False
	elif re.match('<!--.*-->', str(element)):
		return False
	return True

#method for 'Attached Message:' htm(html) msgs, useful information are hiding in tags div and li and p
#but not for those hiding information in tables(tbody) (small percentage)
def HTML_AM_message_process(file_path):
	
	info_list = [];
	html_file = open(file_path,'rb');
	soup = BeautifulSoup(html_file, 'html.parser')
	texts = soup.findAll(text=True)
	visible_list = filter(visible, texts)

	for item in visible_list:
		# print item;
		if IS_HTML_Useful(item):
			info_list.append(item);


	if len(info_list) == 0:
		print file_path;
		info_list.append('test:test');


	final_info_list = [];

	for i in range(0,len(info_list)):
		if len(info_list[i]) > 5 and len(info_list[i].split(':')) >= 2:
			temp_info = info_list[i].split(':',1);
			temp_info[0] = HTML_String_manage(temp_info[0]);
			temp_info[1] = HTML_String_manage(temp_info[1]);
			final_info_list.append(temp_info);
		if len(info_list[i]) > 5 and len(info_list[i].split(':')) < 2:
			temp_info = ['',HTML_String_manage(info_list[i])];
			final_info_list.append(temp_info);

	temp_df = pd.DataFrame(final_info_list);

	return temp_df;
