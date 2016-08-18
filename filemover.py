####### File Mover Script
####### Language: Python
####### Author @Chang Liu @Aleksander Cianciara
####### Date July 12th 2016
####### filemover.py
####### This script is used for moving the messages.
####### Because there are unrelated and invalid messages in the raw messages folder, we move the messages containing useful 
####### 	information to the completed folder and messages without useful information to the junk folder.


# import packages os and shutil
import os
import shutil

#######
# here we set the source directory and the destination directory of the move
#######
source_dir = "Z:\Princeton\Global Data\Financial Data\Common\\ntShare\smsg_test\\raw_msgs\\"
dest_dir =   "Z:\Princeton\Global Data\Financial Data\Common\\ntShare\smsg_test\\raw_msgs\\completed"


# counter for files moved successfully
count = 0
for top, dirs, files in os.walk(source_dir):   #walk through all files in the source directory
    # top is the parent directory 
    # dirs saved all the sub folders in this directory
    for filename in files:    # the following is a sample code for moving Issuer txt files into the completed folder
        if not filename.endswith('.txt'):
            continue
        file_path = os.path.join(top, filename)
        with open(file_path, 'r') as f:
	    if f.read()[0:6] == "Issuer":     #here is the condition we set for different message types
                count += 1
                f.close()
                shutil.move(file_path, os.path.join(dest_dir, filename))
print(str(count) + ' ' + 'files moved successfully')












