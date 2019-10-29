import ftplib
import os
import csv
from config import ftp_host, ftp_username, ftp_password, ftp_path_to_copy, header_row, detail_row, footer_row
# have to make this file generic so it can connect with any of the FTP server (with or without SSL configured)

def copy_files_from_ftp_to_local(server):
	if not os.path.exists(ftp_path_to_copy):
		os.mkdir(ftp_path_to_copy)

	os.chdir(ftp_path_to_copy)
	# Please let me know what conditions I should check while copying any files from the server???????
	try:
		for filename in server.nlst():
			file = open(filename, 'wb')
			server.retrbinary('RETR '+ filename, file.write)
			file.close()
		server.quit()
		return True
	except:
		print('Some issue occured while copying files from the server')
		return False

def refurbish_file_with_delimiters(path):
	if os.path.exists(path):
		files = os.listdir(path)
		filenames = [file for file in files if '360' in file]
		os.chdir(ftp_path_to_copy)
		for file in filenames:
			with open(file, 'r') as f:
				lines = f.readlines()

			with open(file, 'w') as f:
				for line in range(len(lines)):
					line_data = lines[line]
					sum_header_char = sum(header_row)
					sum_detail_row = sum(detail_row)
					sum_footer_row = sum(footer_row)
					new_string = ''
					counter_list = []
					if (line == 0 and len(line_data) - 1 <= sum_header_char):
						counter_list = header_row
					elif (line > 0 and len(line_data) - 1 <= sum_detail_row and len(line_data) -1 > sum_footer_row):
						counter_list = detail_row
					else:
						counter_list = footer_row
					next_char_start_position = 0
					for elm in counter_list:
						newstr = line_data[next_char_start_position:next_char_start_position + elm]
						if (len(newstr) < elm):
							newstr =  "{:<{}}".format(newstr, elm)
						next_char_start_position = next_char_start_position + elm
						newstr = newstr.replace('\n',' ')
						new_string += newstr + ';'
					f.write(new_string + '\n')
			print("{} refurbished successfully".format(file))


def connect_to_ftp_server(hostName, username, password):
	server = ftplib.FTP()
	server.connect(hostName, 21)
	status = server.login(username,password)

	if 'Logged on' in status:
		flag = copy_files_from_ftp_to_local(server)
		if flag:
			print('Files successfully copied from remote server to local destionation.')
			refurbish_file_with_delimiters(ftp_path_to_copy)
	else:
		print('Please make sure the server is up and running.')

	

if __name__ == "__main__":
	connect_to_ftp_server(ftp_host, ftp_username, ftp_password)

