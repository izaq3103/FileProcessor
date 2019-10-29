import csv
import os
import config
import Mysql_Connector


def get_total_count(f):
	row_count = sum(1 for row in f)
	return row_count

def check_whether_file_is_not_broken(list_first_last_row, row_count):
	check_flag = False
	if (list_first_last_row != [] and len(list_first_last_row) == 2):
		if ((list_first_last_row[0][1] + list_first_last_row[0][2] + list_first_last_row[0][3]) == (list_first_last_row[1][1] + list_first_last_row[1][2] + list_first_last_row[1][3]) and (row_count-len(list_first_last_row) == abs(int(list_first_last_row[1][4])))):
			check_flag = True
	return check_flag

def create_values_to_insert(out_str):
	global insert_string
	if (len(out_str) > 0):
		insert_string = '('
		loop_range = config.range_of_list if len(out_str) < config.range_of_list else len(out_str)
		for row in range(loop_range - 1):
			if row == 0:
				if '"' in out_str[row]:
					out_str[row] = out_str[row].replace('"','')
			try:
				insert_string = insert_string + str(out_str[row]) + ','
			except IndexError:
				newstr =  "'{:<{}}'".format(' ', config.detail_row_350[row])
				insert_string = insert_string + newstr + ','
				# insert_string = insert_string + "' '" + ','
			insert_string = insert_string.replace('[', '').replace(']','')
		insert_string = insert_string[:-1] + '),'
		# print(insert_string)
	return insert_string


def process_file(list_rest_rows):
	# print(len(list_rest_rows))
	if len(list_rest_rows) > 0:
		values_string = ''
		for row in list_rest_rows:
			if (len(row) > 11):
				out_str = []
				out_str = str(row).split(',')
				insert_string = create_values_to_insert(out_str)
				values_string += insert_string
		values_string = values_string[:-1]
	return values_string


def list_360_files(path):
	if os.path.exists(path):
		files = os.listdir(path)
		for file in files:
			with open(os.path.join(path, file), 'r') as f:
				row_count = get_total_count(f)
				with open(os.path.join(path, file), 'r') as fn:
					data = csv.reader(fn, delimiter = ';', quotechar=',')
					list_first_last_row = []
					list_rest_rows = []
					for row in data:
						if (len(row) == 11) or (len(row) == 6): #this should be handled dynamically, check also whether it is first and last row.
							list_first_last_row.append(row)
						else:
							list_rest_rows.append(row)
					if check_whether_file_is_not_broken(list_first_last_row, row_count):
						# database logic here to check whether file already processed or not
						if not Mysql_Connector.check_file_already_processed(file, row_count):
							if Mysql_Connector.create_Main_file_Detail_Table():
								values_to_be_inserted =	process_file(list_rest_rows)
								if values_to_be_inserted:
									print('Processing file {}'.format(file))
									result = Mysql_Connector.built_insert_query(values_to_be_inserted)
									if result:
										history_logs = Mysql_Connector.add_processed_file_entry_to_db(file, row_count, config.ftp_host)
										if history_logs:
											print('Data inserted successfully.')
						else:
							print('File with a name {} already processed. Skipping it'.format(file))



# refurbish_file_with_delimiters_350(config.ftp_path_to_copy)

# refurbish_file_with_delimiters(config.ftp_path_to_copy)

result  = Mysql_Connector.establish_dbConnection()
if result:
	status  = Mysql_Connector.create_database()
	if status:
		flag = Mysql_Connector.create_file_processing_history_table()

files  = list_360_files(config.ftp_path_to_copy)


# make most of the char fields to decimal (11) in main table
# Change datetime to utc datetime(6) in history tableś.
# create separate schema.py file to invoke column names from there.
