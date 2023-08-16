import psycopg2
import pandas as pd
import subprocess
import tempfile
import math
import csv

# 根据表名获取对应的配置信息
def get_cfg_msg(table_name):
	"""
	output:dict
		output[cfg_name]:cfg_value
	"""
	conn = psycopg2.connect(database="etl", user="postgres",password="***", host="***.***.***.***", port="5432")
	cursor = conn.cursor()
	# 编辑sql语句，查询对应table的配置信息
	col_name = ["table_name","data_source_type","share_folder_server_ip","share_folder_username","share_folder_password","share_folder_local_name","share_folder_remote_name","ftp_id","ftp_path","ftp_username","ftp_password","key_word","file_type","share_folder_server_name","share_folder_server_path","table_name_dms","table_name_delta","table_name_ods","sheet_name","usecols","include_filename","sftp_id","sftp_username","sftp_keyfile_name","sftp_password","sftp_path","if_ts","ts_command"]
	sql = """select col_list from kira.upload_cfg_msg where table_name='"""+table_name+"';"
	col_list_str = ""
	for i in range(len(col_name)):
		if i != len(col_name)-1:
			col_list_str += col_name[i]+","
		else:
			col_list_str += col_name[i]
	sql = sql.replace('col_list',col_list_str)
	cursor.execute(sql)
	rows = cursor.fetchall()
	cursor.close()
	conn.close()
	# 编辑输出
	output = {}
	for i in range(len(col_name)):
		output[col_name[i]] = rows[0][i]
	return output

# 根据表名获取对应的列信息
def get_col_msg(table_name):
	"""
	output:list
		[file_name,ts_name,data_type,formula]
	"""
	conn = psycopg2.connect(database="etl", user="postgres",password="***", host="***.***.***.***", port="5432")
	cursor = conn.cursor()
	# 编辑sql语句，查询对应table的配置信息
	col_name = ["file_name","ts_name","data_type","formula"]
	sql = """select col_list from kira.upload_col_msg where table_name='"""+table_name+"' and enabled='t';"
	col_list_str = ""
	for i in range(len(col_name)):
		if i != len(col_name)-1:
			col_list_str += col_name[i]+","
		else:
			col_list_str += col_name[i]
	sql = sql.replace('col_list',col_list_str)
	cursor.execute(sql)
	rows = cursor.fetchall()
	cursor.close()
	conn.close()
	# 编辑输出
	output = []
	for i in range(len(rows)):
		output.append(rows[i])
	return output

# 根据表名获取对应的导入记录
def get_result_msg(table_name):
	"""
	output:list
		[file_name,file_size,file_time]
	"""
	conn = psycopg2.connect(database="etl", user="postgres",password="***", host="***.***.***.***", port="5432")
	cursor = conn.cursor()
	# 编辑sql语句，查询对应table的配置信息
	col_name = ["file_name","file_size","file_time"]
	sql = """select col_list from kira.upload_result_msg where table_name='"""+table_name+"';"
	col_list_str = ""
	for i in range(len(col_name)):
		if i != len(col_name)-1:
			col_list_str += col_name[i]+","
		else:
			col_list_str += col_name[i]
	sql = sql.replace('col_list',col_list_str)
	cursor.execute(sql)
	rows = cursor.fetchall()
	cursor.close()
	conn.close()
	# 编辑输出
	output = []
	for i in range(len(rows)):
		output.append(rows[i])
	return output

# 获取etl任务参数
def get_mission_param(mission_path):
	with open(mission_path) as f:
		param_list = f.read().split("\n")
		f.close()
	param_dict = {}
	for i in range(len(param_list)):
		param_list[i] = param_list[i].split("=")
		if len(param_list[i]) == 2:
			param_dict[param_list[i][0]]=param_list[i][1]
		else:
			pass
	return param_dict

# 当多个文件csv文件具有同一个表头时，整合文件
'''
def combine_csv(file_list, col_list, output_file = 'data_file/combine/output.csv'):
	# 慢合并
	df1 = pd.DataFrame(columns=col_list)
	for i in range(len(file_list)):
		df2 = pd.read_csv(file_list[i],dtype=object)
		columns = df2.columns 
		new_columns = []
		for col in columns:
			new_columns.append(col.strip())
		df2.columns = new_columns
		df1 = pd.concat([df1,df2])
	df1 = df1[col_list]
	df1.to_csv(output_file,index=False)
'''
'''
def combine_csv(file_list, col_list, output_file = 'data_file/combine/output.csv'):
	# 快合并
	df_list = []
	df1 = pd.DataFrame(columns=col_list)
	df_list.append(df1)
	for i in range(len(file_list)):
		df2 = pd.read_csv(file_list[i],dtype=object)
		#df2 = df2.T.drop_duplicates().T
		df_list.append(df2)
	df1 = pd.concat(df_list)
	df1 = df1[col_list]
	df1.to_csv(output_file,index=False)
'''

def combine_csv(file_list, col_list, output_file = 'data_file/combine/output.csv'):
		# 快合并2
		df_list = []
		df1 = pd.DataFrame(columns=col_list)
		df_list.append(df1)
		n_files = len(file_list)
		step = 30
		batch_no = math.ceil(n_files/step)
		for i in range(batch_no):
			batch_size = min(step,n_files-(i*step))
			for j in range(batch_size):
				index = i*step+j
				df2 = pd.read_csv(file_list[index],dtype=object)
				df_list.append(df2)
			df3 = pd.concat(df_list)
			df3 = df3[col_list]
			df3.to_csv(f'{output_file}.{i}',index=False)
			df_list = [df1]
		# 合并中间数据集
		written_header = False
		with open(output_file, 'w', newline='') as f_out:
			writer = csv.writer(f_out)
			for i in range(batch_no):
				with open(f'{output_file}.{i}', 'r', newline='') as f_in:
					reader = csv.reader(f_in)
					header = next(reader)
					if not written_header:
						writer.writerow(header)
						written_header = True
					for row in reader:
						writer.writerow(row)
				f_in.close()
				os.remove(f'{output_file}.{i}')
		f_out.close()



'''
# 当多个文件excel文件具有同一个表头时，整合文件,并最终生成csv文件
def combine_excel(file_list, col_list, sheet_name, usecols, include_filename, record_list, output_file = 'data_file/combine/output.csv'):
	# 慢合并
	df1 = pd.DataFrame(columns=col_list)
	for i in range(len(file_list)):
		print(file_list[i])
		df2 = pd.read_excel(file_list[i], sheet_name =sheet_name, skiprows= 0, usecols=usecols,dtype=object)
		data_columns = df2.columns
		for j in range(len(data_columns)):
			if data_columns[j] not in col_list:
				df2.drop([data_columns[j]], axis=1, inplace=True)
		try:
			df2.dropna(subset=['OCR'],inplace=True)
		except:
			pass
		if include_filename=="Y":
			df2.insert(loc=1,column='FileDate',value=record_list[i][2])
			df2.insert(loc=2,column='EXCELFILENAME',value=record_list[i][0])
		columns = df2.columns 
		new_columns = []
		for col in columns:
			new_columns.append(col.strip())
		df2.columns = new_columns
		df1 = pd.concat([df1,df2])
	df1 = df1[col_list]
	df1.to_csv(output_file,index=False)
'''

# 当多个文件excel文件具有同一个表头时，整合文件,并最终生成csv文件
def combine_excel(file_list, col_list, sheet_name, usecols, include_filename, record_list, output_file = 'data_file/combine/output.csv'):
	# 快合并
	df_list = []
	df1 = pd.DataFrame(columns=col_list)
	df_list.append(df1)
	for i in range(len(file_list)):
		print(file_list[i])
		df2 = pd.read_excel(file_list[i], sheet_name =sheet_name, skiprows= 0, usecols=usecols,dtype=object)
		data_columns = df2.columns
		for j in range(len(data_columns)):
			if data_columns[j] not in col_list:
				df2.drop([data_columns[j]], axis=1, inplace=True)
		try:
			df2.dropna(subset=['OCR'],inplace=True)
		except:
			pass
		if include_filename=="Y":
			df2.insert(loc=1,column='FileDate',value=record_list[i][2])
			df2.insert(loc=2,column='EXCELFILENAME',value=record_list[i][0])
		df_list.append(df2)
	df1 = pd.concat(df_list)
	df1 = df1[col_list]
	df1.to_csv(output_file,index=False)

# 整合csv文件
def combine_csv_zip(file_list, col_list, output_file = 'data_file/combine/output.csv'):
    """
    file_list: zip path list
    col_list: parameter list
    """
    # 把文件全部解压，并记录文件名
    csv_list = []
    for i in range(len(file_list)):
        zf = zipfile.ZipFile(file_list[i])
        zf_info_list = zf.infolist()
        zf.extractall(path='data_file/download/')
        for info in range(zf_info_list):
            csv_list.append(info.filename)
    # 合并csv
    df1 = pd.DataFrame(columns=col_list)
    for i in range(len(csv_list)):
        df2 = pd.read_csv(csv_list[i],dtype=object)
        columns = df2.columns 
        new_columns = []
        for col in columns:
            new_columns.append(col.strip())
        df2.columns = new_columns
        df1 = pd.concat([df1,df2])
    df1 = df1[col_list]
    df1.to_csv(output_file,index=False)
    # 删除zip文件
    for i in range(len(file_list)):
        os.remove(file_list[i])

# 获取etl的sql
def get_etl_sql(table_name,table_ods,table_delta,table_dms):
	"""
	output:[ods_to_delta,ods_to_dms]
	"""
	# 根据表名获取对应的sql模板
	conn = psycopg2.connect(database="etl", user="postgres",password="***", host="***.***.***.***", port="5432")
	cursor = conn.cursor()
	get_sql_template = "select etl_template from kira.upload_cfg_msg where table_name='"+table_name+"';"
	cursor.execute(get_sql_template)
	rows = cursor.fetchall()
	cursor.close()
	conn.close()
	sql_template = rows[0][0]
	# 初始化输出
	output = []
	# ods_to_delta
	output.append(sql_template.replace('target_table_name',table_delta).replace('ori_table_name',table_ods))
	# ods_to_dms
	output.append(sql_template.replace('target_table_name',table_dms).replace('ori_table_name',table_ods))
	return output

# 获取postgres里etl的sql
def get_insert(file_name,table_ods,db_ip,db_port,db_user,db_password,db_name,table_name):
	insert_file = table_name+"_insert.sql"
	insert_sql = "\\copy "+table_ods+"("
	for i in range(len(file_name)):
		if i==len(file_name)-1:
			insert_sql += "\""+file_name[i]+"\")"
		else:
			insert_sql += "\""+file_name[i]+"\","
	insert_sql +=""" from 'data_file/combine/output.csv' with  (delimiter ',', format csv, header true);"""
	with open(insert_file,'w') as f:
		f.write(insert_sql)
		f.close()
	# 编辑导入命令
	database_part = '"host='+db_ip+' port='+db_port+' user='+db_user+' password='+db_password+' dbname='+db_name+'"'
	input_command = "psql -f ./"+insert_file+" "+database_part
	return input_command

# 先把ods和delta层的数据清掉，然后导入合并后的文件数据到ods层，如果没报错的话就把对应的文件信息插入到记录表中
def insert_data(insert_command, ods_table_name,delta_table_name, record_list,table_name):
	# record_list:[[[文件名，文件大小，文件时间]]]
	conn = psycopg2.connect(database="etl", user="postgres",password="***", host="***.***.***.***", port="5432")
	cursor = conn.cursor()
	# 先把ods层和delta层表清掉
	truncate_sql = "truncate table table_name;"
	if ods_table_name is not None and ods_table_name!='':
		cursor.execute(truncate_sql.replace("table_name",ods_table_name))
		conn.commit()
	if delta_table_name is not None and delta_table_name!='':
		cursor.execute(truncate_sql.replace("table_name",delta_table_name))
		conn.commit()
	# 开始导入数据
	cmd_res={}
	res = subprocess.Popen(insert_command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	cmd_res['out']=res.stdout.readlines()
	cmd_res['err']=res.stderr.readlines()
	# 判断导入是否错误,如果成功导入就把已导入的文件写入数据库
	if cmd_res['err']==[]:
		insert_sql_template = "insert into kira.upload_result_msg(table_name,file_name,file_size,file_time) values('"+table_name+"',"
		for i in range(len(record_list)):
			insert_sql = insert_sql_template+" "
			for j in range(len(record_list[i])):
				if j!=len(record_list[i])-1:
					insert_sql+="'"+str(record_list[i][j])+"',"
				else:
					insert_sql+="'"+str(record_list[i][j])+"');"
			cursor.execute(insert_sql)
			conn.commit()
	cursor.close()
	conn.close()
	return cmd_res

# etl执行
def etl_exe(etl_sql):
	conn = psycopg2.connect(database="etl", user="postgres",password="***", host="***.***.***.***", port="5432")
	cursor = conn.cursor()
	cursor.execute(etl_sql);
	conn.commit()
	cursor.close()
	conn.close()




import os
import datetime
'''
dir_root = "\\\\dn2nas01\\tsb_data_backup\\MESAutoReportD2\\RAWDATA\\"
file_root = ["ABS"]
print(find_new_file(dir_root+file_root[0]))
'''
# 在linux服务器上下载指定时间段内的文件，并返回下载的路径文件列表
from smb.SMBConnection import SMBConnection
def find_new_file_linux(server_ip,username,password,server_name, share_path,my_name,remote_name,record_NST,logging, end_datetime=None, delta_days=-1, key_word=None,file_type=".csv"):
	# 初始化时间(字符串)
	if end_datetime is None or end_datetime=="None" or end_datetime=="":
		end_datetime = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	end_datetime = datetime.datetime.strptime(end_datetime,'%Y-%m-%d %H:%M:%S')
	start_datetime = (end_datetime+datetime.timedelta(days=int(delta_days))).strftime('%Y-%m-%d %H:%M:%S')
	logging.info("time range:"+str(start_datetime)+"~"+str(end_datetime))
	# 返回的文件路径列表
	output_name = [] # 文件名
	output = []
	# 连接共享文件夹
	conn = SMBConnection(username, password, my_name, remote_name, is_direct_tcp = True)
	assert conn.connect(server_ip, 445)
	# 根据文件创建时间来判断是否需要下载该文件，如果需要就下载到指定的文件夹下面
	sharelist = conn.listPath(server_name,share_path)
	record_list=[]
	for i in range(len(sharelist)):
		# 如果当前是文件夹，进行递归
		if sharelist[i].isDirectory:
			if sharelist[i].filename != '.' and sharelist[i].filename != '..':
				output_item,record_list_item = find_new_file_linux(server_ip,username,password,server_name, share_path+sharelist[i].filename+"/",my_name,remote_name,record_NST,logging, str(end_datetime), delta_days, key_word,file_type)
				output = output+output_item
				record_list = record_list+record_list_item
		else:
			if (key_word is not None or key_word!="" or key_word not in sharelist[i].filename) and (file_type not in sharelist[i].filename):
				pass
			elif '.xlsx#' in sharelist[i].filename:
				pass
			else:
				dateTime = datetime.datetime.fromtimestamp(sharelist[i].create_time).strftime('%Y-%m-%d %H:%M:%S')
				if str(dateTime)>=str(start_datetime) and str(dateTime)<str(end_datetime):
					objfile = tempfile.NamedTemporaryFile()
					file_attributes, filesize = conn.retrieveFile(server_name,share_path+'/'+ sharelist[i].filename,objfile)
					print(filesize)
					if ((sharelist[i].filename,str(filesize),str(dateTime)) not in record_NST):
						output_name.append(sharelist[i].filename)
						if file_type in ['.xls','.zip']:
							local_file = 'data_file/download/'+sharelist[i].filename
						else:
							local_file = 'data_file/download/'+sharelist[i].filename.replace(file_type,'.csv')
						share_file = share_path+sharelist[i].filename
						f = open(local_file,'wb')
						conn.retrieveFile(server_name,share_file,f)
						output.append(local_file)
						f.close()
						record_list.append((sharelist[i].filename,str(filesize),str(dateTime)))
	conn.close()		
	return output,record_list

from ftplib import FTP  
def download_from_ftp(ip,username,password,path,record_NST,logging, end_datetime=None, delta_days=-1, key_word=None, bufsize=1024, file_type=".csv"):
	"""
	从ftp服务器下载文件到ftp文件夹，并返回路径
	"""
	bufsize = bufsize
	ftp=FTP()                         #设置变量
	ftp.set_debuglevel(2)             #打开调试级别2，显示详细信息
	ftp.connect(ip)          #连接的ftp sever和端口
	ftp.login(username,password) 
	ftp.cwd(path)   
	# 初始化时间(字符串)
	if end_datetime is None or end_datetime=="None" or end_datetime=="":
		end_datetime = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	end_datetime = datetime.datetime.strptime(end_datetime,'%Y-%m-%d %H:%M:%S')
	start_datetime = (end_datetime+datetime.timedelta(days=int(delta_days))).strftime('%Y-%m-%d %H:%M:%S')
	print(str(end_datetime),str(start_datetime))
	logging.info("time range:"+str(start_datetime)+"~"+str(end_datetime))
	output = [] # 返回的文件路径列表
	record_list = [] # [文件名，文件大小，文件时间]
	lists = ftp.nlst() #获取文件夹中的所有文件
	# 判断有哪些文件在时间范围之内，把这些文件下载到ftp文件夹（csv格式）中并返回文件名
	for i in range(len(lists)):
		#if (key_word is not None or key_word!="" or key_word not in lists[i]) and (file_type not in lists[i]):
		#	pass
		if (key_word is None or key_word=="" or key_word in lists[i]) and (file_type in lists[i]):
			print(key_word,lists[i],file_type)
			ftp_time = ftp.sendcmd('MDTM ' + "./%s"%lists[i]).split(' ')[1]
			ftp_time = datetime.datetime.strptime(ftp_time,'%Y%m%d%H%M%S')
			ftp_size = ftp.size("./%s"%lists[i])
			if str(ftp_time)>=str(start_datetime) and str(ftp_time)<str(end_datetime) and ((lists[i],str(ftp_size),str(ftp_time)) not in record_NST):
				file_handle = open('data_file/download/%s'%lists[i].replace(file_type,".csv"),'wb').write
				ftp.retrbinary("RETR %s"%lists[i],file_handle,bufsize)
				output.append("data_file/download/"+lists[i].replace(file_type,".csv"))
				record_list.append((lists[i],str(ftp_size),str(ftp_time)))
		else:
			pass
	return output,record_list

import paramiko
def download_from_sftp(ip,username,path,record_NST,logging,keyfile_name=None,password=None, end_datetime=None, delta_days=-1, key_word=None,  file_type=".csv"):
	"""
	从sftp服务器下载文件到sftp文件夹，并返回路径
	"""
	sshClient = paramiko.SSHClient()
	sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	if keyfile_name is not None:
		sshClient.connect(hostname=ip, username=username,key_filename=keyfile_name)
	else:
		sshClient.connect(hostname=ip, username=username,password=password)
	sftp = paramiko.SFTPClient.from_transport(sshClient.get_transport())  
	# 初始化时间(字符串)
	if end_datetime is None or end_datetime=="None" or end_datetime=="":
		end_datetime = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	end_datetime = datetime.datetime.strptime(end_datetime,'%Y-%m-%d %H:%M:%S')
	start_datetime = (end_datetime+datetime.timedelta(days=int(delta_days))).strftime('%Y-%m-%d %H:%M:%S')
	print(str(end_datetime),str(start_datetime))
	logging.info("time range:"+str(start_datetime)+"~"+str(end_datetime))
	output = [] # 返回的文件路径列表
	record_list = [] # [文件名，文件大小，文件时间]
	lists = sftp.listdir(path) #获取文件夹中的所有文件

	# 判断有哪些文件在时间范围之内，把这些文件下载到ftp文件夹（csv格式）中并返回文件名
	for i in range(len(lists)):
		#if (key_word is not None or key_word!="" or key_word not in lists[i]) and (file_type not in lists[i]):
		#	pass
		if (key_word is None or key_word=="" or key_word in lists[i]) and (file_type in lists[i]):
			file_path = path+'/'+lists[i]
			print(key_word,lists[i],file_type)
			file_stat = sftp.stat(file_path)
			ftp_time = file_stat.st_mtime
			ftp_time = datetime.datetime.fromtimestamp(ftp_time).strftime('%Y-%m-%d %H:%M:%S')
			ftp_size = file_stat.st_size
			print(str(ftp_time),str(start_datetime),str(end_datetime))
			if str(ftp_time)>=str(start_datetime) and str(ftp_time)<str(end_datetime) and ((lists[i],str(ftp_size),str(ftp_time)) not in record_NST):
				sftp.get(file_path,'data_file/download/%s'%lists[i])
				output.append("data_file/download/"+lists[i])
				record_list.append((lists[i],str(ftp_size),str(ftp_time)))
		else:
			pass
	return output,record_list

def upload_to_ts(table_name,record_list,command,logging):
	# 编辑导入ts的命令
	'''
	/usr/anaconda3/bin/python remote_tools/data_importer_client.py --cluster_host "10.10.200.11"  --username "kira_lu@sae.com.hk"  --password "a28318932203"    --target_database "SAE200"  --target_schema "falcon_default_schema"  --target_table "ABS"  --field_separator ","  --date_format "%Y-%m-%d"  --date_time_format "%Y-%m-%d %H:%M:%S"  --time_format "%H:%M:%S"  --skip_second_fraction  --escape_character "\""  --enclosing_character "\""  --null_value ""  --max_ignored_rows 100  --has_header_row  --flexible  --date_time_format "%m/%d/%Y %I:%M:%S %p"  --type "CSV"  --source_files "data_file/ABS.csv"
	'''
	cmd_res={}
	res = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	cmd_res['out']=res.stdout.readlines()
	cmd_res['err']=res.stderr.readlines()
	logging.info("Upload to ThoughtSpot:"+str(cmd_res))
	# 判断导入是否错误,如果成功导入就把已导入的文件写入数据库
	result = 0
	status_err_str = """'status': {'code': 'LOAD_FAILED'"""
	for i in range(len(cmd_res['out'])):
		if status_err_str in cmd_res['out'][i]:
			result = 1
			break
	if cmd_res['err']==[] and result==0:
		# record_list:[[[文件名，文件大小，文件时间]]]
		conn = psycopg2.connect(database="etl", user="postgres",password="***", host="***.***.***.***", port="5432")
		cursor = conn.cursor()
		insert_sql_template = "insert into kira.upload_result_msg(table_name,file_name,file_size,file_time) values('"+table_name+"',"
		for i in range(len(record_list)):
			insert_sql = insert_sql_template+" "
			for j in range(len(record_list[i])):
				if j!=len(record_list[i])-1:
					insert_sql+="'"+str(record_list[i][j])+"',"
				else:
					insert_sql+="'"+str(record_list[i][j])+"');"
			cursor.execute(insert_sql)
			conn.commit()
		cursor.close()
		conn.close()