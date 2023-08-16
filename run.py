import logging
from utils import get_mission_param,download_from_ftp,download_from_sftp,get_result_msg,get_col_msg,get_cfg_msg,combine_csv,get_etl_sql,get_insert,insert_data,etl_exe,find_new_file_linux,combine_excel,upload_to_ts

# 日志配置
LOG_FORMAT = "%(asctime)s - %(levelname)s : %(message)s"
# level:'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
logging.basicConfig(filename='log/all.log', level=logging.INFO, format=LOG_FORMAT)
# 相关路径
mission_path="config/mission.config"
postgre_path="config/PostgreSQL.config"
# 读取信息
logging.info("Get config start.")
mission_param = get_mission_param(mission_path)
table_name = mission_param["table_name"]
postgre_param = get_mission_param(postgre_path)
cfg_msg = get_cfg_msg(table_name)
print(cfg_msg)
col_msg = get_col_msg(table_name)
record_NST = get_result_msg(table_name)
logging.info("Get config finish:")
#logging.info("Get config finish:"+str(cfg_msg))
# 把数据文件下载到本地

logging.info(table_name+":Download start.")
if cfg_msg["data_source_type"]=='ftp':
	file_list,record_list = download_from_ftp(logging=logging,ip=cfg_msg['ftp_id'],username=cfg_msg['ftp_username'],password=cfg_msg['ftp_password'],path=cfg_msg['ftp_path'],file_type=cfg_msg['file_type'],end_datetime=mission_param['end_datetime'],delta_days=mission_param['delta_days'],record_NST=record_NST,key_word=cfg_msg['key_word'])
elif cfg_msg["data_source_type"]=='sftp':
	file_list,record_list = download_from_sftp(logging=logging,ip=cfg_msg['sftp_id'],username=cfg_msg['sftp_username'],keyfile_name=cfg_msg['sftp_keyfile_name'],password=cfg_msg['sftp_password'],path=cfg_msg['sftp_path'],file_type=cfg_msg['file_type'],end_datetime=mission_param['end_datetime'],delta_days=mission_param['delta_days'],record_NST=record_NST,key_word=cfg_msg['key_word'])
elif cfg_msg["data_source_type"]=='share_folder':
	file_list,record_list = find_new_file_linux(server_ip=cfg_msg['share_folder_server_ip'],username=cfg_msg['share_folder_username'],password=cfg_msg['share_folder_password'],server_name=cfg_msg['share_folder_server_name'], share_path=cfg_msg['share_folder_server_path'], my_name=cfg_msg['share_folder_local_name'],remote_name=cfg_msg['share_folder_remote_name'],record_NST=record_NST,logging=logging, end_datetime=mission_param["end_datetime"], delta_days=mission_param['delta_days'], key_word=cfg_msg['key_word'],file_type=cfg_msg['file_type'])
logging.info(table_name+":Download finish.")
if len(file_list)==0:
	logging.info(table_name+":no file.")
else:
	# 获取文件的列名
	file_col_list = []
	for i in range(len(col_msg)):
		file_col_list.append(col_msg[i][0])
	# 如果需要记录文件名，则添加一下列
	if cfg_msg["include_filename"]=="Y":
		file_col_list.append("FileDate")
		file_col_list.append("EXCELFILENAME")
	logging.info(table_name+":Combine start")
	if cfg_msg['file_type']=='.xls':
		combine_excel(file_list=file_list, col_list=file_col_list, sheet_name=cfg_msg['sheet_name'],usecols=cfg_msg['usecols'],include_filename= cfg_msg['include_filename'],record_list=record_list)
	else:
		combine_csv(file_list=file_list,col_list=file_col_list)
	logging.info(table_name+":Combine finish")
	if cfg_msg["if_ts"]!='true':
		# 获取ods到delta、ods到dms的ETL的sql
		logging.info(table_name+":Get etl sql start")
		etl_sql = get_etl_sql(table_name=table_name,table_ods=cfg_msg['table_name_ods'],table_delta=cfg_msg['table_name_delta'],table_dms=cfg_msg['table_name_dms'])
		logging.info(table_name+":Get etl sql finish")
		# 获取插入命令
		logging.info(table_name+":Get insert command start")
		insert_command = get_insert(file_name=file_col_list,table_ods=cfg_msg['table_name_ods'],db_ip=postgre_param["ip"],db_port=postgre_param["port"],db_user=postgre_param["user"],db_password=postgre_param["password"],db_name=postgre_param["dp_name"],table_name=table_name)
		logging.info(table_name+":Get insert command finish")
		# 插入数据
		logging.info(table_name+":Insert start.")
		insert_result = insert_data(insert_command=insert_command, ods_table_name=cfg_msg['table_name_ods'],delta_table_name=cfg_msg['table_name_delta'], record_list=record_list,table_name=table_name)
		logging.info(table_name+":Insert result:"+str(insert_result))
		# ods_to_delta
		if cfg_msg["table_name_delta"] is not None and cfg_msg["table_name_delta"]!='':
			logging.info(table_name+":ods to delta start.")
			etl_exe(etl_sql[0])
			logging.info(table_name+":ods to delta finish.")
		# ods_to_dms
		if cfg_msg["table_name_dms"] is not None or cfg_msg["table_name_dms"]!='':
			logging.info(table_name+":ods to dms start.")
			etl_exe(etl_sql[1])
			logging.info(table_name+":ods to dms fihish.")
	else:
		logging.info(table_name+":Start to upload file.")
		upload_to_ts(table_name,record_list,cfg_msg["ts_command"],logging)