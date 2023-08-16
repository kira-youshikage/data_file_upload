from ftplib import FTP  
import logging

class ftp_connection():
	def __init__(self, ip, username, password, bufsize=1024):
		LOG_FORMAT = "%(asctime)s - %(levelname)s : %(message)s"
		logging.basicConfig(filename='log/all.log', level=logging.INFO, format=LOG_FORMAT)
		self.ip = ip
		self.username = username
		self.password = password
		self.password = bufsize
		self.bufsize = bufsize
		logging.info("Try to connect FTP")
		self.ftp=FTP()                         #设置变量
		self.ftp.set_debuglevel(2)             #打开调试级别2，显示详细信息
		self.ftp.connect(ip)          #连接的ftp sever和端口
		self.ftp.login(username, password) 
		logging.info("Connect FTP successfully")

	def upload_file(self, local_path, remote_path):
		logging.info("Start to upload data file")
		fp = open(local_path,'rb')
		self.ftp.storbinary('STOR '+remote_path,fp,self.bufsize)
		fp.close()
		logging.info("Upload data file successfully")

	def download_file(self, local_path, remote_path):
		logging.info("Start to download data file")
		file_handle = open(local_path,'wb').write
		self.ftp.retrbinary("RETR %s"%remote_path,file_handle,self.bufsize)
		logging.info("Upload data file successfully")

	def close_connection(self):
		self.ftp.close()

		