import jaydebeapi 
import pandas as pd  
import logging

class jdbc_connection():
	def __init__(self, db_type, url, user, password):
		LOG_FORMAT = "%(asctime)s - %(levelname)s : %(message)s"
		logging.basicConfig(filename='log/all.log', level=logging.INFO, format=LOG_FORMAT)
		self.db_type = db_type
		self.url = url
		self.user = user
		self.password = password
		logging.info("Start to connect the database")
		if db_type=='GridDB':
			self.dirver = 'com.toshiba.mwcloud.gs.sql.Driver'
			self.jar_file = 'jdbc_jar/gridstore-jdbc-5.1.0.jar'
		if db_type=='PostgreSQL':
			self.dirver = 'org.postgresql.Driver'
			self.jar_file = 'jdbc_jar/postgresql-42.5.0.jar'
		self.conn = jaydebeapi.connect(self.dirver, self.url, [self.user, self.password], self.jar_file)
		self.curs = self.conn.cursor()
		logging.info("Connect the database successfully")

	def search_data(self,sql):
		logging.info("Start to query the data")
		self.curs.execute(sql)
		result = self.curs.fetchall()
		col_msg = self.curs.description
		col_list = []
		for i in range(len(col_msg)):
			col_list.append(col_msg[i][0])
		self.df = pd.DataFrame(result)
		self.df.columns = col_list
		logging.info("Query the data successfully")
		return self.df

	def save_data(self, path):
		logging.info("Start to save the data")
		self.df.to_csv(path,index=False)
		logging.info("Save the data successfully")
		return path

	def close_connect(self):
		self.curs.close()
		self.conn.close()
		logging.info("Colse the connect successfully")
