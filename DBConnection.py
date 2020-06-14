import mysql.connector
from mysql.connector import Error

class MySQLConnection:
    connection = None
    cursor = None

    # ****************************************************************
    # =================== connect to mysql db ===================
    # ************************************************************** #
    def connect(self, host, user, password, database):
        try:
            self.connection = mysql.connector.connect(host = host,
                                                      user = user,
                                                      password = password,
                                                      database = database)
            if self.connection.is_connected():
                db_Info = self.connection.get_server_info()
                #print("Connected to MySQL Server version ", db_Info)
                self.cursor = self.connection.cursor()
                return 0

        except Error as e:
            print("Error while connecting to MySQL", e)
            return -1

    # ****************************************************************
    # ================ disconnect from mysql db ==================
    # ************************************************************** #
    def disconnect(self):
        try:
            if self.connection.is_connected() == False:
                return 0
            self.cursor.close()
            self.connection.close()
            #print("MySQL connection is closed")
            return 0

        except Error as e:
            print("Error while disconnecting from MySQL", e)
            return -1

    # ****************************************************************
    # ======================== select ===========================
    # ************************************************************** #
    def select(self, query):
        try:
            if self.cursor == None: return -1
            self.cursor.execute(query)
            record = self.cursor.fetchall()
            return record

        except Exception as e:
            print ("Error while executing query", e)
            return -1

    # ****************************************************************
    # ======================== execute ===========================
    # ************************************************************** #
    def execute(self, query):
        try:
            if self.cursor == None: return -1
            self.cursor.execute(query)
            self.connection.commit()
            return 0
        except Exception as e:
            print ("Error while executing query", e)
            return -1




