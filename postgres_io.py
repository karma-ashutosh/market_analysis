import psycopg2
import traceback
from psycopg2 import extras


class PostgresIO(object):
    def __init__(self, config=None):
        if config:
            self._ip = config.get("db_ip")
            self._user = config.get("user")
            self._password = config.get("password")
            self._database = config.get("database")
            self._port = config.get("port", "5432")
        self.connection = None
        self.cursor = None
        self._isConnected = False

    def connect(self):
        self.connection = psycopg2.connect(database=self._database, user=self._user, password=self._password,
                                           host=self._ip, port="5432")
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self._isConnected = True

    def disconnect(self):
        self.connection.close()
        self.cursor.close()

    def isConnected(self):
        return self._isConnected

    def _ensure_not_connected(self):
        if self.isConnected():
            raise Exception("Connection has been stablished. Cannot change properties")

    def set_ip(self, ip):
        self._ensure_not_connected()
        self._ip = ip

    def set_user(self, user):
        self._ensure_not_connected()
        self._user = user

    def set_password(self, password):
        self._ensure_not_connected()
        self._password = password

    def set_database(self, database):
        self._ensure_not_connected()
        self._database = database

    def set_port(self, port):
        self._ensure_not_connected()
        self._port = port

    def execute(self, query_list: list):
        exception_list = []
        for query in query_list:
            try:
                self.cursor.execute(query)
            except:
                exception_list.append((query, traceback.format_exc()))
                self.connection.rollback()
        self.connection.commit()
        try:
            execution_result = list(map(lambda dictRow: dict(dictRow), self.cursor.fetchall()))
        except:
            execution_result = None
            exception_list.append(('fetch_result', traceback.format_exc()))
        return {'exception_list': exception_list, 'result': execution_result}

    def insert_jarr(self, json_array: list, table_name: str):
        query_list = []
        query = "INSERT INTO {} ({}) VALUES({})"
        for j_elem in json_array:
            keys = j_elem.keys()
            values = ",".join(map(lambda s: "'" + str(s).replace("'", "''") + "'", [j_elem.get(key) for key in keys]))
            statement = query.format(table_name, ", ".join(list(j_elem.keys())), values)
            query_list.append(statement)
        return self.execute(query_list)

