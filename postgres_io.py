import psycopg2
import traceback

import yaml
from psycopg2 import extras

from general_util import chunks


class PostgresIO(object):
    def __init__(self, config=None):
        print(config)
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

    def execute(self, query_list: list, fetch_result=False):
        exception_list = []
        for query in query_list:
            try:
                self.cursor.execute(query)
            except:
                exception_list.append((query, traceback.format_exc()))
                self.connection.rollback()
        self.connection.commit()
        try:
            execution_result = list(map(lambda dictRow: dict(dictRow), self.cursor.fetchall())) if fetch_result else []
        except:
            execution_result = None
            exception_list.append(('fetch_result', traceback.format_exc()))
        return {'exception_list': exception_list, 'result': execution_result}

    def insert_jarr(self, json_array: list, table_name: str):
        query_list = []
        query = "INSERT INTO {} ({}) VALUES({})"
        for j_elem in json_array:
            keys = j_elem.keys()
            quoted_keys = ['"' + key + '"' for key in j_elem.keys()]
            values = ",".join(map(lambda s: "'" + str(s).replace("'", "''") + "'", [j_elem.get(key) for key in keys]))
            statement = query.format(table_name, ", ".join(list(quoted_keys)), values)
            query_list.append(statement)
        return self.execute(query_list, fetch_result=False)

    def insert_or_skip_on_conflict(self, json_array: list, table_name: str, primary_key_columns: list):
        query_list = []
        query = "INSERT INTO {} ({}) VALUES({}) ON CONFLICT ({}) DO NOTHING"

        for j_elem in json_array:
            keys = j_elem.keys()
            quoted_keys = ['"' + key + '"' for key in j_elem.keys()]
            quoted_primary_key_columns = ['"' + key + '"' for key in primary_key_columns]
            values = ",".join(map(lambda s: "'" + str(s).replace("'", "''") + "'", [j_elem.get(key) for key in keys]))
            statement = query.format(table_name, ", ".join(list(quoted_keys)), values,
                                     ", ".join(quoted_primary_key_columns))
            query_list.append(statement)
        return self.execute(query_list, fetch_result=False)


class PostgresDataMigration:
    def __init__(self):
        self._new_postgres = self._get_new_postgres()
        self._old_postgres = self._get_old_postgres()

    def migrate_data(self, old_table_name, new_table_name):
        data = self._old_postgres.execute(["SELECT * FROM {}".format(old_table_name)], fetch_result=True)['result']
        print("data len is {}".format(len(data)))
        data_chunks = chunks(data, len(data) / 1000)
        print("number of chunks are: {}".format(len(data_chunks)))

        for i in range(len(data_chunks)):
            print("Inserting chunk index: {}".format(i))
            self._new_postgres.insert_jarr(data_chunks[i], new_table_name)

    @staticmethod
    def _get_old_postgres():
        with open("config.yml") as handle:
            old_conf = yaml.load(handle)['postgres-config']
        postgres = PostgresIO(old_conf)
        postgres.connect()
        return postgres

    @staticmethod
    def _get_new_postgres():
        with open("config.yml") as handle:
            new_conf = yaml.load(handle)['postgres-config']
        new_conf['db_ip'] = 'market-poc-alpha-2.cc9kvgcpuesr.ap-south-1.rds.amazonaws.com'
        postgres = PostgresIO(new_conf)
        postgres.connect()
        return postgres
