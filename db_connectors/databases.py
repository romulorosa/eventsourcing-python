import abc
import config
from rethinkdb import RethinkDB


class DatabaseConnector(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def connect(self, hostname, port, database):
        pass

    @abc.abstractmethod
    def disconnect(self):
        pass


class RethinkDbDatabaseConnector(DatabaseConnector):
    def __init__(self):
        self.r = RethinkDB()
        self.conn = None

    def disconnect(self):
        self.conn.close()

    def connect(self, hostname, port, database):
        self.conn = self.r.connect(hostname, port, database)
        return self.r, self.conn


def database_connect():
    engine = config.DATABASE.get('engine')
    hostname = config.DATABASE.get('hostname')
    port = config.DATABASE.get('port')
    database = config.DATABASE.get('database')

    if engine == 'rethinkdb':
        connector = RethinkDbDatabaseConnector()
        return connector.connect(hostname, port, database)
    else:
        raise RuntimeError('Database engine not known')

