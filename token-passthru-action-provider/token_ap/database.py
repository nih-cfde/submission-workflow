import dbm
import os
import pickle


class Database:
    database_directory = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(database_directory, "token_db")

    def persist(self, key, value):
        with dbm.open(self.filename, "c") as datab:
            datab[key] = pickle.dumps(value)

    def query(self, key):
        with dbm.open(self.filename, "c") as datab:
            val = datab.get(key, pickle.dumps(None))
        return pickle.loads(val)

    def delete(self, key):
        with dbm.open(self.filename, "c") as datab:
            del datab[key]


db = Database()
