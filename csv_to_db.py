import json
import psycopg2 as db

class ConfigDatabase:
    def __init__(self):

        config_json_data = json.load(open("dbconfig.json", "r"))

        self.config = {
            "postgres" : {
                "user" : config_json_data["user"],
                "password" : config_json_data["password"],
                "host" : config_json_data["host"],
                "port" : config_json_data["port"],
                "database" : config_json_data["database"]
            }
        }


class Connection(ConfigDatabase):
    def __init__(self):
        ConfigDatabase.__init__(self)
        
        try:
            self.conn = db.connect(**self.config["postgres"])
            self.cur = self.conn.cursor()

        except Exception as e:
            print(" --> ERRO: nao foi possivel conectar", e)
            exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    @property
    def connection(self):
        return self.conn

    @property
    def cursor(self):
        return self.cur

    def commit(self):
        self.connection.commit()  

    def fetchall(self):
        return self.cursor.fetchall()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        
    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()


class Moto(Connection):
    def __init__(self):
        Connection.__init__(self)

    def insert(self, *args):
        try:
            sql = "INSERT INTO motos (modelo, preco) VALUES (%s, %s)"
            self.execute(sql, args)
            self.commit()
        except Exception as e:
            print(" --> ERRO: nao foi possivel inserir informações no Banco de Dados", e) 


if __name__ == "__main__":
    moto = Moto()
    moto.insert("MOTO GENERICA 3", 2000)