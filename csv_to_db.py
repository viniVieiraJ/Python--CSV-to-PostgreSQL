import json
import pandas as pd
import sys
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
            print(" --> ERRO: nao foi possivel inserir informações no Banco de Dados:", e) 

    def delete_by_model(self, *args):
        try:
            sql = "DELETE FROM motos WHERE modelo = %s;"
            self.execute(sql, args)
            self.commit()
        except Exception as e:
            print(" --> ERRO: nao foi possivel deletar as informações no Banco de Dados:", e)
    
    def delete_by_price(self, *args):
        try:
            sql = "DELETE FROM motos WHERE preco = %s;"
            self.execute(sql, args)
            self.commit()
        except Exception as e:
            print(" --> ERRO: nao foi possivel deletar as informações no Banco de Dados:", e)

    def update_preco(self, *args):
        try:
            sql = "UPDATE motos SET preco = %s WHERE modelo = %s"
            self.execute(sql, args)
            self.commit()
        except Exception as e:
            print(" --> ERRO: nao foi possivel atualizar as informações no Banco de Dados:", e)

if __name__ == "__main__":
    moto = Moto()

    csv_file = pd.read_csv(sys.argv[1], sep=";")
    operation = sys.argv[2] # delete_by_model, delete_by_price, update_preco, insert

    if operation == "insert":
        for row in csv_file.values:
            moto.insert(row[0], row[1])

    elif operation == "delete_by_model":
        for row in csv_file["MODELO"]:
            moto.delete_by_model(row)
            
    elif operation == "delete_by_price":
        for row in csv_file["PRECO"]:
            moto.delete_by_price(row)

    elif operation == "update_preco":
        for row in csv_file.values:
            moto.update_preco(row[1], row[0])