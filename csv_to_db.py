import json
import pandas as pd
import sys
import psycopg2 as db
import locale
from datetime import datetime

# Gets all the information from the JSON file
class ConfigDatabase:
    def __init__(self):

        config_json_data = json.load(open("dbconfig.json", "r"))

        self.config = {
            "postgres": {
                "user": config_json_data["user"],
                "password": config_json_data["password"],
                "host": config_json_data["host"],
                "port": config_json_data["port"],
                "database": config_json_data["database"],
            }
        }
# ---------------------------------------------------------------------------


# Connects and have the methods to acess and modify the database
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
# ---------------------------------------------------------


# Method used to manipulate the table Motos
class Moto(Connection):
    def __init__(self):
        Connection.__init__(self)

    def insert(self, *args):
        try:
            sql = "INSERT INTO motos (modelo, preco) VALUES (%s, %s)"
            self.execute(sql, args)
            self.commit()
        except Exception as e:
            print(" --> ERROR: it was not possible to insert into the database:", e)

    def delete_by_model(self, *args):
        try:
            sql = "DELETE FROM motos WHERE modelo = %s;"
            self.execute(sql, args)
            self.commit()
        except Exception as e:
            print(" --> ERROR: it was not possible to delete from the database:", e)

    def update_price(self, *args):
        try:
            sql = "UPDATE motos SET preco = %s WHERE modelo = %s"
            self.execute(sql, args)
            self.commit()
        except Exception as e:
            print(" --> ERROR: it was not possible to update de database:", e)

    def search(self, *args):
        try:
            if args:
                sql = "SELECT modelo,preco FROM motos WHERE modelo = %s"
            else:
                sql = "SELECT modelo,preco FROM motos"

            self.execute(sql, args)

            return self.fetchall()
        except Exception as e:
            print(" --> ERROR: it was not possible to fetch the search", e)
# --------------------------------------------------------------------------


if __name__ == "__main__":

    locale.setlocale(locale.LC_ALL, 'en-US')
    
    def format_price(unformatted_price):
            return locale.currency(float(unformatted_price), grouping=True)

    moto = Moto()

    csv_column_id = {
        "MODEL" : 0,
        "PRICE" : 1
    }

    csv_file_name = sys.argv[1]
    csv_file = pd.read_csv(csv_file_name, sep=";")

    try:
        operation = sys.argv[2]

        if operation == "DELETE":
            for row in csv_file["MODEL"]:
                moto.delete_by_model(row)
                print(" --> [{}] deleted".format(row))
        
            print(" --> Rows deleted according to file /" + csv_file_name)
        
        elif operation == "REPORT":
            sql_query = moto.search()
            sql_query_dataframe = pd.DataFrame(sql_query, columns=['MODEL', 'PRICE'])
            sql_query_report_name="BIKES_REPORT_" + datetime.now().strftime("%d-%m-%Y_%H-%M-%S") + ".csv"

            sql_query_dataframe.to_csv(sql_query_report_name, index = False, sep=";")

            print(" --> CSV report generated as /" + sql_query_report_name)
           
        sys.exit()
        
    except Exception as e:
        print(" --> Operation not informed, following with standarn insert and update", e)

        for row in csv_file.values:

            row_model = row[csv_column_id["MODEL"]]
            row_price = row[csv_column_id["PRICE"]]
            formatted_row_price = format_price(int(row_price))
            sql_model_query = moto.search(row_model)

            if sql_model_query:
                moto.update_price(row_price, row_model)
                print(" --> Updating price [{}] to {}".format(row_model, formatted_row_price))

            else:
                moto.insert(row_model, row_price)
                print(" --> Inserting new row [MODEL: {} with price {}]".format(row_model, formatted_row_price))