import sqlite3 as sqlite
import json

fileName = "task_4_var_12_product_data"

def initDB():
    connection = sqlite.connect('assets/database/database.db')
    db = connection.cursor()

    db.execute("""
            CREATE TABLE IF NOT EXISTS products (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            name                TEXT (255),
            price               FLOAT,
            quantity            INTEGER,
            city                TEXT (100),
            isAvailable         BOOL,
            views               INTEGER,
            category            TEXT (100),
            updated             INTEGER DEFAULT 0
            )""")
    connection.commit()
    return connection

connection = initDB()
connection.row_factory = sqlite.Row
db = connection.cursor()

with open("assets/data/4/"+fileName+".json", 'rb') as f:
    loadAllDataFile = json.load(f)
    result = db.execute("SELECT * FROM products")
    if(result.fetchone() == None):
        for data in loadAllDataFile:
            if(str(data.keys()).__contains__("category") == True):
                db.execute("""
                INSERT INTO products (
                    name,
                    price,
                    quantity,
                    city,
                    isAvailable,
                    views,
                    category
                ) VALUES (
                    '%s',
                    %s,
                    %s,
                    '%s',
                    %s,
                    %s,
                    '%s'
                )
                """%(data["name"],data["price"],data["quantity"],data["fromCity"],data["isAvailable"],data["views"],data["category"]),)
                connection.commit()
            else:
                db.execute("""
                INSERT INTO products (
                    name,
                    price,
                    quantity,
                    city,
                    isAvailable,
                    views
                ) VALUES (
                    '%s',
                    %s,
                    %s,
                    '%s',
                    %s,
                    %s
                )
                """%(data["name"],data["price"],data["quantity"],data["fromCity"],data["isAvailable"],data["views"]),)
                connection.commit()

