import sqlite3 as sqlite
import json

fileName = "task_4_var_12_product_data"
fileNameUpdate = "task_4_var_12_update_data"

def executeDatabase(connection, method, name, param):
    db = connection.cursor()
    if(method == "remove"):
        db.execute(f"DELETE FROM products WHERE name = '{name}'")
    elif(method == "price_abs"):
        if(float(param) > 0):
            result = db.execute(f"SELECT id, price, updated FROM products WHERE name = '{name}'")
            for data in result.fetchall():
                updated = data["updated"] + 1
                id = data["id"]
                db.execute(f"UPDATE products SET price = {param}, updated = {updated} WHERE id = {id}")
    elif(method == "quantity_sub"):
        result = db.execute(f"SELECT id, quantity, updated FROM products WHERE name = '{name}'")
        for data in result.fetchall():
            updated = data["updated"] + 1
            id = data["id"]
            quantity = data["quantity"] - int(param)
            if(quantity >= 0):
                db.execute(f"UPDATE products SET quantity = {quantity}, updated = {updated} WHERE id = {id}")
    elif(method == "available"):
        result = db.execute(f"SELECT id, isAvailable, updated FROM products WHERE name = '{name}'")
        for data in result.fetchall():
            updated = data["updated"] + 1
            id = data["id"]
            available = bool(data["isAvailable"] == True or data["isAvailable"] == 1)
            db.execute(f"UPDATE products SET isAvailable = {available}, updated = {updated} WHERE id = {id}")
    elif(method == "quantity_add"):
        result = db.execute(f"SELECT id, quantity, updated FROM products WHERE name = '{name}'")
        for data in result.fetchall():
            updated = data["updated"] + 1
            id = data["id"]
            quantity = data["quantity"] + int(param)
            if(quantity >= 0):
                db.execute(f"UPDATE products SET price = {quantity}, updated = {updated} WHERE id = {id}")
    elif(method == "price_percent"):
        result = db.execute(f"SELECT id, price, updated FROM products WHERE name = '{name}'")
        for data in result.fetchall():
            updated = data["updated"] + 1
            id = data["id"]
            price = data["price"] * float(param)
            if(price >= 0):
                db.execute(f"UPDATE products SET price = {price}, updated = {updated} WHERE id = {id}")
    connection.commit()
    
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

allUpdate = []
with open("assets/data/4/"+fileNameUpdate+".text", 'r') as f:
    rawData = dict()
    for row in f.readlines():
        if(row.__contains__("=====") == True):
            allUpdate.append(rawData)
            rawData = dict()
        else:
            rawData[row.strip().split("::")[0]] = row.strip().split("::")[1]

for update in allUpdate:
    executeDatabase(connection, update["method"], update["name"], update["param"])

result = db.execute("SELECT * FROM products ORDER BY updated DESC LIMIT 10")

output_most_updated = []

for data in result.fetchall():
    dataUpToDate = dict()
    dataUpToDate["id"] = data["id"]
    dataUpToDate["name"] = data["name"]
    dataUpToDate["price"] = data["price"]
    dataUpToDate["quantity"] = data["quantity"]
    dataUpToDate["city"] = data["city"]
    dataUpToDate["isAvailable"] = bool(data["isAvailable"] == 1 or data["isAvailable"] == True)
    dataUpToDate["views"] = data["views"]
    dataUpToDate["category"] = data["category"]
    dataUpToDate["updated"] = data["updated"]
    output_most_updated.append(dataUpToDate)
    

with open("assets/output/4/output_most_updated.json", "w") as outfile:
    json.dump(output_most_updated, outfile, indent=4, ensure_ascii=False)