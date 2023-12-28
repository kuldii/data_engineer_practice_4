import numpy as np
import sqlite3
import json

def saveToJson(name, data):
    with open("assets/output/4/resolved/"+name, 'w') as jsonFile:
        jsonFile.write(data)
 
def printJson(dataJson):
    print("============================")
    print(dataJson)
    
def fromFetchToJson(cursor, allData, toList):
    columns = [desc[0] for desc in cursor.description]
    if (toList == True):
        return [dict(zip(columns, row)) for row in allData]
    else:
        return [dict(zip(columns, row)) for row in allData] if len(allData) >= 2 else dict(zip(columns, allData[0]))
        
def updateProduct(conn, cursor, sqlCommand):
    cursor.execute(sqlCommand)
    conn.commit()

def queryProduct(cursor, sqlCommand):
    cursor.execute(sqlCommand)
    return cursor.fetchall()

def insertProduct(cursor, product):
    cursor.execute('''
        INSERT INTO products (
            name,
            price,
            quantity,
            from_city,
            is_available,
            views,
            category
        ) VALUES (?,?,?,?,?,?,?)
    ''', (product.name, product.price, product.quantity, product.from_city, product.is_available, product.views, product.category))

def createTableProducts(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT (255),
            price           FLOAT,
            quantity        INTEGER,
            from_city       TEXT (100),
            is_available    BOOL,
            views           INTEGER,
            category        TEXT (100) DEFAULT NULL,
            updated         INTEGER DEFAULT 0
        )
    ''')

def connectToSQLite(dbName):
    return sqlite3.connect(dbName)

def close_connection(conn):
    conn.commit()
    conn.close()

def getDataTextFile(name, ext):
    allUpdate = []
    with open("assets/data/4/"+fileNameUpdate+".text", 'r') as f:
        rawData = dict()
        for row in f.readlines():
            if(row.__contains__("=====") == True):
                allUpdate.append(rawData)
                rawData = dict()
            else:
                rawData[row.strip().split("::")[0]] = row.strip().split("::")[1]
    return allUpdate

def getDataJsonFile(name, ext):
    data = None
    with open("assets/data/4/"+name+ext, 'r') as jsonFile:
        data = json.load(jsonFile)
    return data

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
    
class Product:
    def __init__(self, name, price, quantity, from_city, is_available, views, category=None, id=None, updated=None):
        self.id = id
        self.name = name
        self.price = price
        self.quantity = quantity
        self.from_city = from_city
        self.is_available = is_available
        self.views = views
        self.category = category
        self.updated = updated

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "price": self.price,
            "quantity": self.quantity,
            "from_city": self.from_city,
            "is_available": self.is_available,
            "views": self.views,
            "category": self.category,
            "updated": self.updated
        }

    @classmethod
    def from_dict(cls, data, haveCategory):
        if haveCategory == True:
            return cls(
                data["name"],
                data["price"],
                data["quantity"],
                data["fromCity"],
                data["isAvailable"],
                data["views"],
                data["category"]
            )
        else:
            return cls(
                data["name"],
                data["price"],
                data["quantity"],
                data["fromCity"],
                data["isAvailable"],
                data["views"]
            )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)
    
# ================================================= #

fileName = "task_4_var_12_product_data"
fileNameUpdate = "task_4_var_12_update_data"

allData = getDataJsonFile(fileName, ".json")
allUpdated = getDataTextFile(fileNameUpdate, ".text")

dbName = 'assets/database/database.db'

conn = connectToSQLite(dbName)
cursor = conn.cursor()

createTableProducts(cursor)

products = queryProduct(cursor, "SELECT * FROM products")

if(len(products) == 0):
    for data in allData:
        productInstance = Product.from_dict(data, str(data.keys()).__contains__("category") == True)
        insertProduct(cursor, productInstance)
        
    for update in allUpdated:
        name = update["name"]
        method = update["method"]
        param = update["param"]
        
        if(method == "remove"):
            updateProduct(conn, cursor, f"DELETE FROM products WHERE name = '{name}'")
        elif(method == "price_abs"):
            queryResult = queryProduct(cursor, f"SELECT id, price, updated FROM products WHERE name = '{name}'")
            dataJsonResult = fromFetchToJson(cursor, queryResult, toList=True)
            for dataJson in dataJsonResult:
                updated = dataJson["updated"] + 1
                id = dataJson["id"]
                updateProduct(conn, cursor, f"UPDATE products SET price = {abs(float(param))}, updated = {updated} WHERE id = {id}")
        elif(method == "quantity_sub"):
            queryResult = queryProduct(cursor, f"SELECT id, quantity, updated FROM products WHERE name = '{name}'")
            dataJsonResult = fromFetchToJson(cursor, queryResult, toList=True)
            for dataJson in dataJsonResult:
                updated = dataJson["updated"] + 1
                id = dataJson["id"]
                quantity = dataJson["quantity"] - abs(float(param))
                if(quantity >= 0):
                    updateProduct(conn, cursor, f"UPDATE products SET quantity = {quantity}, updated = {updated} WHERE id = {id}")
        elif(method == "available"):
            queryResult = queryProduct(cursor, f"SELECT id, is_available, updated FROM products WHERE name = '{name}'")
            dataJsonResult = fromFetchToJson(cursor, queryResult, toList=True)
            for dataJson in dataJsonResult:
                updated = dataJson["updated"] + 1
                id = dataJson["id"]
                updateProduct(conn, cursor, f"UPDATE products SET is_available = {param}, updated = {updated} WHERE id = {id}")
        elif(method == "quantity_add"):
            queryResult = queryProduct(cursor, f"SELECT id, quantity, updated FROM products WHERE name = '{name}'")
            dataJsonResult = fromFetchToJson(cursor, queryResult, toList=True)
            for dataJson in dataJsonResult:
                updated = dataJson["updated"] + 1
                id = dataJson["id"]
                quantity = dataJson["quantity"] + abs(float(param))
                if(quantity >= 0):
                    updateProduct(conn, cursor, f"UPDATE products SET quantity = {quantity}, updated = {updated} WHERE id = {id}")
        elif(method == "price_percent"):
            queryResult = queryProduct(cursor, f"SELECT id, price, updated FROM products WHERE name = '{name}'")
            dataJsonResult = fromFetchToJson(cursor, queryResult, toList=True)
            for dataJson in dataJsonResult:
                updated = dataJson["updated"] + 1
                id = dataJson["id"]
                price = dataJson["price"]
                price *= (1+float(param))
                if(price >= 0):
                    updateProduct(conn, cursor, f"UPDATE products SET price = {price}, updated = {updated} WHERE id = {id}")

# 1) вывести топ-10 самых обновляемых товаров
sqlCommand = f'SELECT * FROM products ORDER BY updated DESC LIMIT 10'
queryResult = queryProduct(cursor, sqlCommand)
dataJson = fromFetchToJson(cursor, queryResult, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_1.json", dataJsonResult)

# 2) проанализировать цены товаров, найдя (сумму, мин, макс, среднее) для каждой группы, а также количество товаров в группе
sqlCommand = f'SELECT category, COUNT(*) AS totalData, SUM(price) AS sumPrice, MIN(price) AS minPrice, MAX(price) as maxPrice, AVG(price) as avgPrice FROM products GROUP BY category'
queryResult = queryProduct(cursor, sqlCommand)
dataJson = fromFetchToJson(cursor, queryResult, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_2.json", dataJsonResult)

# 3) проанализировать остатки товаров, найдя (сумму, мин, макс, среднее) для каждой группы товаров
sqlCommand = f'SELECT category, SUM(quantity) AS sumQuantity, MIN(quantity) AS minQuantity, MAX(quantity) as maxQuantity, AVG(quantity) as avgQuantity FROM products GROUP BY category'
queryResult = queryProduct(cursor, sqlCommand)
dataJson = fromFetchToJson(cursor, queryResult, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_3.json", dataJsonResult)

close_connection(conn)