import numpy as np
import sqlite3
import json
import csv

def saveToJson(name, data):
    with open("assets/output/5/"+name, 'w') as jsonFile:
        jsonFile.write(data)
 
def printJson(dataJson):
    print("============================")
    print(dataJson)
    
def fromFetchToJson(cursor, allData, toList=False):
    columns = [desc[0] for desc in cursor.description]
    if (toList == True):
        return [dict(zip(columns, row)) for row in allData]
    else:
        return [dict(zip(columns, row)) for row in allData] if len(allData) >= 2 else dict(zip(columns, allData[0]))
        
def updateExecute(conn, cursor, sqlCommand):
    cursor.execute(sqlCommand)
    conn.commit()

def queryExecute(cursor, sqlCommand):
    cursor.execute(sqlCommand)
    return cursor.fetchall()

def insertTransaction(cursor, transaction):
    cursor.execute('''
        INSERT INTO transactions (
            date,
            total,
            customer_id,
            book_id
        ) VALUES (?,?,?,?)
    ''', (transaction.date, transaction.total, transaction.customer_id, transaction.book_id))
    
def insertCustomer(cursor, customer):
    cursor.execute('''
        INSERT INTO customers (
            name,
            address,
            age,
            sex
        ) VALUES (?,?,?,?)
    ''', (customer.name, customer.address, customer.age, customer.sex))

def insertBook(cursor, book):
    cursor.execute('''
        INSERT INTO books (
            title,
            writer,
            price,
            category,
            year,
            rating
        ) VALUES (?,?,?,?,?,?)
    ''', (book.title, book.writer, book.price, book.category, book.year, book.rating))

def createTableTransactions(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                TEXT (100),
            total               INTEGER,
            customer_id         FOREIGN KEY (customer_id) REFERENCES customers(id),
            book_id             FOREIGN KEY (book_id) REFERENCES books(id)
        )
    ''')

def createTableCustomers(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT (255),
            address     TEXT (255),
            age         INTEGER,
            sex         TEXT (50)
        )
    ''')
    
def createTableBooks(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS books (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            title           TEXT (255),
            writer          TEXT (255),
            price           FLOAT,
            category        TEXT (100),
            year            INTEGER,
            rating          FLOAT
        )
    ''')

def connectToSQLite(dbName):
    return sqlite3.connect(dbName)

def close_connection(conn):
    conn.commit()
    conn.close()

def getDataCsvFile(name, ext):
    data = []
    with open("assets/data/5/"+name+ext, 'r') as csvFile:
        csvReader = csv.DictReader(csvFile)
        for row in csvReader:
            data.append(row)
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
    
class Book:
    def __init__(self, title, writer, price, category, year, rating, id=None):
        self.id = id
        self.title = title
        self.writer = writer
        self.price = price
        self.category = category
        self.year = year
        self.rating = rating

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "writer": self.writer,
            "price": self.price,
            "category": self.category,
            "year": self.year,
            "rating": self.rating
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["title"],
            data["writer"],
            data["price"],
            data["category"],
            data["year"],
            data["rating"]
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)

class Customer:
    def __init__(self, name, address, age, sex, id=None):
        self.id = id
        self.name = name
        self.address = address
        self.age = age
        self.sex = sex

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "address": self.address,
            "age": self.age,
            "sex": self.sex
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["name"],
            data["address"],
            data["age"],
            data["sex"]
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)
 
class Transaction:
    def __init__(self, date, total, customer_id, book_id, id=None):
        self.id = id
        self.date = date
        self.total = total
        self.customer_id = customer_id
        self.book_id = book_id

    def to_dict(self):
        return {
            "id": self.id,
            "date": self.date,
            "total": self.total,
            "customer_id": self.customer_id,
            "book_id": self.book_id
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["date"],
            data["total"],
            data["customer_id"],
            data["book_id"]
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)

# ================================================= #

bookFileName = "books"
customerFileName = "customers"
transactionFileName = "transactions"
ext = ".csv"

allBooks = getDataCsvFile(bookFileName, ext)
allCustomers = getDataCsvFile(customerFileName, ext)
allTransactions = getDataCsvFile(transactionFileName, ext)

