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
    ''', (customers.name, customers.address, customers.age, customers.sex))

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
            customer_id         INTEGER,
            book_id             INTEGER,
            FOREIGN KEY (customer_id) REFERENCES customers(id),
            FOREIGN KEY (book_id) REFERENCES books(id)
        )
    ''')

def createTableCustomers(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT (255),
            address         TEXT (255),
            age             INTEGER,
            sex             TEXT (50)
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

dbName = 'assets/database/database.db'

conn = connectToSQLite(dbName)
cursor = conn.cursor()

createTableBooks(cursor)
createTableCustomers(cursor)
createTableTransactions(cursor)

books = queryExecute(cursor, "SELECT * FROM books")
customers = queryExecute(cursor, "SELECT * FROM customers")
transactions = queryExecute(cursor, "SELECT * FROM transactions")

if(len(books) == 0 and len(customers) == 0 and len(transactions) == 0):
    for book in allBooks:
        bookInstance = Book.from_dict(book)
        insertBook(cursor, bookInstance)
        
    for customer in allCustomers:
        customerInstance = Customer.from_dict(customer)
        insertCustomer(cursor, customerInstance)
        
    for transaction in allTransactions:
        transactionInstance = Transaction.from_dict(transaction)
        insertTransaction(cursor, transactionInstance)
        
query_1 = "SELECT * FROM books WHERE price > 20 ORDER BY rating DESC LIMIT 10"
result_1 = queryExecute(cursor, query_1)
dataJson = fromFetchToJson(cursor, result_1, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_1.json", dataJsonResult)
        
query_2 = "SELECT COUNT(*) AS total_transaction FROM transactions"
result_2 = queryExecute(cursor, query_2)
dataJson = fromFetchToJson(cursor, result_2, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_2.json", dataJsonResult)
        
query_3 = "SELECT writer, SUM(total) AS total_books FROM books JOIN transactions ON books.id = transactions.book_id GROUP BY writer"
result_3 = queryExecute(cursor, query_3)
dataJson = fromFetchToJson(cursor, result_3, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_3.json", dataJsonResult)
        
query_4 = "UPDATE books SET price = 25.00 WHERE id = 100"
updateExecute(conn, cursor, query_4)
saveToJson("output_4.json", '{"result":"Update book price (id = 100) successful."}')
        
query_5 = "SELECT customers.name, SUM(books.price * transactions.total) AS total_price FROM customers JOIN transactions ON customers.id = transactions.customer_id JOIN books ON transactions.book_id = books.id GROUP BY customers.name ORDER BY total_price DESC"
result_5 = queryExecute(cursor, query_5)
dataJson = fromFetchToJson(cursor, result_5, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_5.json", dataJsonResult)
        
query_6 = "SELECT AVG(price) AS avg_price FROM books"
result_6 = queryExecute(cursor, query_6)
dataJson = fromFetchToJson(cursor, result_6, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_6.json", dataJsonResult)
        
query_7 = "SELECT books.title, SUM(transactions.total) AS total_sell FROM books JOIN transactions ON books.id = transactions.book_id GROUP BY books.title ORDER BY total_sell DESC LIMIT 10"
result_7 = queryExecute(cursor, query_7)
dataJson = fromFetchToJson(cursor, result_7, toList=False)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_7.json", dataJsonResult)

close_connection(conn)