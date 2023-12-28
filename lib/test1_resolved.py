import numpy as np
import sqlite3
import pickle
import json

def printJson(dataJson):
    print("============================")
    print(dataJson)

def fromFetchToJson(allData):
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in allData] if len(allData) >= 2 else dict(zip(columns, allData[0]))

def queryGame(cursor, sqlCommand):
    cursor.execute(sqlCommand)
    return cursor.fetchall()

def insertGame(cursor, game):
    cursor.execute('''
        INSERT INTO games (
            id,
            name,
            city,
            begin,
            system,
            tours_count,
            min_rating,
            time_on_game
        ) VALUES (?,?,?,?,?,?,?,?)
    ''', (game.id, game.name, game.city, game.begin, game.system, game.tours_count, game.min_rating, game.time_on_game))

def createTableGames(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS games (
            id           INTEGER    PRIMARY KEY ASC,
            name         TEXT (255),
            city         TEXT (100),
            begin        TEXT (100),
            system       TEXT (100),
            tours_count  INTEGER,
            min_rating   INTEGER,
            time_on_game INTEGER
        )
    ''')

def connectToSQLite(dbName):
    return sqlite3.connect(dbName)

def close_connection(conn):
    conn.commit()
    conn.close()

def saveToJson(name, data):
    with open("assets/output/1/resolved/"+name, 'w') as jsonFile:
        jsonFile.write(data)
        
def getDataPklFile(name, ext):
    data = None
    with open("assets/data/1/"+name+ext, 'rb') as pklFile:
        data = pickle.load(pklFile)
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
    
class Game:
    def __init__(self, id, name, city, begin, system, tours_count, min_rating, time_on_game):
        self.id = id
        self.name = name
        self.city = city
        self.begin = begin
        self.system = system
        self.tours_count = tours_count
        self.min_rating = min_rating
        self.time_on_game = time_on_game
        
    def to_dict(self):
        return {
            "id" : self.id,
            "name" : self.name,
            "city" : self.city,
            "begin" : self.begin,
            "system" : self.system,
            "tours_count" : self.tours_count,
            "min_rating" : self.min_rating,
            "time_on_game" : self.time_on_game
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["id"],
            data["name"],
            data["city"],
            data["begin"],
            data["system"],
            data["tours_count"],
            data["min_rating"],
            data["time_on_game"]
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)

# ================================================= #

fileName = "task_1_var_12_item"
varian = 12

allData = getDataPklFile(fileName, ".pkl")

dbName = 'assets/database/resolved/database.db'

conn = connectToSQLite(dbName)
cursor = conn.cursor()

createTableGames(cursor)

games = queryGame(cursor, "SELECT * FROM games")

if(len(games) == 0):
    for data in allData:
        gameInstance = Game.from_dict(data)
        insertGame(cursor, gameInstance)

# 1) вывод первых (VAR+10) отсортированных по произвольному числовому полю строк из таблицы в файл формата json;
sqlCommand = f'SELECT * FROM games ORDER BY id LIMIT {varian + 10}'
queryResult = queryGame(cursor, sqlCommand)
dataJson = fromFetchToJson(queryResult)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_1.json", dataJsonResult)

# 2) вывод (сумму, мин, макс, среднее) по произвольному числовому полю;
sqlCommand = f'SELECT SUM(min_rating) AS sumRating, MIN(min_rating) AS minRating, MAX(min_rating) as maxRating, AVG(min_rating) as avgRating, SUM(time_on_game) AS sumGameTime, MIN(time_on_game) AS minGameTime, MAX(time_on_game) as maxGameTime, AVG(time_on_game) as avgGameTime FROM games'
queryResult = queryGame(cursor, sqlCommand)
dataJson = fromFetchToJson(queryResult)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
printJson(dataJsonResult)

# 3) вывод частоты встречаемости для категориального поля;
sqlCommand = f'SELECT city, COUNT(*) as total FROM games GROUP BY city'
queryResult = queryGame(cursor, sqlCommand)
dataJson = fromFetchToJson(queryResult)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
printJson(dataJsonResult)

# 4) вывод первых (VAR+10) отфильтрованных по произвольному предикату отсортированных по произвольному числовому полю строк из таблицы в файл формате json.
sqlCommand = f'SELECT * FROM games WHERE min_rating >= 2500 and min_rating <= 2600 ORDER BY id LIMIT {varian + 10}'
queryResult = queryGame(cursor, sqlCommand)
dataJson = fromFetchToJson(queryResult)
dataJsonResult = json.dumps(dataJson, indent=4, cls=NpEncoder, ensure_ascii=False)
saveToJson("output_2.json", dataJsonResult)

close_connection(conn)