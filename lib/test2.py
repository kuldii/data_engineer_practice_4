import pickle
import sqlite3 as sqlite
import json

fileName = "task_2_var_12_subitem"

def initDB():
    connection = sqlite.connect('assets/database/database.db')
    db = connection.cursor()

    db.execute("""
            CREATE TABLE IF NOT EXISTS detail_games (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER REFERENCES games (id),
            name    TEXT (255),
            place   INTEGER,
            price   INTEGER
            )""")
    connection.commit()
    return connection

def getIdByName(db, name):
    result = db.execute(f"SELECT * FROM games WHERE name = '{name}'")
    return result.fetchone()["id"]

output = []

with open("assets/data/2/"+fileName+".pkl", 'rb') as f:
    allDataFile = pickle.load(f)
    
    connection = initDB()
    connection.row_factory = sqlite.Row
    db = connection.cursor()
    
    result = db.execute("SELECT * FROM detail_games")
    
    if(result.fetchone() == None):
        allDataDetail = []
        
        for data in allDataFile:
            id = getIdByName(db, data["name"])
            dataDetail = (id,data["name"],data["place"],data["prise"])
            allDataDetail.append(dataDetail)
        
        db.executemany("""INSERT INTO detail_games
                            (game_id, name, place, price) 
                            VALUES (?, ?, ?, ?);""",allDataDetail)
        connection.commit()
    
    result = db.execute("SELECT games.id as id, games.name as name, city, begin, system, tours_count, min_rating, time_on_game, place, price  FROM games,detail_games WHERE games.id = detail_games.game_id")
    
    
    for data in result.fetchall():
        dataOutput = dict()
        dataOutput["id"] = data["id"]
        dataOutput["name"] = data["name"]
        dataOutput["city"] = data["city"]
        dataOutput["begin"] = data["begin"]
        dataOutput["system"] = data["system"]
        dataOutput["toursCount"] = data["tours_count"]
        dataOutput["minRating"] = data["min_rating"]
        dataOutput["timeOnGame"] = data["time_on_game"]
        dataOutput["place"] = data["place"]
        dataOutput["price"] = data["price"]
        
        output.append(dataOutput)
        
with open("assets/output/2/output.json", "w") as outfile:
    json.dump(output, outfile, indent=4, ensure_ascii=False)