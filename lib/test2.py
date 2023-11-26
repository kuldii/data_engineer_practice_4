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


with open("assets/data/2/"+fileName+".pkl", 'rb') as f:
    allDataFile = pickle.load(f)
    
    connection = initDB()
    connection.row_factory = sqlite.Row
    db = connection.cursor()
    
    allDataDetail = []
    
    for data in allDataFile:
        id = getIdByName(db, data["name"])
        dataDetail = (id,data["name"],data["place"],data["prise"])
        allDataDetail.append(dataDetail)
        
    db.executemany("""INSERT INTO detail_games
                          (game_id, name, place, price) 
                          VALUES (?, ?, ?, ?);""",allDataDetail)
    connection.commit()
    
    