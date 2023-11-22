import pickle
import sqlite3

fileName = "task_1_var_12_item"

with open("assets/data/1/"+fileName+".pkl", 'rb') as f:
    allData = pickle.load(f)
    allData = sorted(allData,  key=lambda k: k["id"],  reverse=False)
    
    connection = sqlite3.connect('assets/database/database1.db')
    db = connection.cursor()
    
    try:
        db.execute("SELECT * FROM games")
    except:
        db.execute("""
            CREATE TABLE games (
            id           INTEGER    PRIMARY KEY ASC,
            name         TEXT (255),
            city         TEXT (100),
            begin        TEXT (100),
            system       TEXT (100),
            tours_count  INTEGER,
            min_rating   INTEGER,
            time_on_game INTEGER
            )""")
        
    for data in allData:
        db.execute("""
            INSERT INTO games VALUES (
            %s,
            '%s',
            '%s',
            '%s',
            '%s',
            %s,
            %s,
            %s
            )
            """%(data["id"],data["name"],data["city"],data["begin"],data["system"],data["tours_count"],data["min_rating"],data["time_on_game"]),)
        
    connection.commit()
    connection.close()