import pickle
import sqlite3 as sqlite
import json

varian = 12
fileName1 = "task_3_var_12_part_1"
fileName2 = "task_3_var_12_part_2"

def initDB():
    connection = sqlite.connect('assets/database/database.db')
    db = connection.cursor()

    db.execute("""
            CREATE TABLE IF NOT EXISTS songs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            artist              TEXT (255),
            song                TEXT (255),
            duration            INTEGER,
            year                INTEGER,
            tempo               FLOAT,
            genre               TEXT (255),
            acousticness        FLOAT,
            instrumentalness    FLOAT,
            energy              FLOAT,
            loudness            FLOAT,
            explicit            BOOL,
            popularity          INTEGER
            )""")
    connection.commit()
    return connection

connection = initDB()
connection.row_factory = sqlite.Row
db = connection.cursor()

allData1 = []
with open("assets/data/3/"+fileName1+".pkl", 'rb') as f:
    loadAllDataFile1 = pickle.load(f)
    for data in loadAllDataFile1:
        allData1.append(
            (
                data["artist"],
                data["song"],
                int(data["duration_ms"]),
                int(data["year"]),
                float(data["tempo"]),
                data["genre"],
                float(data["acousticness"]),
                float(data["energy"]),
                int(data["popularity"])
            )
        )
        
allData2 = []   
with open("assets/data/3/"+fileName2+".text", 'r') as f:
    allDataFile2 = []
    rawData = dict()
    for row in f.readlines():
        if(row.__contains__("=====") == True):
            allDataFile2.append(rawData)
            rawData = dict()
        else:
            if(row.strip().split("::")[0] == "duration_ms"):
                rawData["duration"] = int(row.strip().split("::")[1])
            elif(row.strip().split("::")[0] == "year"):
                rawData[row.strip().split("::")[0]] = int(row.strip().split("::")[1])
            elif(row.strip().split("::")[0] == "explicit"):
                rawData[row.strip().split("::")[0]] = bool(row.strip().split("::")[1] == "True")
            elif(row.strip().split("::")[0] == "tempo" or row.strip().split("::")[0] == "instrumentalness" or row.strip().split("::")[0] == "loudness"):
                rawData[row.strip().split("::")[0]] = float(row.strip().split("::")[1])
            else:
                rawData[row.strip().split("::")[0]] = row.strip().split("::")[1]
                
    for data in allDataFile2:
        allData2.append(
            (
                data["artist"],
                data["song"],
                data["duration"],
                data["year"],
                data["tempo"],
                data["genre"],
                data["instrumentalness"],
                data["loudness"],
                data["explicit"]
            )
        )
        
result = db.execute("SELECT * FROM songs")

if(result.fetchone() == None):
    db.executemany("""INSERT INTO songs
                            (artist,
                            song,
                            duration,
                            year,
                            tempo,
                            genre,
                            acousticness,
                            energy,
                            popularity) 
                            VALUES (?,?,?,?,?,?,?,?,?);""",allData1)
    connection.commit()
    
    db.executemany("""INSERT INTO songs
                            (artist,
                            song,
                            duration,
                            year,
                            tempo,
                            genre,
                            instrumentalness,
                            loudness,
                            explicit) 
                            VALUES (?,?,?,?,?,?,?,?,?);""",allData2)
    connection.commit()
    
# TASK 1
result = db.execute(f"SELECT * FROM songs ORDER BY id ASC LIMIT {varian + 10}")
task1 = result.fetchall()
dataTask1 = []
for data in task1:
    dataDic = dict()
    for keys in data.keys():
        if(keys == "genre"):
            dataDic[keys] = data[keys].split(", ")
        else:
            dataDic[keys] = data[keys]
    dataTask1.append(dataDic)

with open("assets/output/3/output_task_1.json", "w") as outfile:
    json.dump(dataTask1, outfile, indent=4, ensure_ascii=False)
    
# TASK 3
result = db.execute(f"SELECT artist, COUNT(artist) AS total FROM songs GROUP BY artist;")
task3 = result.fetchall()
dataTask3 = []
for data in task3:
    dataDic = dict()
    for keys in data.keys():
        dataDic[keys] = data[keys]
    dataTask3.append(dataDic)
    
with open("assets/output/3/output_category.json", "w") as outfile:
    json.dump(dataTask3, outfile, indent=4, ensure_ascii=False)