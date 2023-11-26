import pickle
import sqlite3 as sqlite
import json

fileName = "task_1_var_12_item"
varian = 12

def initDB():
    connection = sqlite.connect('assets/database/database.db')
    db = connection.cursor()

    db.execute("""
            CREATE TABLE IF NOT EXISTS games (
            id           INTEGER    PRIMARY KEY ASC,
            name         TEXT (255),
            city         TEXT (100),
            begin        TEXT (100),
            system       TEXT (100),
            tours_count  INTEGER,
            min_rating   INTEGER,
            time_on_game INTEGER
            )""")
    connection.commit()
    return connection
    
outputAllData = []
dataStatistic = dict()
outputCity = []
with open("assets/data/1/"+fileName+".pkl", 'rb') as f:
    allDataFile = pickle.load(f)
    # allDataFile = sorted(allDataFile,  key=lambda k: k["id"],  reverse=False)
    
    connection = initDB()
    connection.row_factory = sqlite.Row
    db = connection.cursor()
    
    result = db.execute("SELECT * FROM games")
    
    if(result.fetchone() == None):
        for data in allDataFile:
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
    
    db.execute(f"SELECT * FROM games ORDER BY id ASC LIMIT {varian+10}")
    allDataVarian = db.fetchall()
    
    totalValueMinRating = 0
    totalValueTimeOnGame = 0
    allMinRating = []
    allTimeOnGame = []
    uniqueCity = []
    
    for data in allDataVarian:
        outputAllData.append(
            {
                "id": data['id'],
                "name": data['name'],
                "city": data['city'],
                "begin": data['begin'],
                "system": data['system'],
                "tours_count": data['tours_count'],
                "min_rating": data['min_rating'],
                "time_on_game": data['time_on_game']
            }
        )
        totalValueMinRating += int(data['min_rating'])
        totalValueTimeOnGame += int(data['time_on_game'])
        allMinRating.append(int(data['min_rating']))
        allTimeOnGame.append(int(data['time_on_game']))
        if(data['city'] not in uniqueCity):
            uniqueCity.append(data['city'])
    
    allMinRating = sorted(allMinRating, reverse=False)
    allTimeOnGame = sorted(allTimeOnGame, reverse=False)
    
    minValueMinRating = min(allMinRating)
    maxValueMinRating = max(allMinRating)
    minValueTimeOnGame = min(allTimeOnGame)
    maxValueTimeOnGame = max(allTimeOnGame)
    
    avgMinRating = totalValueMinRating / len(allMinRating)
    avgTimeOnGame = totalValueTimeOnGame / len(allTimeOnGame)
    
    for city in uniqueCity:
        total = 0
        for data in allDataVarian:
            if(city == data['city']):
                total += 1
        outputCity.append(
            {"city": city, "total": total}
        )
    
    dataStatistic["totalRating"] = totalValueMinRating
    dataStatistic["meanRating"] = avgMinRating
    dataStatistic["minRating"] = minValueMinRating
    dataStatistic["maxRating"] = maxValueMinRating
    dataStatistic["totalGameTime"] = totalValueTimeOnGame
    dataStatistic["meanGameTime"] = avgTimeOnGame
    dataStatistic["minGameTime"] = minValueTimeOnGame
    dataStatistic["maxGameTime"] = maxValueTimeOnGame
    
    
with open("assets/output/1/output.json", "w") as outfile:
    json.dump(outputAllData, outfile, indent=4, ensure_ascii=False)
with open("assets/output/1/output_categories.json", "w") as outfile:
    json.dump(outputCity, outfile, indent=4, ensure_ascii=False)
with open("assets/output/1/output_statistic.json", "w") as outfile:
    json.dump(dataStatistic, outfile, indent=4, ensure_ascii=False)