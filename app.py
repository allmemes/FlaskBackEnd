from flask import Flask, request
from flask_cors import CORS, cross_origin
import sqlite3
import os
import json
import pandas as pd
import numpy as np
import csvprocessing as cp
import geometry_tools as gt


app = Flask(__name__)
app.config["DEBUG"] = True
app.config["APPLICATION_ROOT"] = "/"
app.config.from_object(__name__)
CORS(app, resources={r"/*":{'origins':'http://localhost:8080', "allow_headers":"Access-Control-Allow-Origin"}})


# table names
sqliteTableList = ["PointsTable", "BuffersTable", "LinesTable", "PeaksTable"]

# back end functions
def checkMetaDataBase(DBpath):
    with open('MetaDataBase.json', 'r') as f:
        mataDataBase = json.load(f)
        allPaths = mataDataBase["paths"]
        if DBpath in allPaths:
            return True
        else:
            return False

def insertIntoMetaDataBase(DBpath):
    with open('MetaDataBase.json', 'r+') as f:
        path_data = json.load(f)
        path_data["paths"].append(DBpath)
        f.seek(0)
        json.dump(path_data, f, indent = 4)

def connectAndUpload(DBpath, tableList):
    initialData = {}
    if os.path.exists(DBpath):
        if not checkMetaDataBase(DBpath):
            insertIntoMetaDataBase(DBpath)
        # load every geojson
        connection = sqlite3.connect(DBpath)
        cursor = connection.cursor()
        for i in tableList[1:]:
            data = cursor.execute(f"SELECT * FROM {i}").fetchall()
            if not len(data) == 0:
                initialData[i] = {}
                for j in data:
                    initialData[i][j[1]] = j[0]
                # {tableName: {name1: geometry1, name2: geometry2}}
    else:
        if not checkMetaDataBase(DBpath):
            # create database if not exist
            connection = sqlite3.connect(DBpath)
            cursor = connection.cursor()
            for i,j in enumerate(tableList):
                if i == 0:
                    create_table = '''\
                        CREATE TABLE IF NOT EXISTS {tableName} (
                        "Microsec" INTEGER PRIMARY KEY UNIQUE,
                        "Flight_date" TEXT,
                        "Senselong" REAL NOT NULL,
                        "Senselat" REAL NOT NULL,
                        "CH4" INTEGER,
                        "Peak" INTEGER,
                        "Source_name" TEXT NOT NULL
                        )\
                        '''.format(tableName = j)
                    cursor.execute(create_table)
                else:
                    create_table = '''\
                    CREATE TABLE IF NOT EXISTS {tableName} (
                        "Geometry" TEXT,
                        "Source_name" TEXT
                    )\
                    '''.format(tableName = j)
                    cursor.execute(create_table)
            connection.commit()
            insertIntoMetaDataBase(DBpath)
        else:
            initialData["error"] = "DB missing"
    return initialData

def deleteFromDB(DBpath, tableName, dataName):
    try:
        connection = sqlite3.connect(DBpath)
        cursor = connection.cursor()
        delete_code = '''DELETE FROM {tableName} WHERE Source_name=="{dataName}"'''.format(tableName = tableName, dataName = dataName)
        cursor.execute(delete_code)
        connection.commit()
        return True
    except:
        return False

def insertIntoDB(DBcursor, tableName, dataName, geometry):
    insert_code = '''INSERT INTO {tableName} VALUES (?, ?)'''.format(tableName = tableName)
    DBcursor.execute(insert_code, [geometry, dataName])

# functions to create json.
def createBuff(buffDis, df):
    points = gt.series_to_multipoint(df["points"])
    global sr
    sr = gt.find_utm_zone(points[0].y, points[0].x)
    points = gt.reproject(points, sr)
    buff = points.buffer(buffDis)
    buff = gt.reproject(buff, 4326, sr)
    geo_j = gt.shapely_to_geojson(buff).replace('"', "'")
    return geo_j

def createPath(df, longColumnName, latColumnName):
    points = df[[longColumnName, latColumnName]].to_numpy()
    lineDict = {'type': 'LineString'}
    lineDict['coordinates'] = gt.rdp(points, 0.0001)
    return str(lineDict)

# sniffer drone data
def createSnifferPeaks(buffDis, df):
    # return None
    cp.find_ch4_peaks(df)
    peaks = df[df['Peak'] == True]
    if len(peaks) == 0:
        return None
    points = gt.series_to_multipoint(peaks["points"])
    # sr = gt.find_utm_zone(points[0].y, points[0].x)
    points = gt.reproject(points, sr)
    buff = points.buffer(buffDis)
    buff = gt.reproject(buff, 4326, sr)
    geo_j = gt.shapely_to_geojson(buff).replace('"', "'")
    return geo_j

def addSnifferPoints():
    pass

# inficon data
def addInficonPoints():
    pass


@app.route('/', methods=["GET", "POST"])
def index():
    return ("Welcome to sniffer web app")

# Back end functions.
@app.route('/accessDB', methods=["GET", "POST"])
@cross_origin()
def accessDB():
    if request.method == "POST":
        # create the collection of all database paths.
        if not os.path.exists('MetaDataBase.json'):
            allDBpaths = {"paths": []}
            with open('MetaDataBase.json', 'w') as f:
                f.write(json.dumps(allDBpaths, indent=4))
        global localPath
        localPath = request.json["DBpath"]
        return connectAndUpload(localPath, sqliteTableList)
    return ("Test")

@app.route('/delete/<table>/<dataName>', methods=["DELETE"])
@cross_origin()
def delete(table, dataName):
    if deleteFromDB(localPath, table, dataName):
        return ({"status": 200})
    else:
        return ({"status": 204})

@app.route('/buffer', methods=["GET", "POST"])
@cross_origin()
def buffer():
    if request.method == "POST":
        # obtain buffer distance list.
        bufferList = request.form['bufferText'].split(",")
        bufferIndex = 0
        # prepare json for front end
        returnedJson = {"BuffersTable": {}, "LinesTable": {}, "PeaksTable": {}}
        # connect to database
        connection = sqlite3.connect(localPath)
        cursor = connection.cursor()
        # obtain csv data and related info.
        for i in request.files:
            # 1, obtain csv name, buffer distance, and dataframe
            csvName = i
            bufferDistance = float(bufferList[bufferIndex])
            data = request.files.get(i)
            if len(csvName.split("_")) == 3:
                # 2, SnifferDrone: Ben's algorithm to create geojson.
                csvDf = pd.read_csv(data)
                df = cp.clean_flight_log(csvDf)
                gt.add_points_to_df(df, "SenseLong", "SenseLat")
                # 3, add into database based on types and load onto json. name: csvName-buffer, csvName-peaks.....
                buffJson = createBuff(bufferDistance, df)
                insertIntoDB(cursor, "BuffersTable", csvName+"-buffer", buffJson)
                returnedJson["BuffersTable"][csvName+"-buffer"] = buffJson

                peakJson = createSnifferPeaks(bufferDistance, df)
                if peakJson:
                    insertIntoDB(cursor, "PeaksTable", csvName+"-peaks", peakJson)
                returnedJson["PeaksTable"][csvName+"-peaks"] = peakJson

                pathJson = createPath(df, "SenseLong", "SenseLat")
                insertIntoDB(cursor, "LinesTable", csvName+"-path", pathJson)
                returnedJson["LinesTable"][csvName+"-path"] = pathJson

            else:
                # 2, Inficon: Ben's algorithm to create geojson.
                while data.readline().decode() != '\r\n':
                    pass
                csvDf = pd.read_csv(data)
                df = cp.cleanInficon(csvDf)
                gt.add_points_to_df(df, "Long", "Lat")
                # 3, add into database based on types and load onto json. name: csvName-buffer, csvName-peaks.....
                pathJson = createPath(df, "Long", "Lat")
                insertIntoDB(cursor, "LinesTable", csvName+"-path", pathJson)
                returnedJson["LinesTable"][csvName+"-path"] = pathJson

            bufferIndex += 1

        connection.commit()
        return (returnedJson)

    return ("This is a buffer page")

if __name__ == '__main__':
    app.run()

    # print(deleteFromDB("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/web_app_db.sqlite", "BuffersTable", "buffer_polygon_test"))

    # if not os.path.exists('MetaDataBase.json'):
    #     allDBpaths = {"paths": []}
    #     with open('MetaDataBase.json', 'w') as f:
    #         f.write(json.dumps(allDBpaths, indent=4))
    # print(connectAndUpload("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/web_app_db.sqlite", ["PointsTable", "BuffersTable", "LinesTable", "PeaksTable"]))

    # some sqlite code for testing:
    # INSERT INTO BuffersTable
    # VALUES ("{'type': 'Polygon', 'coordinates': [[[-83.56849193572998, 42.39554091720936], [-83.55626106262207, 42.39344912341609], [-83.55681896209717, 42.397727717992005], [-83.56849193572998, 42.39554091720936]]]}", "buffer_polygon_test")

    # INSERT INTO BuffersTable
    # VALUES ("{'type': 'Polygon', 'coordinates': [[[-83.54278564453125,42.39848832648868],[-83.54570388793944,42.39024791005226],[-83.53274345397949,42.39176929913866],[-83.53008270263672,42.39810802339276],[-83.53763580322266,42.3958261564156],[-83.54278564453125,42.39848832648868]]]}", "buffer_polygon_test_2")

    # INSERT INTO BuffersTable
    # VALUES ("{'type': 'LineString', 'coordinates': [[-83.55939388275146, 42.41135388972042], [-83.5478925704956, 42.411132092017496], [-83.54308605194092, 42.40634740777102], [-83.54304313659667, 42.40102362155978]]}", "line_test")

    # buffer_polygon_test_2
    # {'type': 'Polygon', 'coordinates': [[[-83.54278564453125,42.39848832648868],[-83.54570388793944,42.39024791005226],[-83.53274345397949,42.39176929913866],[-83.53008270263672,42.39810802339276],[-83.53763580322266,42.3958261564156],[-83.54278564453125,42.39848832648868]]]}
    # line_test
    # {'type': 'LineString', 'coordinates': [[-83.55939388275146, 42.41135388972042], [-83.5478925704956, 42.411132092017496], [-83.54308605194092, 42.40634740777102], [-83.54304313659667, 42.40102362155978]]}