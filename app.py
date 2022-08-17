from flask import Flask, request
from flask_cors import CORS, cross_origin
import sqlite3
import os
import json
import pandas as pd
import numpy as np
import csvprocessing as cp
import geometry_tools as gt
from shapely.geometry import MultiPoint

app = Flask(__name__)
app.config["DEBUG"] = True
app.config["APPLICATION_ROOT"] = "/"
app.config.from_object(__name__)
CORS(app, resources={r"/*":{'origins':'http://localhost:8080', "allow_headers":"Access-Control-Allow-Origin"}})


# table names
sqliteTableList = ["PointsTable", "BuffersTable", "LinesTable", "PeaksTable"]
bufferResolution = 6

# basic database related functions
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
                        "Microsec" INTEGER,
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

# function to add Json into DB
def insertGeoJsonIntoDB(DBcursor, tableName, dataName, geometry):
    insert_code = '''INSERT INTO {tableName} VALUES (?, ?)'''.format(tableName = tableName)
    DBcursor.execute(insert_code, [geometry, dataName])

# functions to create buffer and path json.
def createBuff(points, buffDis, sr):
    buff = points.buffer(buffDis, resolution=bufferResolution)
    buff = gt.reproject(buff, 4326, sr)
    geo_j = gt.shapely_to_geojson(buff).replace('"', "'")
    return geo_j

def createPath(df):
    points = df[["SenseLong", "SenseLat"]].to_numpy()
    lineDict = {'type': 'LineString'}
    lineDict['coordinates'] = gt.rdp(points, 0.0001)
    return str(lineDict)

# sniffer drone peaks creating function
def createSnifferPeaks(points, buffDis, sr, df, conn):
    cp.find_ch4_peaks(df)
    # insert points into point table
    df[["Microsec", "Flight_Date", "SenseLong", "SenseLat", "CH4", "Peak", "Source_Name"]].to_sql(name="PointsTable", con=conn, if_exists='append', index=False)
    # continue creating the json
    peaks = df[df['Peak'] == 1]
    if len(peaks) == 0:
        return None
    peakList = []
    for i in peaks.index:
        peakList.append((points[i].x, points[i].y))
    peakPoints = MultiPoint(peakList)
    buff = peakPoints.buffer(buffDis, resolution=bufferResolution)
    buff = gt.reproject(buff, 4326, sr)
    geo_j = gt.shapely_to_geojson(buff).replace('"', "'")
    return geo_j


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
    if table[0] == "B":
        deleteFromDB(localPath, "PointsTable", dataName)
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
        insepctionType = bufferList[-1]
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
            if insepctionType == "S":
                # 2, SnifferDrone: Ben's algorithm to create geojson.
                csvDf = pd.read_csv(data)
                cleanedDf = cp.clean_flight_log(csvName+"-buffer", csvDf)

                # add a column of points
                gt.add_points_to_df(cleanedDf)
                points = gt.series_to_multipoint(cleanedDf["points"])
                # pre project the points to prepare for buffer.
                sr = gt.find_utm_zone(points[0].y, points[0].x)
                points = gt.reproject(points, sr)

                # 3, add into database based on types and load onto json. name: csvName-buffer, csvName-peaks.....
                buffJson = createBuff(points, bufferDistance, sr)
                insertGeoJsonIntoDB(cursor, "BuffersTable", csvName+"-buffer", buffJson)
                returnedJson["BuffersTable"][csvName+"-buffer"] = buffJson

                # while creating peaks, also insert points into point table.
                peakJson = createSnifferPeaks(points, bufferDistance, sr, cleanedDf, connection)
                if peakJson:
                    insertGeoJsonIntoDB(cursor, "PeaksTable", csvName+"-peaks", peakJson)
                returnedJson["PeaksTable"][csvName+"-peaks"] = peakJson

                pathJson = createPath(cleanedDf)
                insertGeoJsonIntoDB(cursor, "LinesTable", csvName+"-path", pathJson)
                returnedJson["LinesTable"][csvName+"-path"] = pathJson

            else:
                # 2, Inficon: Ben's algorithm to create geojson.
                while data.readline().decode() != '\r\n':
                    pass
                csvDf = pd.read_csv(data)
                cleanedDf = cp.cleanInficon(csvName+"-buffer", csvDf)

                # 3, add points into points table.
                cleanedDf.to_sql(name="PointsTable", con=connection, if_exists='append', index=False)

                # add a column of points
                gt.add_points_to_df(cleanedDf)
                points = gt.series_to_multipoint(cleanedDf["points"])
                # pre project the points to prepare for buffer.
                sr = gt.find_utm_zone(points[0].y, points[0].x)
                points = gt.reproject(points, sr)

                # 4, add into database based on types and load onto json. name: csvName-buffer, csvName-peaks.....
                buffJson = createBuff(points, bufferDistance, sr)
                insertGeoJsonIntoDB(cursor, "BuffersTable", csvName+"-buffer", buffJson)
                returnedJson["BuffersTable"][csvName+"-buffer"] = buffJson

                pathJson = createPath(cleanedDf)
                insertGeoJsonIntoDB(cursor, "LinesTable", csvName+"-path", pathJson)
                returnedJson["LinesTable"][csvName+"-path"] = pathJson

            bufferIndex += 1

        connection.commit()
        return (returnedJson)

    return ("This is a buffer page")


if __name__ == '__main__':
    app.run()