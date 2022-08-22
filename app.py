from flask import Flask, request
from flask_cors import CORS, cross_origin
import sqlite3
import os
import json
import pandas as pd
import numpy as np
import csvprocessing as cp
import geometry_tools as gt
from shapely.geometry import MultiPoint, mapping
import urllib
import urllib.request
import collections
import multiprocessing as mp


################
# Flask set up #
################

app = Flask(__name__)
app.config["DEBUG"] = True
app.config["APPLICATION_ROOT"] = "/"
app.config.from_object(__name__)
CORS(app, resources={r"/*":{'origins':'http://localhost:8080', "allow_headers":"Access-Control-Allow-Origin"}})

####################
# Global variables #
####################

sqliteTableList = ["PointsTable", "BuffersTable", "LinesTable", "PeaksTable"]
bufferResolution = 6

####################################
# basic database related functions #
####################################

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
                        "Source_name" TEXT NOT NULL,
                        "Utmlong" REAL NOT NULL,
                        "Utmlat" REAL NOT NULL
                        )\
                        '''.format(tableName = j)
                    cursor.execute(create_table)
                else:
                    create_table = '''\
                    CREATE TABLE IF NOT EXISTS {tableName} (
                        "Geometry" TEXT,
                        "Source_name" TEXT,
                        "EsriGeometry" TEXT
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

def insertGeoJsonIntoDB(DBcursor, tableName, dataName, geometry, esriGeo):
    insert_code = '''INSERT INTO {tableName} VALUES (?, ?, ?)'''.format(tableName = tableName)
    DBcursor.execute(insert_code, [geometry, dataName, esriGeo])

##############################################
# functions to handle geojson and conversion #
##############################################

# convert geojson into esri geometry for polygons only.
def toEsriGeometry(geoJson):
    esriGeometry = {'rings': []}
    if len(geoJson["coordinates"]) > 1:
        for i in geoJson["coordinates"]:
            esriGeometry['rings'].append(i[0])
    else:
        for i in geoJson["coordinates"]:
            esriGeometry['rings'].append(i)
    return json.dumps(esriGeometry)

# functions to create buffer and path json.
def createBuff(points, buffDis, sr):
    buff = points.buffer(buffDis, resolution=bufferResolution)
    # create esriJson
    rawJson = mapping(buff)
    esriJson = toEsriGeometry(rawJson).replace('"', "'")
    # create geoJson
    buff = gt.reproject(buff, 4326, sr)
    geo_j = gt.shapely_to_geojson(buff).replace('"', "'")
    return geo_j, esriJson

def createPath(df):
    points = df[["SenseLong", "SenseLat"]].to_numpy()
    lineDict = {'type': 'LineString'}
    lineDict['coordinates'] = gt.rdp(points, 0.0001)
    return str(lineDict)

# sniffer drone peaks creating function
def createSnifferPeaks(points, sr, df, conn):
    cp.find_ch4_peaks(df)
    # 1, add utmlong and utmlat colums.
    df["Utmlong"] = [i.x for i in points]
    df["Utmlat"] = [i.y for i in points]
    # 2, add the new df into sqlite point table
    df[["Microsec", "Flight_Date", "SenseLong", "SenseLat", "CH4", "Peak", "Source_Name", "Utmlong", "Utmlat"]].to_sql(name="PointsTable", con=conn, if_exists='append', index=False)
    # 3, filter peaks
    peaks = df[df['Peak'] == 1]
    if len(peaks) == 0:
        return None
    peakList = []
    for i in peaks.index:
        peakList.append((points[i].x, points[i].y))
    peakPoints = MultiPoint(peakList)
    # 4, create bufer geojson.
    buff = peakPoints.buffer(13.58, resolution=bufferResolution)
    buff = gt.reproject(buff, 4326, sr)
    geo_j = gt.shapely_to_geojson(buff).replace('"', "'")
    return geo_j

#############################
# Arcgis rest api functions #
#############################

def get_token(userName, passWord):
    referer = "http://www.arcgis.com/"
    query_dict = {'username': userName, 'password': passWord,
                    'referer': referer, 'expiration': 900}
    query_string = urllib.parse.urlencode(query_dict).encode('utf-8')
    url = "https://www.arcgis.com/sharing/rest/generateToken"
    token = json.loads(urllib.request.urlopen(url + "?f=json", query_string).read())

    if "token" not in token:
        print(token['error'])
        return None
    else:
        return token["token"]

def add_feature(token, features, targetUrl):
    query_dict = {"f": "json",
                    "token": token,
                    "features": features
                    }
    urllib.request.urlopen(targetUrl + r"/addFeatures", urllib.parse.urlencode(query_dict).encode('utf-8'))
    # jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
    # print (jsonOutput['addResults'])

def delete_all(token, targetUrl):
    delete_dict = {"f": "json",
                    "token": token,
                    "where": "OBJECTID >= 0"
                    }
    urllib.request.urlopen(targetUrl + r"/deleteFeatures", urllib.parse.urlencode(delete_dict).encode('utf-8'))
    # jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
    # print (jsonOutput['addResults'])

##################################
# communicate with the front end #
##################################

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
    if table[0] == "L":
        deleteFromDB(localPath, "PointsTable", dataName)
    if deleteFromDB(localPath, table, dataName):
        return ({"status": 200})
    else:
        return ({"status": 204})

@app.route('/buffer', methods=["GET", "POST"])
@cross_origin()
def buffer():
    if request.method == "POST":
        # obtain buffer distance list, and inspection type.
        bufferList = request.form['bufferText'].split(",")
        insepctionType = request.form['task']
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
                cleanedDf = cp.clean_flight_log(csvName+"-path", csvDf)

                # 3, add a column of points
                gt.add_points_to_df(cleanedDf)
                points = gt.series_to_multipoint(cleanedDf["points"])
                # 4, pre project the points to prepare for buffer.
                sr = gt.find_utm_zone(points[0].y, points[0].x)
                points = gt.reproject(points, sr)

                # 5, add into database based on types and load onto json. name: csvName-buffer, csvName-peaks.....
                buffJson = createBuff(points, bufferDistance, sr)
                insertGeoJsonIntoDB(cursor, "BuffersTable", csvName+"-buffer", buffJson[0], buffJson[1])
                returnedJson["BuffersTable"][csvName+"-buffer"] = buffJson[0]

                # while creating peaks, also insert points into point table.
                peakJson = createSnifferPeaks(points, sr, cleanedDf, connection)
                if peakJson:
                    insertGeoJsonIntoDB(cursor, "PeaksTable", csvName+"-peaks", peakJson, "")
                returnedJson["PeaksTable"][csvName+"-peaks"] = peakJson

                pathJson = createPath(cleanedDf)
                insertGeoJsonIntoDB(cursor, "LinesTable", csvName+"-path", pathJson, "")
                returnedJson["LinesTable"][csvName+"-path"] = pathJson

            else:
                # 2, Inficon: Ben's algorithm to create geojson.
                while data.readline().decode() != '\r\n':
                    pass
                csvDf = pd.read_csv(data)
                cleanedDf = cp.cleanInficon(csvName+"-buffer", csvDf)

                # 3, add a column of points
                gt.add_points_to_df(cleanedDf)
                points = gt.series_to_multipoint(cleanedDf["points"])
                # 4, pre project the points to prepare for buffer.
                sr = gt.find_utm_zone(points[0].y, points[0].x)
                points = gt.reproject(points, sr)

                # 5, add utm points into points table.
                cleanedDf["Utmlong"] = [i.x for i in points]
                cleanedDf["Utmlat"] = [i.y for i in points]

                # 6, make the new df into sql
                cleanedDf[["Microsec", "Flight_Date", "SenseLong", "SenseLat", "CH4", "Peak", "Source_Name", "Utmlong", "Utmlat"]].to_sql(name="PointsTable", con=connection, if_exists='append', index=False)

                # 7, add into database based on types and load onto json. name: csvName-buffer, csvName-peaks.....
                buffJson = createBuff(points, bufferDistance, sr)
                insertGeoJsonIntoDB(cursor, "BuffersTable", csvName+"-buffer", buffJson[0], buffJson[1])
                returnedJson["BuffersTable"][csvName+"-buffer"] = buffJson[0]

                pathJson = createPath(cleanedDf)
                insertGeoJsonIntoDB(cursor, "LinesTable", csvName+"-path", pathJson, "")
                returnedJson["LinesTable"][csvName+"-path"] = pathJson

            bufferIndex += 1

        connection.commit()
        return (returnedJson)

    return ("This is a buffer page")

@app.route('/append', methods=["GET", "POST"])
@cross_origin()
def append():
    if request.method == "POST":
        # start the database.
        connection = sqlite3.connect(localPath)
        cursor = connection.cursor()

        # authentication
        userName = request.form["userName"]
        passWord = request.form["passWord"]
        token = get_token(userName, passWord)

        # get append function information
        sourceLayers = request.form["sourceLayers"].split(",")
        inspectionType = request.form["task"]

        # prepare point and buffer feature list for appending, two inspections have both those two feature types.
        pointFeatures, bufferFeatures = [], []

        # append operation based on inspection type
        if inspectionType == "S":
            peaksFeatures = []
            bufferUrl = request.form["bufferUrl"]
            peaksUrl = request.form["peaksUrl"]
            pointsUrl = request.form["pointsUrl"]

            for i in sourceLayers:
                if i[-1] == "r":
                    query = cursor.execute("SELECT EsriGeometry FROM BuffersTable WHERE Source_name == '" + i + "'").fetchall()[0][0]
                    esriGeometry = json.loads(query.replace("'", '"'))
                    uploadStruct = {
                        "attributes" : {},
                        "geometry" : esriGeometry
                    }
                    bufferFeatures.append(uploadStruct)

                elif i[-1] == "s":
                    pass

                else:
                    query = cursor.execute("SELECT Flight_date, Senselat, Senselong, CH4, Source_name, Utmlong, Utmlat FROM PointsTable WHERE Source_name == '" + i + "'").fetchall()
                    for i in query:
                        esriPoint = {"attributes" : {
                                        "Flight_Date": i[0],
                                        "SenseLat": i[1],
                                        "SenseLong": i[2],
                                        "CH4": i[3],
                                        "Source_Name" : i[4].split("-")[0]
                                        },
                                        "geometry" :
                                        {
                                            "x" : i[5],
                                            "y" : i[6]
                                        }}
                        pointFeatures.append(esriPoint)

            # Multiprocess add.
            # add_feature(token, bufferFeatures, bufferUrl)
            bufferAppend = mp.Process(target=add_feature, args=[token, bufferFeatures, bufferUrl])
            # add_feature(token, pointFeatures, pointsUrl)
            pointsAppend = mp.Process(target=add_feature, args=[token, pointFeatures, pointsUrl])
            # add_feature(token, pointFeatures, pointsUrl)
            # peaksAppend = mp.Process(target=add_feature, args=[token, peaksFeatures, peaksUrl])
            bufferAppend.start()
            pointsAppend.start()
            # peaksAppend.start()
            bufferAppend.join()
            pointsAppend.join()
            # peaksAppend.join()

        else:
            inficonPointsUrl = request.form["inficonPoints"]
            inficonBufferUrl = request.form["inficonBuffer"]

            for i in sourceLayers:
                if i[-1] == "r":
                    pass
                else:
                    pass

        return ({"sourceLayer": request.form["sourceLayers"]})

    return ("This is for appending function")

@app.route('/reset', methods=["GET", "POST"])
@cross_origin()
def reset():
    if request.method == "POST":
        userName = request.form["userName"]
        passWord = request.form["passWord"]
        token = get_token(userName, passWord)
        inspectionType = request.form["task"]
        if inspectionType == "S":
            bufferUrl = request.form["bufferUrl"]
            peaksUrl = request.form["peaksUrl"]
            pointsUrl = request.form["pointsUrl"]
            # Multiprocess delete
            delete_all(token, bufferUrl)
        else:
            inficonPointsUrl = request.form["inficonPoints"]
            inficonBufferUrl = request.form["inficonBuffer"]
            # Multiprocess delete


if __name__ == '__main__':
    app.run()