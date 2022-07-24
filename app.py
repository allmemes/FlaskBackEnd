from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
import sqlite3


app = Flask(__name__)
app.config["DEBUG"] = True
app.config["APPLICATION_ROOT"] = "/"
app.config.from_object(__name__)
CORS(app, resources={r"/*":{'origins':'http://localhost:8080', "allow_headers":"Access-Control-Allow-Origin"}})


# back end functions
def connectAndUpload(DBpath, tableList):
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
    # load every geojson
    initialData = {}
    for i in tableList[1:]:
        data = cursor.execute(f"SELECT * FROM {i}").fetchall()
        if not len(data) == 0:
            # {name: xxx, geometry: {"type": "Polygon", "coordinates": []}}
            initialData[data[0][1]] = data[0][0]
    return initialData


# @app.route('/', methods=["GET", "POST"])
# def index():
#     return ("Hello World")

# Back end functions.
@app.route('/accessDB', methods=["GET", "POST"])
@cross_origin()
def accessDB():
    if request.method == "POST":
        localPath = request.json["DBpath"]
        sqliteTableList = ["PointsTable", "BuffersTable", "LinesTable", "PeaksTable"]
        return connectAndUpload(localPath, sqliteTableList)
    return ("False")


if __name__ == '__main__':
    app.run()