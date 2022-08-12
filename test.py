import array
import sqlite3
import app
import pandas as pd
import numpy as np
import csvprocessing as cp
import geometry_tools as gt
import json
from shapely.geometry import mapping, MultiPoint
import re
import time

# print(app.connectAndUpload("web_app_db.sqlite", ["PointsTable", "BuffersTable", "LinesTable", "PeaksTable"]))

# connection = sqlite3.connect("web_app_db.sqlite")
# cursor = connection.cursor()
# data = cursor.execute(f"SELECT * FROM PeaksTable").fetchall()
# print(data)
# print(data[0][1], data[0][0])

# a = {"type": "Polygon", "coordinates": [[[-83.56849193572998, 42.39554091720936],[-83.55626106262207,42.39344912341609],[-83.55681896209717,42.397727717992005],[-83.56849193572998,42.39554091720936]]]}
# a = {"type": "LineString",
#           "coordinates": [
#             [
#               -83.55939388275146,
#               42.41135388972042
#             ],
#             [
#               -83.5478925704956,
#               42.411132092017496
#             ],
#             [
#               -83.54308605194092,
#               42.40634740777102
#             ],
#             [
#               -83.54304313659667,
#               42.40102362155978
#             ]]}
# print(str(a))

# start = np.tile(np.expand_dims(points[0], axis=0), (points.shape[0], 1))
# end = np.tile(np.expand_dims(points[-1], axis=0), (points.shape[0], 1))
# dist_point_to_line = np.abs(np.cross(end - start, points - start, axis=-1)) / np.linalg.norm(end - start, axis=-1)
# max_idx = np.argmax(dist_point_to_line)
# max_value = dist_point_to_line[max_idx]

# start = time.time()
file = open("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/data/inficon/92003059_20220718_1935_IRW0081.csv")
while file.readline() != '\n':
    pass
csvDf = pd.read_csv(file)
file.close()
df = cp.cleanInficon(csvDf)
gt.add_points_to_df(df, "Long", "Lat")
points = gt.series_to_multipoint(df["points"])
sr = gt.find_utm_zone(points[0].y, points[0].x)
points = gt.reproject(points, sr)
print(app.createBuff(points, 15, sr))

# csvDf = pd.read_csv("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/data/snifferdrone/N1_15_2.8_20220620_153056.csv")
# df = cp.clean_flight_log(csvDf)
# gt.add_points_to_df(df, "SenseLong", "SenseLat")
# points = gt.series_to_multipoint(df["points"])
# sr = gt.find_utm_zone(points[0].y, points[0].x)
# points = gt.reproject(points, sr)

# cp.find_ch4_peaks(df)
# peaks = df[df['Peak'] == True]
# print(len(peaks))
# points = gt.series_to_multipoint(peaks["points"])
# sr = gt.find_utm_zone(points[0].y, points[0].x)
# points = gt.reproject(points, sr)

# print(peaks.index)
# peakList = []
# for i in peaks.index:
#     peakList.append((points[i].x, points[i].y))
# MultiPoint(peakList)
# print(MultiPoint([(i.y, i.x) for i in ]))
# points[850, 2685]



# start = time.time()
# points2 = df[["Long", "Lat"]].to_numpy()
# print(points2)
# end = time.time()
# print(end - start)

# connection = sqlite3.connect("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/web_app_db.sqlite")
# cursor = connection.cursor()
# app.insertIntoDB(cursor, "PeaksTable", "testing", geo_j)
# connection.commit()

# a = ''' {"f":[1,2,3]}'''
# print(a[:4] + a[4:])


# def try1():
#     global a
#     a = 3
#     print(a)

# try1()