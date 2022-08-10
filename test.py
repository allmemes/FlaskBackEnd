import sqlite3
import app
import pandas as pd
import numpy as np
import csvprocessing as cp
import geometry_tools as gt
import json
from shapely.geometry import mapping
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

start = time.time()
testDf = pd.read_csv("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/data/snifferdrone/N1_15_2.8_20220620_153056.csv")
df = cp.clean_flight_log("test_small", testDf)
gt.add_points_to_df(df)


# points = gt.series_to_multipoint(list(df["points"]))
# sr = gt.find_utm_zone(points[0].y, points[0].x)
# points = gt.reproject(points, sr)
# buff = points.buffer(15.0)
# buff = gt.reproject(buff, 4326, sr)
# geo_j = json.dumps(mapping(buff)).replace('"', "'")


end = time.time()
print(end - start)

# connection = sqlite3.connect("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/web_app_db.sqlite")
# cursor = connection.cursor()
# app.insertIntoDB(cursor, "PeaksTable", "testing", geo_j)
# connection.commit()

# a = ''' {"f":[1,2,3]}'''
# print(a[:4] + a[4:])