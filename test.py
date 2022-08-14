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
from datetime import datetime

# start = time.time()
file = open("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/data/inficon/92003059_20220718_1935_IRW0081.csv")
while file.readline() != '\n':
    pass
csvDf = pd.read_csv(file)
file.close()
# print(csvDf)
df = cp.cleanInficon("-buffer", csvDf)
# print(df)
# print(df['Microsec'].duplicated().any())

# gt.add_points_to_df(df, "Long", "Lat")
# points = gt.series_to_multipoint(df["points"])
# sr = gt.find_utm_zone(points[0].y, points[0].x)
# points = gt.reproject(points, sr)
# print(app.createBuff(points, 15, sr))




# start = time.time()
# csvDf = pd.read_csv("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/data/snifferdrone/N1_15_2.8_20220620_153056.csv")
# cleanedDf = cp.clean_flight_log("-buffer", csvDf)
# print(cleanedDf.dtypes)
# end = time.time()
# print(end-start)

# # add a column of points
# gt.add_points_to_df(cleanedDf, "SenseLong", "SenseLat")
# points = gt.series_to_multipoint(cleanedDf["points"])
# # pre project the points to prepare for buffer.
# sr = gt.find_utm_zone(points[0].y, points[0].x)
# points = gt.reproject(points, sr)
# print(app.createSnifferPeaks(points, 15, sr, cleanedDf))

# cp.find_ch4_peaks(cleanedDf)
# print(cleanedDf)
# print(cleanedDf)
# print(cleanedDf.dtypes)

# # add a column of points
# gt.add_points_to_df(cleanedDf, "SenseLong", "SenseLat")
# peaks = cleanedDf[cleanedDf['Peak'] == 1]
# print(peaks)
# points = gt.series_to_multipoint(peaks["points"])
# # pre project the points to prepare for buffer.
# sr = gt.find_utm_zone(points[0].y, points[0].x)
# points = gt.reproject(points, sr)
# # print(points)





# conn = sqlite3.connect("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/web_app_db.sqlite")
# cursor = conn.cursor()
# cleanedDf[["Microsec", "Flight_Date", "SenseLong", "SenseLat", "CH4", "Peak", "Source_Name"]].to_sql(name="PointsTable", con=conn, if_exists='append', index=False)
# df.to_sql(name="PointsTable", con=conn, if_exists='append', index=False)





# count = 0
# index = 0
# for i in df["Peak"]:
#     if i == 1:
#         count += 1
#         print(index)
#     index += 1
# print(count)





# a = {"a":[1,1,3,10,10], "b":[2,2,4,8,8], "c":[5,5,7,0,9]}
# df = pd.DataFrame(a)
# print(df)
# # df = df.drop_duplicates(subset = ['a', 'b', 'c'], keep = 'first')
# c_maxes = df.groupby(['a', 'b']).c.transform(max)
# print(c_maxes)
# df2 = df.loc[df.c == c_maxes]
# # df2 = df2.drop_duplicates(subset = ['a', 'b', 'c'], keep = 'first')
# print(df2.reset_index())

# dt_obj = datetime.strptime('2022-06-20 15:30:58',
#                            '%Y-%m-%d %H:%M:%S')
# millisec = dt_obj.timestamp() * 1000

# print(millisec)