import sqlite3
import app
import pandas as pd
import numpy as np
import csvprocessing as cp
import geometry_tools as gt
import json
from shapely.geometry import mapping, MultiPoint, Point
import re
import time
from datetime import datetime
import urllib
import urllib.request
import collections
import multiprocessing as mp
from ctypes import c_char_p

# csvDf = pd.read_csv("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/data/snifferdrone/N1_15_2.8_20220620_153056.csv")
# cleanedDf = cp.clean_flight_log("-buffer", csvDf)
# print(cleanedDf)
# # # # end = time.time()
# # # # print(end-start)

# # add a column of points
# gt.add_points_to_df(cleanedDf)
# points = gt.series_to_multipoint(cleanedDf["points"])
# # pre project the points to prepare for buffer.
# sr = gt.find_utm_zone(points[0].y, points[0].x)
# points = gt.reproject(points, sr)


# # cleanedDf["UtmLong"] = [i.x for i in points]
# # cleanedDf["UtmLat"] = [i.y for i in points]
# # print(cleanedDf)

# buff = points.buffer(15, resolution=8)
# geo_j = mapping(buff)
# # geometry = json.loads(app.toEsriGeometry(geo_j))
# geometry = app.toEsriGeometry(geo_j)

# print(geometry)

def delete_all(token, targetUrl):
    delete_dict = {"f": "json",
                    "token": token,
                    "where": "OBJECTID >= 0"
                    }
    urllib.request.urlopen(targetUrl + r"/deleteFeatures", urllib.parse.urlencode(delete_dict).encode('utf-8'))
    # jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
    # print (jsonOutput['addResults'])


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
        #print(f"Authenticated as {self.username}")
        return token["token"]


def add_feature(token):
    features = [
            {
                "attributes" : {
                "Flight_Date" : "2022-07-18 19:35:32",
                "SenseLat" : 42.4,
                "SenseLong" : -83.55,
                "CH4" : 0,
                "Source_Name" : "test_Source_Name.csv",
                "BUFF_DIST": 15,
                "ORIG_FID": 1
                # "uniqueId": 1,
                # "objectId": 1,
                # "globalId": 1
                },
                "geometry" :
                {"rings": [[[289153.8256086962, 4698014.516955851], [289153.5646950932, 4698011.867855582], [289152.7919810475, 4698009.320558752], [289151.53716152656, 4698006.9729565475], [289149.84845854086, 4698004.915266006], [289147.7907679992, 4698003.226563021], [289145.44316579495, 4698001.9717435], [289142.89586896444, 4698001.199029454], [289140.24676869623, 4698000.938115851], [289137.59766842803, 4698001.199029454], [289135.0503715975, 4698001.9717435], [289132.7027693933, 4698003.226563021], [289130.6450788516, 4698004.915266006], [289128.9563758659, 4698006.9729565475], [289127.701556345, 4698009.320558752], [289126.92884229927, 4698011.867855582], [289126.66792869626, 4698014.516955851], [289126.92884229927, 4698017.166056119], [289127.701556345, 4698019.713352949], [289128.9563758659, 4698022.060955154], [289130.6450788516, 4698024.118645695], [289132.7027693933, 4698025.807348681], [289135.0503715975, 4698027.062168201], [289137.59766842803, 4698027.834882247], [289140.24676869623, 4698028.09579585], [289142.89586896444, 4698027.834882247], [289145.44316579495, 4698027.062168201], [289147.7907679992, 4698025.807348681], [289149.84845854086, 4698024.118645695], [289151.53716152656, 4698022.060955154], [289152.7919810475, 4698019.713352949], [289153.5646950932, 4698017.166056119], [289153.8256086962, 4698014.516955851]], [[289152.8313806838, 4697939.923694723], [289152.5704670808, 4697937.274594455], [289151.7977530351, 4697934.727297625], [289150.54293351417, 4697932.37969542], [289148.85423052846, 4697930.322004879], [289146.7965399868, 4697928.633301893], [289144.44893778255, 4697927.3784823725], [289141.90164095204, 4697926.605768327], [289139.25254068384, 4697926.344854724], [289136.60344041564, 4697926.605768327], [289134.0561435851, 4697927.3784823725], [289131.7085413809, 4697928.633301893], [289129.6508508392, 4697930.322004879], [289127.9621478535, 4697932.37969542], [289126.7073283326, 4697934.727297625], [289125.9346142869, 4697937.274594455], [289125.67370068387, 4697939.923694723], [289125.9346142869, 4697942.572794992], [289126.7073283326, 4697945.120091822], [289127.9621478535, 4697947.467694026], [289129.6508508392, 4697949.525384568], [289131.7085413809, 4697951.214087553], [289134.0561435851, 4697952.468907074], [289136.60344041564, 4697953.24162112], [289139.25254068384, 4697953.502534723], [289141.90164095204, 4697953.24162112], [289144.44893778255, 4697952.468907074], [289146.7965399868, 4697951.214087553], [289148.85423052846, 4697949.525384568], [289150.54293351417, 4697947.467694026], [289151.7977530351, 4697945.120091822], [289152.5704670808, 4697942.572794992], [289152.8313806838, 4697939.923694723]]]}
            }
        ]
    # features[0]["geometry"] = geometry
    query_dict = {"f": "json",
                    "token": token,
                    "features": features
                    }

    jsonResponse = urllib.request.urlopen("https://services6.arcgis.com/QUg4M9ZDNdUHdQcy/arcgis/rest/services/web_app_test_peaks/FeatureServer/2" + r"/addFeatures", urllib.parse.urlencode(query_dict).encode('utf-8'))  
    jsonOutput = json.loads(jsonResponse.read(),
                            object_pairs_hook=collections.OrderedDict)

    # returnJson["result"].append(jsonOutput['addResults'])
    # returnJson["result"] = returnJson["result"] + [jsonOutput['addResults']]
    if jsonOutput['addResults']:
      print("not empty")
    else:
      print("empty")
    # print(jsonOutput['addResults'])


# returnJson = {"result": []}
# # 1.8462049961090088
# start = time.time()
# token = get_token("hangchen_sniffergis", "Ch970721")
# add_feature(token)
# add_feature(token, returnJson)
# add_feature(token, returnJson)
# end = time.time()
# print(end - start)
# print(returnJson)

# 0.7975919246673584
# manager = mp.Manager()
# returnJson = manager.dict()
# returnJson["result"] = []
# start = time.time()
# token = get_token("hangchen_sniffergis", "Ch970721")
# p1 = mp.Process(target=add_feature, args = [token, returnJson])
# p2 = mp.Process(target=add_feature, args = [token, returnJson])
# p3 = mp.Process(target=add_feature, args = [token, returnJson])
# p1.start()
# p2.start()
# p3.start()
# p1.join()
# p2.join()
# p3.join()
# end = time.time()
# print(end - start)
# print(returnJson)


features = [
  {
    "attributes" : {
      "Flight_Date" : "8/10/2022, 11:25 AM",
      "SenseLat" : 42.4,
      "SenseLong" : -83.55,
      "CH4" : 0,
      "Source_Name" : "test_Source_Name.csv",
      "BUFF_DIST": 15,
      "ORIG_FID": 1
    },
    "geometry" :
    {
      "rings" : [
      [
        [289331.1604, 4697752.6758], 
        [289333.518345537, 4697752.4695065], 
        [289335.804646092, 4697751.85689411], 
        [289337.949833579, 4697750.85657675], 
        [289339.888727563, 4697749.49894857], 
        [289341.562415731, 4697747.8252604], 
        [289342.920043914, 4697745.88636642], 
        [289343.920361267, 4697743.74117893], 
        [289344.532973655, 4697741.45487838], 
        [289344.739267158, 4697739.09693284], 
        [289344.532973655, 4697736.7389873], 
        [289343.920361267, 4697734.45268675], 
        [289342.920043914, 4697732.30749926], 
        [289341.562415731, 4697730.36860528], 
        [289339.888727563, 4697728.69491711], 
        [289337.949833579, 4697727.33728893], 
        [289335.804646092, 4697726.33697157], 
        [289333.518345537, 4697725.72435919], 
        [289331.1604, 4697725.51806568], 
        [289328.802454464, 4697725.72435919], 
        [289326.516153909, 4697726.33697157], 
        [289324.370966421, 4697727.33728893], 
        [289322.432072438, 4697728.69491711], 
        [289320.75838427, 4697730.36860528], 
        [289319.400756087, 4697732.30749926], 
        [289318.400438733, 4697734.45268675], 
        [289317.787826346, 4697736.7389873], 
        [289317.581532842, 4697739.09693284], 
        [289317.787826346, 4697741.45487838], 
        [289318.400438733, 4697743.74117893], 
        [289319.400756087, 4697745.88636642], 
        [289320.75838427, 4697747.8252604], 
        [289322.432072438, 4697749.49894857], 
        [289324.370966421, 4697750.85657675], 
        [289326.516153909, 4697751.85689411], 
        [289328.802454464, 4697752.4695065], 
        [289331.1604, 4697752.6758]
      ],
      [
        [289331.1604, 4697744.9735], 
        [289332.180853198, 4697744.88422191], 
        [289333.170300441, 4697744.61910032], 
        [289334.098677877, 4697744.18619082], 
        [289334.937777226, 4697743.59864713], 
        [289335.66210288, 4697742.87432147], 
        [289336.249646569, 4697742.03522212], 
        [289336.682556077, 4697741.10684469], 
        [289336.947677667, 4697740.11739745], 
        [289337.036955753, 4697739.09694425], 
        [289336.947677667, 4697738.07649105], 
        [289336.682556077, 4697737.08704381], 
        [289336.249646569, 4697736.15866637], 
        [289335.66210288, 4697735.31956702], 
        [289334.937777226, 4697734.59524137], 
        [289334.098677877, 4697734.00769768], 
        [289333.170300441, 4697733.57478817], 
        [289332.180853198, 4697733.30966658], 
        [289331.1604, 4697733.22038849], 
        [289330.139946803, 4697733.30966658], 
        [289329.15049956, 4697733.57478817], 
        [289328.222122124, 4697734.00769768], 
        [289327.383022775, 4697734.59524137], 
        [289326.658697121, 4697735.31956702], 
        [289326.071153432, 4697736.15866637], 
        [289325.638243924, 4697737.08704381], 
        [289325.373122334, 4697738.07649105], 
        [289325.283844247, 4697739.09694425], 
        [289325.373122334, 4697740.11739745], 
        [289325.638243924, 4697741.10684469], 
        [289326.071153432, 4697742.03522212], 
        [289326.658697121, 4697742.87432147], 
        [289327.383022775, 4697743.59864713], 
        [289328.222122124, 4697744.18619082], 
        [289329.15049956, 4697744.61910032], 
        [289330.139946803, 4697744.88422191], 
        [289331.1604, 4697744.9735]
      ]
    ]
  }
},
{
    "attributes" : {
      "Flight_Date" : "8/10/2022, 11:25 AM",
      "SenseLat" : 42.4,
      "SenseLong" : -83.55,
      "CH4" : 0,
      "Source_Name" : "test_Source_Name.csv",
      "BUFF_DIST": 15,
      "ORIG_FID": 1
    },
    "geometry" :
    {
      "rings" : [
      [
        [289331.1604, 4697752.6758], 
        [289333.518345537, 4697752.4695065], 
        [289335.804646092, 4697751.85689411], 
        [289337.949833579, 4697750.85657675], 
        [289339.888727563, 4697749.49894857], 
        [289341.562415731, 4697747.8252604], 
        [289342.920043914, 4697745.88636642], 
        [289343.920361267, 4697743.74117893], 
        [289344.532973655, 4697741.45487838], 
        [289344.739267158, 4697739.09693284], 
        [289344.532973655, 4697736.7389873], 
        [289343.920361267, 4697734.45268675], 
        [289342.920043914, 4697732.30749926], 
        [289341.562415731, 4697730.36860528], 
        [289339.888727563, 4697728.69491711], 
        [289337.949833579, 4697727.33728893], 
        [289335.804646092, 4697726.33697157], 
        [289333.518345537, 4697725.72435919], 
        [289331.1604, 4697725.51806568], 
        [289328.802454464, 4697725.72435919], 
        [289326.516153909, 4697726.33697157], 
        [289324.370966421, 4697727.33728893], 
        [289322.432072438, 4697728.69491711], 
        [289320.75838427, 4697730.36860528], 
        [289319.400756087, 4697732.30749926], 
        [289318.400438733, 4697734.45268675], 
        [289317.787826346, 4697736.7389873], 
        [289317.581532842, 4697739.09693284], 
        [289317.787826346, 4697741.45487838], 
        [289318.400438733, 4697743.74117893], 
        [289319.400756087, 4697745.88636642], 
        [289320.75838427, 4697747.8252604], 
        [289322.432072438, 4697749.49894857], 
        [289324.370966421, 4697750.85657675], 
        [289326.516153909, 4697751.85689411], 
        [289328.802454464, 4697752.4695065], 
        [289331.1604, 4697752.6758]
      ],
      [
        [289331.1604, 4697744.9735], 
        [289332.180853198, 4697744.88422191], 
        [289333.170300441, 4697744.61910032], 
        [289334.098677877, 4697744.18619082], 
        [289334.937777226, 4697743.59864713], 
        [289335.66210288, 4697742.87432147], 
        [289336.249646569, 4697742.03522212], 
        [289336.682556077, 4697741.10684469], 
        [289336.947677667, 4697740.11739745], 
        [289337.036955753, 4697739.09694425], 
        [289336.947677667, 4697738.07649105], 
        [289336.682556077, 4697737.08704381], 
        [289336.249646569, 4697736.15866637], 
        [289335.66210288, 4697735.31956702], 
        [289334.937777226, 4697734.59524137], 
        [289334.098677877, 4697734.00769768], 
        [289333.170300441, 4697733.57478817], 
        [289332.180853198, 4697733.30966658], 
        [289331.1604, 4697733.22038849], 
        [289330.139946803, 4697733.30966658], 
        [289329.15049956, 4697733.57478817], 
        [289328.222122124, 4697734.00769768], 
        [289327.383022775, 4697734.59524137], 
        [289326.658697121, 4697735.31956702], 
        [289326.071153432, 4697736.15866637], 
        [289325.638243924, 4697737.08704381], 
        [289325.373122334, 4697738.07649105], 
        [289325.283844247, 4697739.09694425], 
        [289325.373122334, 4697740.11739745], 
        [289325.638243924, 4697741.10684469], 
        [289326.071153432, 4697742.03522212], 
        [289326.658697121, 4697742.87432147], 
        [289327.383022775, 4697743.59864713], 
        [289328.222122124, 4697744.18619082], 
        [289329.15049956, 4697744.61910032], 
        [289330.139946803, 4697744.88422191], 
        [289331.1604, 4697744.9735]
      ]
    ]
  }
}
]

# targetUrl = "https://services6.arcgis.com/QUg4M9ZDNdUHdQcy/arcgis/rest/services/web_app_test_peaks/FeatureServer/2"
# urlCheckDict = {targetUrl: "-peaks"}

# returnJson = {}
# returnJson["task"] = "S"
# returnJson["pointsAppended"] = False
# returnJson["bufferSuccess"] = []
# returnJson["bufferFail"] = []
# returnJson["peaksSuccess"] = []
# returnJson["peaksFail"] = []

# app.add_feature(token, features, targetUrl, urlCheckDict, returnJson)
# print(returnJson)


# delete_all(token, "https://services6.arcgis.com/QUg4M9ZDNdUHdQcy/arcgis/rest/services/web_app_test_points/FeatureServer/3")
# delete_all(token, "https://services6.arcgis.com/QUg4M9ZDNdUHdQcy/arcgis/rest/services/web_app_test_buffer/FeatureServer/0")
# delete_all(token, "https://services6.arcgis.com/QUg4M9ZDNdUHdQcy/arcgis/rest/services/web_app_test_peaks/FeatureServer/2")

# i = "N1_15_2.8_20220620_153056.csv-buffer"
# sql = "Source_Name = '" + i.split("-")[0] + "'"
# # sql = "Source_Name = '" + "N1_15_2.8_20220620_153056.csv" + "'"
# print(app.query_feature(token, sql, "https://services6.arcgis.com/QUg4M9ZDNdUHdQcy/arcgis/rest/services/web_app_test_buffer/FeatureServer/0"))





# print(geometry)
# result = [
#             ('addResults',
#                 [
#                     OrderedDict(
#                     [
#                         ('objectId', 17), ('uniqueId', 17), ('globalId', None), ('success', True)
#                     ]),
#                     OrderedDict(
#                     [
#                         ('objectId', 18), ('uniqueId', 18), ('globalId', None), ('success', True)
#                     ])
#                 ]
#             )
#         ]

# start = time.time()
# file = open("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/data/inficon/92003059_20220718_1935_IRW0081.csv")
# while file.readline() != '\n':
#     pass
# csvDf = pd.read_csv(file)
# file.close()
# # print(csvDf)
# df = cp.cleanInficon("-buffer", csvDf)
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
# # print(cleanedDf)
# # # # end = time.time()
# # # # print(end-start)

# add a column of points
# gt.add_points_to_df(cleanedDf)
# points = gt.series_to_multipoint(cleanedDf["points"])
# # pre project the points to prepare for buffer.
# sr = gt.find_utm_zone(points[0].y, points[0].x)
# points = gt.reproject(points, sr)


# cleanedDf["UtmLong"] = [i.x for i in points]
# cleanedDf["UtmLat"] = [i.y for i in points]
# print(cleanedDf)

# buff = points.buffer(15, resolution=8)
# geo_j = mapping(buff)
# with open("test.json", "w") as file:
#     file.write(app.toEsriGeometry(geo_j))
# app.toEsriGeometry(geo_j)

# geo_j = gt.shapely_to_geojson(buff)


# cp.find_ch4_peaks(cleanedDf)
# peaks = cleanedDf[cleanedDf['Peak'] == 1]
# peakList = []
# for i in peaks.index:
#     peakList.append((points[i].x, points[i].y))
# peakPoints = MultiPoint(peakList)
# outerBuffer = peakPoints.buffer(13.57884, resolution=8)
# geo_j = mapping(outerBuffer)
# result = app.toEsriGeometry(geo_j)
# print(json.loads(result)["rings"])

# # innerBuffer = peakPoints.buffer(5.876544, resolution=8)
# # # start = time.time()
# geo_j = gt.shapely_to_geojson(outerBuffer).replace('"', "'")
# # end = time.time()
# # print(end-start)
# print(geo_j)


# # with open("sample_polygon.json", "w") as file:
# #     file.write(geo_j)
# print(len(geo_j['coordinates']))



# for i in geojson:
#     testPeak.append(i[0])
# print(testPeak)



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


# a = {"rings": (1232, 44)}
# print(type(json.loads(json.dumps(a))))

# connection = sqlite3.connect("/Users/apple/Desktop/python/internship/web_vue/web_viewer_back/web_app_db.sqlite")
# cursor = connection.cursor()
# i = "N1_15_2.8_20220620_153056.csv-buffer"
# query = cursor.execute("SELECT Source_name, EsriGeometry FROM BuffersTable WHERE Source_name == '" + i + "'").fetchall()[0]
# print(query[0])


# a = "N1_15_2.8_20220620_153056.csv-peaks"
# sourceName = a.replace("peaks", "path")
# query = cursor.execute("SELECT Flight_date, Senselat, Senselong, CH4, Source_name, Utmlong, Utmlat FROM PointsTable WHERE Source_name == '" + sourceName + "' AND Peak == 1").fetchall()
# orig_id = 1
# for i in query:
#     peakCenter = Point(i[5], i[6])
#     outerCircle = {"attributes" : {
#                     "Flight_Date": i[0],
#                     "SenseLat": i[1],
#                     "SenseLong": i[2],
#                     "CH4": i[3],
#                     "Source_Name" : i[4].split("-")[0],
#                     "BUFF_DIST": 13.57884,
#                     "ORIG_FID": orig_id
#                     }}
#     outerBuffer = mapping(peakCenter.buffer(13.57884, resolution=6))
#     esriOuterBuffer = json.loads(app.toEsriGeometry(outerBuffer))
#     outerCircle["geometry"] = esriOuterBuffer

#     innerCircle = {"attributes" : {
#                 "Flight_Date": i[0],
#                 "SenseLat": i[1],
#                 "SenseLong": i[2],
#                 "CH4": i[3],
#                 "Source_Name" : i[4].split("-")[0],
#                 "BUFF_DIST": 5.876544,
#                 "ORIG_FID": orig_id
#                 }}
#     innerBuffer = mapping(peakCenter.buffer(5.876544, resolution=6))
#     esriInnerBuffer = json.loads(app.toEsriGeometry(innerBuffer))
#     innerCircle["geometry"] = esriInnerBuffer

#     orig_id += 1

#     print(innerCircle, outerCircle)

# esriGeo = json.loads(query.replace("'", '"'))

# uploadStruct = {
#     "attributes" : {},
#     "geometry" : esriGeo
# }

# print(uploadStruct)


# def p1(dicta):
#     # dicta["success"].append("a")
#     dicta["success"] = dicta["success"] + ["a"]

# def p2(dicta):
#     # dicta.append("b")
#     dicta["fail"] = True

# manager = mp.Manager()
# a = manager.dict()
# a["task"] = "s"
# a["success"] = []
# a["fail"] = False
# P1 = mp.Process(target=p1, args=[a])
# P2 = mp.Process(target=p2, args=[a])

# P1.start()
# P2.start()

# P1.join()
# P2.join()

# print(json.dumps(a.copy()))

# a = [0,1,2,3]

# for i in a:
#   if i%2 == 0:
#     print(i)


appendDict = {"a": 4, "b": 2, "c": 6, "d": 4}
appendList = [
  {"name": "a", "status": True},
  {"name": "a", "status": True},
  {"name": "a", "status": False},
  {"name": "a", "status": True},

  {"name": "b", "status": True},
  {"name": "b", "status": True},

  {"name": "c", "status": False},
  {"name": "c", "status": True},
  {"name": "c", "status": True},
  {"name": "c", "status": True},
  {"name": "c", "status": True},
  {"name": "c", "status": True},

  {"name": "d", "status": True},
  {"name": "d", "status": True},
  {"name": "d", "status": True},
  {"name": "d", "status": True}
]

returnJson={"peaksSuccess": [], "peaksFail": []}
appendIndex = 0
skippedIndexNumber = 0
while appendIndex < len(appendList):
    result = appendList[appendIndex]
    # recombine the layer name
    layerName = appendList[appendIndex]['name']
    # get the total number of peaks of current appended layers and compare it with the current appendIndex
    totalPeaksNumber = appendDict[layerName]
    if result["status"]:
        # if the current index - previously skipped index + 1 = total peaks number, then add peaks layer name to the returnJson
        if appendIndex - skippedIndexNumber + 1 == totalPeaksNumber:
            returnJson["peaksSuccess"] += [layerName]
            skippedIndexNumber += totalPeaksNumber
        appendIndex += 1
    else:
        returnJson["peaksFail"] += [layerName]
        appendIndex = skippedIndexNumber + totalPeaksNumber
        skippedIndexNumber += totalPeaksNumber
print(returnJson)