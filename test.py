import sqlite3
import app

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

with open("test.txt", "r") as file:
  a = file.readline()
  print(a)