import pandas as pd
from scipy.signal import find_peaks


def time_to_microsecond(t):
    return t.hour * 1e+6 + t.minute * 6e+7 + t.second * 1e+6 + t.microsecond


def clean_flight_log(flight_log):
    # source_file_name = in_file.split('\\')[-1]
    # flight_log = pd.read_csv(in_file)

    # l = len(flight_log.index)
    flight_log = flight_log[flight_log["SenseLong"] != 0.0]
    flight_log = flight_log[flight_log["SenseLat"] != 0.0]

    # l2 = len(flight_log.index)
    # diff = l - l2
    # if diff > 0:
    #     print(f"{diff} Lat/Lons at 0 removed")

    flight_log = flight_log[pd.to_numeric(flight_log['SenseLong'], errors='coerce').notnull()]
    flight_log = flight_log[pd.to_numeric(flight_log['SenseLat'], errors='coerce').notnull()]

    # diff = l2 - len(flight_log.index)
    # if diff > 0:
    #     print(f"{diff} null Lat/Lons removed")

    # flight_log["Source_Name"] = source_file_name

    flight_log["CH4"] = round(flight_log["CH4"]).astype(int)

    time_ns = flight_log["Timestamp(ms)"] * 1000000
    time_convert = pd.to_datetime(time_ns, yearfirst=True, unit="ns")
    time_col = time_convert.dt.strftime("%Y-%m-%d, %H:%M:%S")
    re_convert = pd.to_datetime(time_col)
    flight_log["Flight_Date"] = re_convert

    time_col = time_convert.dt.strftime("%Y-%m-%d, %H:%M:%S.%f")
    time_col = pd.to_datetime(time_col).dt.time
    flight_log["time"] = time_col
    flight_log["Microsec"] = flight_log.apply(lambda r: time_to_microsecond(r["time"]), axis=1)

    flight_log = flight_log[["Microsec", "Flight_Date", "SenseLong", "SenseLat", "CH4"]]

    return flight_log


def cleanInficon(flight_log):
    # Lat,Long
    # l = len(flight_log.index)
    flight_log = flight_log[flight_log["Long"] != 0.0]
    flight_log = flight_log[flight_log["Lat"] != 0.0]
    flight_log = flight_log[pd.to_numeric(flight_log['Long'], errors='coerce').notnull()]
    flight_log = flight_log[pd.to_numeric(flight_log['Lat'], errors='coerce').notnull()]
    # diff = l - len(flight_log.index)
    # if diff > 0:
    #     print(f"{diff} null Lat/Lons removed")
    flight_log["CH4"] = round(flight_log["CH4"]).astype(int)
    flight_log = flight_log[["Date", "Time", "Lat", "Long", "CH4"]]
    return flight_log


def find_ch4_peaks(df, height=200, distance=7):
    peaks = find_peaks(df["CH4"], height=height, distance=distance)
    df.loc[peaks[0], ["Peak"]] = True