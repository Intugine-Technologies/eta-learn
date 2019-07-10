import numpy as np
import scipy.interpolate
from math import radians, cos, sin, asin, sqrt
from datetime import datetime

time_fmt = "%Y-%m-%dT%H:%M:%S.%fz"


def parse_time(time):
    if isinstance(time, str):
        try:
            time = datetime.strptime(time, time_fmt)
        except Exception as e:
            pass
    return time

def haversine(coords_1, coords_2):
    lon1 = radians(coords_1[1])
    lat1 = radians(coords_1[0])
    lon2 = radians(coords_2[1])
    lat2 = radians(coords_2[0])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = (sin(dlat / 2)) ** 2 + cos(lat1) * cos(lat2) * (sin(dlon / 2)) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r


def sourcefacilityout(var_1, var_2):
    if 'src' not in var_2:
        return None
    for i in range(1, len(var_1) + 1):
        var_2['src'][0] = float(var_2['src'][0])
        var_2['src'][1] = float(var_2['src'][1])
        coords_1 = var_2['src']
        if 'loc' not in var_1[-i]:
            continue
        coords_2 = var_1[-i]['loc']
        if 10 > haversine(coords_1, coords_2) > 1:
            return var_1[-i]['createdAt']


def fetch_eta(var1, var2, var3):
    var_3 = var3
    y_interp = { }
    for m in range(len(var_3)):
        tripid = str(var_3[m])
        if tripid == '5cdd3e14d19f9d5ddac5c68a':
            continue
        var_1 = var1[tripid]
        var_2 = var2[tripid]
        if 'reached_set_time' not in var_2:
            continue
        if 'srcname' not in var_2:
            continue
        if 'destname' not in var_2:
            continue
        if len(var_1) == 0:
            continue
        sourceout = sourcefacilityout(var_1, var_2)
        if sourceout is None:
            continue
        if 'eta_days' in var_2:
            var_2['advance_time'] = 24 * (float(var_2['eta_days']) -
                                          ((var_2['reached_set_time'] - sourceout).total_seconds() / 86400))
            if var_2['advance_time'] > float(var_2['eta_days']) * 12:
                continue
        if 'eta_hours' in var_2:
            var_2['advance_time'] = var_2['eta_hours'] - (
                    (var_2['reached_set_time'] - sourceout).total_seconds() / 3600)
            if var_2['advance_time'] > var_2['eta_hours'] / 2:
                continue
        if 'eta_time' in var_2:
            eta_time = parse_time(var_2['eta_time'])
            if isinstance(eta_time, str):
                continue
            var_2['advance_time'] = (eta_time - var_2['reached_set_time']).total_seconds() / 3600
            del eta_time
        if 'advance_time' not in var_2:
            continue
        var2[tripid]['advance_time'] = var_2['advance_time']
        if var_2['advance_time'] < 0:
            continue
        d_given = []
        t_given = []
        for i in range(len(var_1)):
            if 'distance_remained' not in var_1[i]:
                continue
            if 'createdAt' not in var_1[i]:
                continue
            if ((var_2['reached_set_time'] - var_1[i]['createdAt']).total_seconds() < 0):
                continue

            d_given.append(var_1[i]['distance_remained'] / 1000)
            timeremained = (var_2['reached_set_time'] - var_1[i]['createdAt']).total_seconds()
            t_given.append(timeremained / 3600)
        d_given.append(0)
        t_given.append(0)
        if len(d_given) < 2:
            continue
        y_interp[tripid] = scipy.interpolate.interp1d(d_given, t_given)
    return y_interp, var2


def eta_learn(var):
    var1 = { }
    var2 = { }
    var3 = []
    for i in range(len(var)):
        tripid = str(var[i]['_id'])
        # print("TripId", tripid)
        var1[tripid] = var[i]['locations']
        var2[tripid] = var[i]
        var3.append(tripid)
    y_interp, var2 = fetch_eta(var1, var2, var3)
    out = []
    time_remained = { }
    i = -1
    while True:
        object1 = { }
        i += 1
        time_remained[i] = { }
        for tripid in y_interp:
            try:
                time_remained[i][tripid] = y_interp[tripid](5 * i)
            except Exception as e:
                continue
        if time_remained[i] == { }:
            break
        array = { }
        for tripid in time_remained[i]:
            array[tripid] = var2[tripid]['advance_time']
        arr = sorted(array, reverse=True, key=array.__getitem__)
        l = len(arr)
        if l == 0:
            object1['predicted_time'] = None
        if l == 1:
            object1['predicted_time'] = float(time_remained[i][arr[0]])
        if l == 2:
            object1['predicted_time'] = time_remained[i][arr[0]] * 0.75 + time_remained[i][arr[1]] * 0.25
        if l > 2:
            num = 0
            den = 0
            for j in range(0, int(0.4 * l)):
                num += time_remained[i][arr[j]] * 6
                den += 6
            for j in range(int(0.4 * l), int(0.7 * l)):
                num += ((time_remained[i][arr[j]]) * 3)
                den += 3
            for j in range(int(0.7 * l), int(1 * l)):
                num += time_remained[i][arr[j]] * 1
                den += 1
            object1['predicted_time'] = num / den
        object1['num_trips'] = l
        val = list(time_remained[i].values())
        object1['variance'] = np.var(val)
        object1['distance_remain'] = 5 * i
        out.append(object1)
    # print(out)
    return out
