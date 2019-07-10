from intuginehelper import intudb
import datetime
from bson import ObjectId
from eta_learn import eta_learn
import time
from pymongo import MongoClient
import os


def get_database():
    server, port = str(os.environ['DATABASE_SERVER_ETA']).rsplit(':', 1)
    client = MongoClient(server, port=int(port))
    database = client['eta_prediction']
    return database


# Database Server for ETA
db_to_write = get_database()
lanes_db = db_to_write['lanes']
eta_db = db_to_write['eta']


def write_to_db(data):
    print(data)
    if 'srcname' not in data['_id']:
        return
    if 'destname' not in data['_id']:
        return
    if 'client_client' not in data['_id']:
        data['_id']['client_client'] = None
    lanes_db.update_one(data['_id'], { "$set": data['_id'] }, upsert=True)
    lanes_search = lanes_db.find(data['_id'])
    cnt = 0
    laneId = ''
    for lane in lanes_search:
        laneId = lane['_id']
        cnt += 1
    if cnt > 1:
        print("[ERROR] = Multiple lanes with data = ", data['_id'])

    # print(laneId)
    predicted = data['eta']
    if predicted:
        for p in predicted:
            p['laneId'] = laneId
            eta_db.update_one(p, { "$set": p }, upsert=True)


def get_all_pings(trips_ids):
    """
    get all pings for all the trips
    :param trips_list: list of all the trips Id's
    :return: Object containing list of ['_id':'tripId', 'pings': list( all pings )]
    """
    database = intudb.get_database()
    collection = database['status']
    data = collection.aggregate([{
        '$match': { 'tripId': { '$in': [ObjectId(x) for x in trips_ids] } }
    }, {
        '$group': { '_id': '$tripId', 'pings': { '$push': '$$ROOT' } }
    }], allowDiskUse=True)
    return list(x for x in data)


def get_trips():
    """
    :return: Returns all the Trips in the DB with {'user', 'srcname', 'destname', 'client_client'}
    """
    print(datetime.datetime.now())
    time = datetime.datetime.now() - datetime.timedelta(days=int(os.environ['ETA_DAYS']))
    print(time)
    database = intudb.get_database()
    collection = database['trips']
    data = collection.aggregate([{
        "$match": {
            "createdAt": {
                '$gte': time,
            },
            "running": False,
            "submit": False,
            "src": {
                "$exists": True,
                "$size": 2
            },
            "dest": {
                "$exists": True,
                "$size": 2
            }
        }
    }, {
        "$project": {
            "srcname": "$srcname",
            "destname": "$destname",
            "client": "$client",
            "user": "$user",
            "client_client": "$client_client",
            'eta_days': '$eta_days',
            'eta_hours': '$eta_hours',
            'eta_time': '$eta_time',
            'reached_set_time': '$reached_set_time',
            'src': "$src",
            'dest': "$dest"
        }
    }, {
        '$group': {
            '_id': {
                'user': "$user",
                'srcname': "$srcname",
                'destname': "$destname",
                'client_client': "$client",
            },
            'trips': { '$push': '$$ROOT' } }
    }], allowDiskUse=True)
    return list(x for x in data)


def get_eta(all_trips):
    all_trips['eta'] = eta_learn(all_trips['trips'])
    del all_trips['trips']
    return all_trips


def get_data_db():
    all_trips = get_trips()
    all_trips_ids = []
    for trips in all_trips:
        for trip in trips['trips']:
            all_trips_ids.append(str(trip['_id']))

    print("[all_trips_ids] = ", len(all_trips_ids))
    all_pings = get_all_pings(all_trips_ids)
    print("[all ping] = ", len(all_pings))
    pings_map = { }
    for pings in all_pings:
        pings_map[str(pings['_id'])] = pings['pings']
    del all_pings

    total_time = time.time()
    for trips in all_trips:
        # print(trips)
        print("[trips]", len(trips))

        pings_cnt = 0
        for trip in trips['trips']:
            # trip_ids.append(trip['_id']);
            # print(trip['_id'])
            trip['locations'] = []
            try:
                total_pings = pings_map[str(trip['_id'])]
                trip['locations'] = total_pings
                print("[Total Pings are] = ", len(total_pings))
            except Exception as e:
                del trip
                continue
                # print("No Locations for", trip['_id'])
            pings_cnt += len(trip['locations'])

        # print(trips)
        print("ETA LEARN Start")
        print("[trips * pings] = ", len(trips['trips']), pings_cnt)
        start_time = time.time()
        eta = get_eta(trips)
        print("ETA Took --- %s seconds ---" % (time.time() - start_time))
        # print(eta)
        write_to_db(eta)
    print('[Total trips are] = ', len(all_trips))
    print("TOTAL Time = {} s".format(time.time() - total_time))
