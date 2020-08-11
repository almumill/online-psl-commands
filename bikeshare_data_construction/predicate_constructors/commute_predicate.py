from helpers.lines_to_predicate_file import write_lines_to_predicate_file
import pandas as pd
import os
import datetime

#TODO: weekday only in preprocessing
#TODO: 6am to 6pm preprocessing

def commute_predicate(trip_df, min_threshold, ratio_threshold, data_path, fold=0, setting="eval", ts=0):
    trips_dict = dict({})
    commute_predicate_lines =[]

    for idx, row in trip_df.iterrows():
        trip_tuple = (row["start_station_id"], row["end_station_id"])
        timetuple = pd.to_datetime(row["start_date"]).timetuple()
        hour = timetuple[3]
        weekday = timetuple[6]

        # if the trip isn't between 6am-6pm or is on a weekend,
        # then we don't want to count it
        if hour < 6 or hour > 18 or weekday >= 5:
            continue

        try:
            trips_dict[trip_tuple] += 1
        except:
            trips_dict[trip_tuple] = 1

    for trip_info in trips_dict.items():
        trip_count = trips_dict[(trip_info[0][0], trip_info[0][1])]
        reverse_trip_stations = (trip_info[0][1], trip_info[0][0])

        if reverse_trip_stations in trips_dict.keys():
            return_trip_count = trips_dict[(trip_info[0][1], trip_info[0][0])]
        else:
            return_trip_count = 0.000001

        if trip_count > min_threshold and return_trip_count > min_threshold and \
                min(float(trip_count/return_trip_count), float(return_trip_count/trip_count)) > ratio_threshold:
            commute_predicate_lines += [str(trip_info[0][0]) + "\t" + str(trip_info[0][1]) + "\t1"]

    write_lines_to_predicate_file(os.path.join(data_path, str(fold), setting, str(ts), "commute_obs.txt"), commute_predicate_lines)