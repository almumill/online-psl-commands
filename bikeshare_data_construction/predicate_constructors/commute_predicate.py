from helpers.lines_to_predicate_file import write_lines_to_predicate_file
import pandas as pd

#TODO: weekday only in preprocessing
#TODO: 6am to 6pm preprocessing

def commute_predicate(trip_df, min_threshold, ratio_threshold, fold=0, setting="eval"):
    trips_dict = dict({})
    commute_predicate_lines =[]

    for idx, row in trip_df.iterrows():
        trip_tuple = (row["start_station_id"], row["end_station_id"])
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

    write_lines_to_predicate_file("psl_data/" + str(fold) + "/" + str(setting) + "/commute_obs.txt", commute_predicate_lines)