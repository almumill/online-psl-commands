from helpers.lines_to_predicate_file import write_lines_to_predicate_file
import pandas as pd
from helpers.haversine_distance import haversine_distance
from helpers.process_times import matches_day
import pgeocode



def station_to_zipcode_map(station_df, weather_df):
    zipcode_list = weather_df["zip_code"].unique()

    zipmodel = pgeocode.Nominatim('us')

    station_to_zip = dict({})
    zip_to_station = dict({})

    for idx, row in station_df.iterrows():
        station_id = row["id"]

        station_lat = row["lat"]
        station_lon = row["long"]

        zip_distances = []

        for zipcode in zipcode_list:
            zipcode_info = zipmodel.query_postal_code(str(zipcode))
            zip_lat = zipcode_info["latitude"]
            zip_lon = zipcode_info["longitude"]

            zip_distances += [(zipcode, haversine_distance(station_lat, station_lon, zip_lat, zip_lon))]

            nearest_zip = sorted(zip_distances, key = lambda x:x[1])[0][0]
            station_to_zip[station_id] = nearest_zip
            if nearest_zip not in zip_to_station.keys():
                zip_to_station[zipcode] = [station_id]
            else:
                zip_to_station[zipcode] += [station_id]

    for item in station_to_zip.items():
        print(item)

    return station_to_zip, zip_to_station

def get_times_on_day(day, time_to_constant_dict):
    matching_time_list = []
    for time in time_to_constant_dict.keys():
        if matches_day(time, day):
            matching_time_list += [time_to_constant_dict[time]]
    return matching_time_list

def raining_predicate(weather_df, station_df, time_to_constant_dict, fold=0, setting="eval"):
    raining_predicate_lines = []
    station_to_zip, zip_to_station = station_to_zipcode_map(station_df, weather_df)
    weather_events_df = weather_df[weather_df.events.notnull()]

    encountered_station_time_tuples = set()

    for idx, row in weather_events_df.iterrows():
        for station_id in zip_to_station[row["zip_code"]]:
            times_matching_day = get_times_on_day(row["date"], time_to_constant_dict)
            for time in times_matching_day:
                if (station_id, time) not in encountered_station_time_tuples:
                    encountered_station_time_tuples.add((station_id, time))
                    raining_predicate_lines += [str(station_id) + "\t" + str(time) + "\t1"]
    write_lines_to_predicate_file(data_path + str(fold) + "/" + str(setting) + "/raining_obs.txt", raining_predicate_lines)