from helpers.haversine_distance import haversine_distance
from helpers.lines_to_predicate_file import write_lines_to_predicate_file

def nearby_predicate(station_df, threshold, data_path, fold=0, setting="eval"):
    nearby_predicate_lines = []

    for idx, row in station_df.iterrows():
        for idx_2, row_2 in station_df.iterrows():
            lat_1 = row["lat"]
            lat_2 = row_2["lat"]
            lon_1 = row["long"]
            lon_2 = row_2["long"]

            station_1_id = row["id"]
            station_2_id = row_2["id"]
            if haversine_distance(lat_1, lon_1, lat_2, lon_2) < threshold:
                nearby_predicate_lines += [str(station_1_id) + "\t" + str(station_2_id) + "\t1"]

    write_lines_to_predicate_file(data_path + str(fold) + "/" + str(setting) + "/nearby_obs.txt", nearby_predicate_lines)