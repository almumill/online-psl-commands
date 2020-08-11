import os
from helpers.lines_to_predicate_file import write_lines_to_predicate_file

def station_predicate(status_df, data_path, fold, setting="eval", ts=0):
	station_predicate_lines = []
	path = os.path.join("psl_data", str(fold), setting)
	if not os.path.exists(path):
		os.makedirs(path)
	stations = status_df.set_index(["station_id"]).index.unique()
	for station in stations:
		station_predicate_lines += [str(station) + "\t1"]
	write_lines_to_predicate_file(os.path.join(data_path, str(fold), setting, str(ts), "station_obs.txt"), station_predicate_lines)