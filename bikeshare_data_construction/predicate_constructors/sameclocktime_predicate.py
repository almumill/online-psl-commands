from helpers.lines_to_predicate_file import write_lines_to_predicate_file
from datetime import date, datetime
import pandas as pd
import os

def sameclocktime_predicate(time_to_constant_dict, time_step, block_count, data_path, fold='0', setting='eval'):
	sameclocktime_predicate_lines = []
	times_list = sorted(list(time_to_constant_dict.keys()))
	path = os.path.join("psl_data", str(fold), setting)
	if not os.path.exists(path):
		os.makedirs(path)
	for idx, time in enumerate(times_list):
		k = 0
		for idx_2, time_2 in enumerate(times_list[idx+1:]):
			# if the hour fields are the same, then they have the same clock time
			if pd.to_datetime(time).hour == pd.to_datetime(time_2).hour:
				sameclocktime_predicate_lines += [str(idx) + "\t" + str(idx_2 + idx) + "\t1"]
				k += 1
		k = 0
	write_lines_to_predicate_file(data_path +"sameclocktime_obs.txt", sameclocktime_predicate_lines)