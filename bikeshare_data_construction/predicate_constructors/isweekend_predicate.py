from helpers.lines_to_predicate_file import write_lines_to_predicate_file
from datetime import date
import os
import pandas as pd

def isweekend_predicate(time_to_constant_dict, time_step, data_path, fold='0', setting='eval', ts=0):
	isweekend_predicate_lines = []
	times_list = sorted(list(time_to_constant_dict.keys()))
	path = os.path.join("psl_data", str(fold), setting)
	if not os.path.exists(path):
		os.makedirs(path)
	for idx, time in enumerate(times_list):
		time = pd.to_datetime(time)
		if (date(time.year, time.month, time.day).weekday() == 5 or date(time.year, time.month, time.day).weekday() == 6):
			isweekend_predicate_lines += [str(idx) + "\t1"]
	write_lines_to_predicate_file(os.path.join(data_path, str(fold), setting, str(ts), "isweekend_obs.txt"), isweekend_predicate_lines)