from helpers.lines_to_predicate_file import write_lines_to_predicate_file
from datetime import date
import os
import pandas as pd

def sameweekday_predicate(time_to_constant_dict, time_step, fold='0', setting='eval'):
	sameweekday_predicate_lines = []
	times_list = sorted(list(time_to_constant_dict.keys()))
	path = os.path.join("psl_data", str(fold), setting)
	if not os.path.exists(path):
		os.makedirs(path)
	for idx, time in enumerate(times_list):
		time = pd.to_datetime(time)
		for idx_2, time_2 in enumerate(times_list[idx+1:]):
			time_2 = pd.to_datetime(time_2)
			# if the hour fields are the same, then they have the same clock time
			if date(time.year, time.month, time.day).weekday() == date(time_2.year, time_2.month, time_2.day).weekday():
				sameweekday_predicate_lines += [str(idx) + "\t" + str(idx_2 + idx) + "\t1"]
	write_lines_to_predicate_file(path+"/sameweekday_obs.txt", sameweekday_predicate_lines)