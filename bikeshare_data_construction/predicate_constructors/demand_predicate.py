from helpers.lines_to_predicate_file import write_lines_to_predicate_file
import pandas as pd
import numpy as np
import os

def demand_predicate(obs_demand_df, time_to_constant_dict, time_step, data_path, fold='0', setting='eval'):
	demand_predicate_lines = []
	for idx, row in obs_demand_df.iterrows():
		station_id = row["station_id"]
		time_constant = time_to_constant_dict[row["time"]]
		demand_predicate_lines += [str(station_id) + "\t" + str(time_constant) + "\t" + str(row["bikes_available"])]
	write_lines_to_predicate_file(data_path + "demand_obs.txt", demand_predicate_lines)