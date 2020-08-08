from helpers.lines_to_predicate_file import write_lines_to_predicate_file
import os

def demand_targets(truth_demand_df, time_to_constant_dict, time_step, data_path, fold='0', setting='eval'):
	demand_targets_lines = []
	path = os.path.join("psl_data", str(fold), setting)
	if not os.path.exists(path):
		os.makedirs(path)
	for idx, row in truth_demand_df.iterrows():
		station_id = row["station_id"]
		time_constant = time_to_constant_dict[row["time"]]
		demand_targets_lines += [str(station_id) + "\t" + str(time_constant)]
	write_lines_to_predicate_file(data_path + "demand_targets.txt", demand_targets_lines)