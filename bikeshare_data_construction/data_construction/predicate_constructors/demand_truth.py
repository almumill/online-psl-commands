from helpers.lines_to_predicate_file import write_lines_to_predicate_file
import os

def demand_truth(truth_demand_df, time_to_constant_dict, time_step, fold='0', setting='eval'):
	demand_truth_lines = []
	path = os.path.join("psl_data", str(fold), setting)
	if not os.path.exists(path):
		os.makedirs(path)
	for idx, row in truth_demand_df.iterrows():
		station_id = row["station_id"]
		time_constant = time_to_constant_dict[row["time"]]
		demand_truth_lines += [str(station_id) + "\t" + str(time_constant) + "\t" + str(row["bikes_available"])]
	write_lines_to_predicate_file(path + "/demand_truth.txt", demand_truth_lines)