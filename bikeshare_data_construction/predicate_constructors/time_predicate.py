import os
import pandas as pd
from helpers.lines_to_predicate_file import write_lines_to_predicate_file

def time_predicate(status_df, time_to_constant_dict, data_path, fold=0, setting="eval", ts=0):
    time_predicate_lines = []
    times = status_df["time"].unique()
    for time in times:
        time_predicate_lines += [str(time_to_constant_dict[pd.Timestamp(time)]) + "\t1"]

    write_lines_to_predicate_file(os.path.join(data_path, str(fold), setting, str(ts), "time_obs.txt"), time_predicate_lines)