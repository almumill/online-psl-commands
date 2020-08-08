import pandas as pd
from helpers.lines_to_predicate_file import write_lines_to_predicate_file

def time_predicate(status_df, time_to_constant_dict, fold=0, setting="eval"):
    time_predicate_lines = []
    times = status_df["time"].unique()
    for time in times:
        time_predicate_lines += [str(time_to_constant_dict[pd.Timestamp(time)]) + "\t1"]

    write_lines_to_predicate_file("psl_data/" + str(fold) + "/" + str(setting) + "/time_obs.txt", time_predicate_lines)