from helpers.lines_to_predicate_file import write_lines_to_predicate_file
from datetime import date
import os
import pandas as pd

def hour_predicate(data_path, fold=0, setting="eval", ts=0):
    hour_predicate_lines = []
    hours_list = [x for x in range(24)]
    for hour in hours_list:
        hour_predicate_lines += [str(hour) + "\t1"]
    write_lines_to_predicate_file(os.path.join(data_path, str(fold), setting, str(ts), "hour_obs.txt"), hour_predicate_lines)