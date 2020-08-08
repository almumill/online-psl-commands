from helpers.lines_to_predicate_file import write_lines_to_predicate_file
from datetime import date, datetime
import pandas as pd
import os

def ishour_predicate(time_to_constant_dict, data_path, fold=0, setting="eval"):
    ishour_predicate_lines = []
    for time in time_to_constant_dict.keys():
        hour = datetime.timetuple(time)[3]
        ishour_predicate_lines += [str(time_to_constant_dict[time]) + "\t" + str(hour) + "\t1"]
    write_lines_to_predicate_file(data_path+str(fold)+"/"+setting+"/ishour_obs.txt", ishour_predicate_lines)