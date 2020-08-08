from helpers.lines_to_predicate_file import write_lines_to_predicate_file
from datetime import date, datetime
import os
import pandas as pd

def recenttime_predicate(time_to_constant_dict, target_start_date, num_days=7, fold=0, setting="eval"):
    recenttime_predicate_lines = []
    target_start_date_tuple = datetime.timetuple(target_start_date)
    target_start_date_object = date(target_start_date_tuple[0], target_start_date[1], target_start_date[2])

    for time in time_to_constant_dict.keys():
        time_constant = time_to_constant_dict[time]

        time_tuple = datetime.timetuple(time)
        time_date_object = date(time_tuple[0], time_tuple[1], time_tuple[2])

        if (time_date_object - target_start_date_object).days <= num_days:
            recenttime_predicate_lines += [str(time_constant) + "\t1"]

    write_lines_to_predicate_file("psl_data/"+str(fold)+"/"+setting+"/recenttime_obs.txt")