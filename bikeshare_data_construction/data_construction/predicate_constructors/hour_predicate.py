from helpers.lines_to_predicate_file import write_lines_to_predicate_file
from datetime import date
import os
import pandas as pd

def hour_predicate(fold=0, setting="eval"):
    hour_predicate_lines = []
    hours_list = [x for x in range(24)]
    for hour in hours_list:
        hour_predicate_lines += [str(hour)]
    write_lines_to_predicate_file("psl_data/"+str(fold)+"/"+setting+"/hour_obs.txt", hour_predicate_lines)