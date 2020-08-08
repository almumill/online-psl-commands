import pandas as pd
from datetime import datetime
import numpy as np

def process_times(status_df):
    time_to_constant_dict = dict({})
    constant_to_time_dict = dict({})
    cur_const = 0
    for time in status_df["time"].unique():
        time = pd.Timestamp(time)
        if time not in time_to_constant_dict.keys():
            time_to_constant_dict[time] = cur_const
            constant_to_time_dict[cur_const] = time
            cur_const += 1
    return time_to_constant_dict, constant_to_time_dict

def chop_off_minutes(dt):
    tuple = datetime.timetuple(dt)
    return np.datetime64(datetime(tuple[0], tuple[1], tuple[2], tuple[3]))

def truncate_to_day(dt):
    tuple = datetime.timetuple(dt)
    return np.datetime64(datetime(tuple[0], tuple[1], tuple[2], 0))

def truncate_to_week(dt):
    tuple = datetime.timetuple(dt)
    day = dt.weekday()
    return np.datetime64(datetime(tuple[0], tuple[1], tuple[2] - day, 0))

def matches_day(dt, candidate_time):
    tuple = datetime.timetuple(dt)
    candidate_tuple = datetime.timetuple(candidate_time)
    if (tuple[0], tuple[1], tuple[2]) == (candidate_tuple[0], candidate_tuple[1], candidate_tuple[2]):
        return True


def matches_week(dt, candidate_day):
    tuple = datetime.timetuple(dt)
    candidate_tuple = datetime.timetuple(candidate_day)
    if (tuple[0], tuple[1], tuple[2]) == (candidate_tuple[0], candidate_tuple[1], candidate_tuple[2]):
        return True