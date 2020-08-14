import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import math

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

def get_start_and_end_times(status_df):
    status_df.time = pd.to_datetime(status_df.time, format='%Y/%m/%d %H:%M:%S')
    status_df = status_df.sort_values(by="time")

    start_time = pd.to_datetime(truncate_to_day(status_df.iloc[0, 3]))
    end_time = pd.to_datetime(truncate_to_day(status_df.iloc[status_df.shape[0] - 1, 3]))

    return start_time, end_time

def get_splits(start_time, end_time, split_count):

    total_days = (end_time - start_time).days
    split_size = math.floor(total_days / split_count)

    splits = []

    for split_idx in range(split_count):
        split = [0, 0]
        split[0] = start_time + timedelta(days=split_idx * split_size)
        split[1] = start_time + timedelta(days=(split_idx + 1) * split_size)
        splits += [split]

    return splits

def train_validation_split(start_time, end_time, proportion):

    total_days = (end_time - start_time).days

    split = [0, 0]
    obs_end_date = start_time + timedelta(days=math.floor(total_days * proportion))
    target_start_date = obs_end_date
    target_end_date = end_time

    return start_time, obs_end_date, target_start_date, target_end_date

def get_timestep_ranges(start_time, end_time, init_proportion, step_count):
    timestep_ranges = [0] * step_count
    total_days = (end_time - start_time).days
    initial_obs_size = math.floor(init_proportion *  total_days)
    timestep_size = math.floor((total_days - initial_obs_size) / step_count)

    obs_start_date = pd.to_datetime(truncate_to_day(start_time))
    target_end_date = pd.to_datetime(truncate_to_day(end_time))

    for ts in range(step_count):
        obs_end_date = start_time + timedelta(days=initial_obs_size + ((ts + 1) * timestep_size))
        target_start_date = obs_end_date
        timestep_ranges[ts] = [obs_start_date, obs_end_date, target_start_date, target_end_date]

    return timestep_ranges