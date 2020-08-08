import numpy as np
import pandas as pd
from datetime import date, datetime
from helpers.process_times import truncate_to_day, truncate_to_week

def chop_off_minutes(dt, resolution):
    tuple = datetime.timetuple(dt)
    hour = tuple[3]
    if hour % resolution != 0:
        hour -= hour % resolution
    return np.datetime64(datetime(tuple[0], tuple[1], tuple[2], hour))

def status_df_to_demand_df(status_df, resolution, transform_fn = chop_off_minutes):
    status_df.time = pd.to_datetime(status_df.time, format='%Y/%m/%d %H:%M:%S')
    if transform_fn == chop_off_minutes:
        argtuple = (resolution,)
    else:
        argtuple = ()
    status_df["time"] = status_df["time"].apply(transform_fn, args=argtuple)
    status_df = status_df.set_index(["station_id", "time"])
    status_df["bikes_available"] = status_df["bikes_available"] / (
				status_df["bikes_available"] + status_df["docks_available"])
    demands = 1 - status_df["bikes_available"].groupby(["station_id", "time"]).mean()
    return demands