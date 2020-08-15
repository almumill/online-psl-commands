import pandas as pd
from helpers.process_times import train_validation_split, process_times, truncate_to_day, truncate_to_week, matches_day, get_start_and_end_times, get_splits, get_timestep_ranges
from helpers.process_demand import status_df_to_demand_df
from helpers.process_weather import get_zipcode_constant_dict
from helpers.predicate_diff_to_commands import predicate_diff_to_commands
from helpers.arima import get_arima_params
from predicate_constructors.sameclocktime_predicate import sameclocktime_predicate
from predicate_constructors.demand_predicate import demand_predicate
from predicate_constructors.demand_targets import demand_targets
from predicate_constructors.sameweekday_predicate import sameweekday_predicate
from predicate_constructors.demand_truth import demand_truth
from predicate_constructors.isweekend_predicate import isweekend_predicate
from data_exploration.plot_demand import plot_demand
from predicate_constructors.station_predicate import station_predicate
from predicate_constructors.nearby_predicate import nearby_predicate
from predicate_constructors.time_predicate import time_predicate
from predicate_constructors.commute_predicate import commute_predicate
from predicate_constructors.weather_predicates import raining_predicate
from predicate_constructors.ishour_predicate import ishour_predicate
from predicate_constructors.hour_predicate import hour_predicate
from predicate_constructors.recenttime_predicate import recenttime_predicate
from predicate_constructors.arima_predicates import arima_predicate
from helpers.lines_to_predicate_file import write_lines_to_predicate_file
import command_constructors.constants as constants
import os.path
import math
from datetime import date, datetime
import numpy as np
from pmdarima.arima import auto_arima
import statistics
import time
import pickle

DATA_PATH = "psl-datasets/bikeshare/data/"
PSL_DATA_PATH = "psl-datasets/bikeshare/psl-data/"
# unnecessary when running on a LINQS server
STATUS_CSV_LINE_COUNT = 5000000

TIME_GRANULARITY = 3
FOLD_COUNT = 6
TIMESTEP_COUNT = 10
INIT_OBS_PROPORTION = 0.3
TRAIN_VAL_PROPORTION = 0.7

OBS_FILE_LIST = ["commute_obs.txt", "demand_obs.txt", "hour_obs.txt",
                 "ishour_obs.txt", "isweekend_obs.txt", "nearby_obs.txt",
                 "raining_obs.txt", "sameclocktime_obs.txt", "sameweekday_obs.txt",
                 "station_obs.txt", "time_obs.txt"]

OBS_PREDICATE_NAMES = ["Commute", "Demand", "Hour", "IsHour",
                       "IsWeekend", "Nearby", "Raining", "SameClockTime",
                       "SameWeekday", "Station", "Time"]

TARGET_FILE_LIST = ["demand_targets.txt"]
TARGET_PREDICATE_NAMES = ["Demand"]


def is_in_list(series, list):
    bool_array = []
    for idx, row in series.iteritems():
        if row in list:
            bool_array += [True]
        else:
            bool_array += [False]
    return bool_array

def construct_bikeshare_predicates(obs_start_date, obs_end_date, target_start_date, target_end_date, df_list, arima_params_dict, fold=0, setting="eval", ts=0):

    print("building data for fold " + str(fold) + ", timestep " + str(ts))

    target_dir = os.path.join(PSL_DATA_PATH, str(fold), setting, str(ts))
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # unpack dataframes, convert timestamps
    station_df = df_list[0]
    status_df = df_list[1]
    trip_df = df_list[2]
    weather_df = df_list[3]

    #
    weather_df.date = pd.to_datetime(weather_df.date,format='%m/%d/%Y')
    status_df.time = pd.to_datetime(status_df.time, format='%Y/%m/%d %H:%M:%S')
    trip_df.start_date = pd.to_datetime(trip_df.start_date, format='%m/%d/%Y %H:%M')
    trip_df.end_date = pd.to_datetime(trip_df.end_date, format='%m/%d/%Y %H:%M')

    # cut out all statuses that aren't in the observed/unobserved partitions
    status_df = status_df.loc[(obs_start_date <= status_df["time"]) & (status_df["time"] < target_end_date)]
   # weather_df = weather_df.loc[(obs_start_date <= weather_df["date"]) & (weather_df["date"] < target_end_date)]
    #day_demand_df = status_df_to_demand_df(status_df, 1, truncate_to_day)

    # create df containing average demand for every hour
    demand_df = status_df_to_demand_df(status_df, 1).to_frame().reset_index()

    # map times to PSL constants
    time_to_constant_dict, constant_to_time_dict = process_times(status_df)

    # make arima predictions
    arima_predicate(status_df, arima_params_dict, obs_start_date, obs_end_date, target_start_date, target_end_date, time_to_constant_dict, PSL_DATA_PATH, fold=0, setting=setting)

    # create maps between timestamps and PSL constants
    time_to_constant_dict, constant_to_time_dict = process_times(demand_df)

    # commute
   # commute_predicate(trip_df, 40, 0.4, PSL_DATA_PATH, fold, setting, ts=ts)

    # associate times with hours of the day ([0, 23])
    ishour_predicate(time_to_constant_dict, PSL_DATA_PATH, fold=fold, setting=setting, ts=ts)
    #hour_predicate(PSL_DATA_PATH, fold=fold, setting=setting, ts=ts)
    # preprocess trip_df to only contain info about this fold&time-step
   # trip_df = trip_df.loc[(obs_start_date <= trip_df["start_date"]) & (trip_df["start_date"] < obs_end_date)]

    # weather predicate
    #raining_predicate(weather_df, station_df, time_to_constant_dict, PSL_DATA_PATH, fold, setting, ts)

    # separate the train/test status data
    obs_status_df = status_df.loc[(obs_start_date <= status_df["time"]) & (status_df["time"] < obs_end_date)]
    target_status_df = status_df.loc[(target_start_date <= status_df["time"]) & (status_df["time"] < target_end_date)]

    #time_predicate(target_status_df, time_to_constant_dict, PSL_DATA_PATH, fold, setting, ts=ts)
    #nearby_predicate(station_df.loc[is_in_list(station_df["id"], status_df["station_id"].unique())], 5, PSL_DATA_PATH, fold, setting, ts=ts)

    # write time-based and scoping predicates
    #station_predicate(status_df, PSL_DATA_PATH, fold, setting, ts=ts)
    #sameclocktime_predicate(time_to_constant_dict, 0, 140, PSL_DATA_PATH, fold, setting, ts=ts)
    #sameweekday_predicate(time_to_constant_dict, 0, PSL_DATA_PATH, fold, setting, ts=ts)
    #isweekend_predicate(time_to_constant_dict, 0, PSL_DATA_PATH, fold, setting, ts=ts)

  #  # create the train/test demand dataframes
    #obs_demand_df = status_df_to_demand_df(obs_status_df, 1).to_frame().reset_index()
    #target_demand_df = status_df_to_demand_df(target_status_df, 1).to_frame().reset_index()

    # write the train/test demand predicates
    #demand_predicate(obs_demand_df, time_to_constant_dict, 0, PSL_DATA_PATH, fold, setting, ts=ts)
    #demand_targets(target_demand_df, time_to_constant_dict, 0, PSL_DATA_PATH, fold, setting, ts=ts)
    #demand_truth(target_demand_df, time_to_constant_dict, 0, PSL_DATA_PATH, fold, setting, ts=ts)


def load_dataframes():
    # skip the row containing column names
    station_df = pd.read_csv(DATA_PATH + "station.csv", sep=',', header=None, encoding="ISO-8859-1",
                            engine='python', skiprows=[0])
    station_df.columns = ["id", "name", "lat", "long", "dock_count", "city", "installation_date"]

    # comment out nrows when running on a LINQS server
    status_df = pd.read_csv(DATA_PATH + "status.csv", sep=',', header=None, encoding="ISO-8859-1",
                            engine='python', skiprows=[0])
    status_df.columns = ["station_id", "bikes_available", "docks_available", "time"]

    trip_df = pd.read_csv(DATA_PATH + "trip.csv", sep=',', header=None, encoding="ISO-8859-1",
                            engine='python', skiprows=[0])
    trip_df.columns = ["id", "duration", "start_date", "start_station_name", "start_station_id", "end_date",
                       "end_station_name", "end_station_id", "bike_id", "subscription_type", "zip_code"]

    weather_df = pd.read_csv(DATA_PATH + "weather.csv", sep=',', header=None, encoding="ISO-8859-1",
                            engine='python', skiprows=[0])
    weather_df.columns = ["date", "max_temperature_f", "mean_temperature_f", "min_temperature_f", "max_dew_point_f",
                          "mean_dew_point_f", "min_dew_point_f", "max_humidity", "mean_humidity", "min_humidity",
                          "max_sea_level_pressure_inches", "mean_sea_level_pressure_inches", "min_sea_level_pressure_inches",
                          "max_visibility_miles", "mean_visibility_miles", "min_visibility_miles", "max_wind_Speed_mph",
                          "mean_wind_speed_mph", "max_gust_speed_mph", "precipitation_inches", "cloud_cover", "events",
                          "wind_dir_degrees", "zip_code"]

    return station_df, status_df, trip_df, weather_df


def create_commands(fold, setting="eval"):
    base_path = os.path.join(PSL_DATA_PATH, str(fold), setting)

    timestep_dirs = [os.path.join(base_path, str(timestep)) for timestep in range(TIMESTEP_COUNT)]
    command_file_names = [os.path.join(base_path, "ts_"+str(timestep)+".txt") for timestep in range(TIMESTEP_COUNT)]

    for timestep in range(TIMESTEP_COUNT):
        command_file_handle = open(command_file_names[timestep], "w+")
        commands = ""

        for idx, file in enumerate(TARGET_FILE_LIST):
            predicate_name = TARGET_PREDICATE_NAMES[idx]
            if timestep == 0:
                pred_file_1 = os.path.join(timestep_dirs[timestep], file)
                pred_file_2 = None
            else:
                pred_file_1 = os.path.join(timestep_dirs[timestep - 1], file)
                pred_file_2 = os.path.join(timestep_dirs[timestep], file)
            commands += "\n".join(predicate_diff_to_commands(pred_file_1, pred_file_2, predicate_name, constants.TARGET))

        for idx, file in enumerate(OBS_FILE_LIST):
            predicate_name = OBS_PREDICATE_NAMES[idx]
            if timestep == 0:
                pred_file_1 = os.path.join(timestep_dirs[timestep], file)
                pred_file_2 = None
            else:
                pred_file_1 = os.path.join(timestep_dirs[timestep - 1], file)
                pred_file_2 = os.path.join(timestep_dirs[timestep], file)
            commands += "\n".join(predicate_diff_to_commands(pred_file_1, pred_file_2, predicate_name, constants.OBS))

        command_file_handle.write(commands)
        command_file_handle.close()

def main():

    station_df, status_df, trip_df, weather_df = load_dataframes()
    df_list = [station_df, status_df, trip_df, weather_df]

    start_date, end_date = get_start_and_end_times(status_df)
    splits = get_splits(start_date, end_date, FOLD_COUNT)

    timesteps = [0] * FOLD_COUNT

    for idx, split in enumerate(splits):
        timesteps[idx] = get_timestep_ranges(split[0], split[1], INIT_OBS_PROPORTION, TIMESTEP_COUNT)

    for fold in range(FOLD_COUNT):

        val_obs_start_date, val_obs_end_date, val_target_start_date, val_target_end_date = train_validation_split(
            splits[fold][0], splits[fold][1], TRAIN_VAL_PROPORTION)
        arima_params_dict = get_arima_params(status_df, val_obs_start_date, val_obs_end_date)
        outfile = open("arima_params_dict_"+str(fold)+".p", "wb")
        pickle.dump(arima_params_dict, outfile)
        for ts_idx in range(TIMESTEP_COUNT):
                timestep = timesteps[fold][ts_idx]
                obs_start_date = timestep[0]
                obs_end_date = timestep[1]
                target_start_date = timestep[2]
                target_end_date = timestep[3]


                construct_bikeshare_predicates(obs_start_date, obs_end_date, target_start_date, target_end_date,
                                               df_list, arima_params_dict, fold=fold, setting="eval", ts=ts_idx)

    for fold in range(FOLD_COUNT):
        create_commands(fold)



if __name__ == '__main__':
    main()
