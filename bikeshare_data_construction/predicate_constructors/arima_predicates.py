from datetime import date, datetime
import time
import numpy as np
import pandas as pd
import statistics
from pmdarima.arima import auto_arima
from helpers.process_demand import status_df_to_demand_df
from helpers.lines_to_predicate_file import write_lines_to_predicate_file
from helpers.process_times import chop_off_minutes
import math

TIME_GRANULARITY = 3

# expand_predictions:
# duplicate arima predictions to account for
# different granularities

def expand_predictions(preds, granularity):
    expanded_preds = []
    for pred in preds:
        for x in range(granularity):
            expanded_preds += [pred]
    return expanded_preds

def dict_lookup(key, dict):
    return dict[key]

def arima_predicate(status_df, obs_start_date, obs_end_date, target_start_date, target_end_date, time_to_constant_dict, data_path, fold=0, setting="eval"):
    arima_predicate_lines = ""

    # separate observed and truth data about bike demand
    status_df.time = pd.to_datetime(status_df.time, format='%Y/%m/%d %H:%M:%S')
    obs_status_df = status_df.loc[(obs_start_date <= status_df["time"]) & (status_df["time"] < obs_end_date)]
    target_status_df = status_df.loc[(target_start_date <= status_df["time"]) & (status_df["time"] < target_end_date)]

    # create training time series data with specified granularity
    obs_demand_df = status_df_to_demand_df(obs_status_df, TIME_GRANULARITY).to_frame().reset_index()

    # create truth data with a granularity of 1hr for comparison
    fine_truth_demand_df = status_df_to_demand_df(target_status_df, 1).to_frame().reset_index()

    stations = obs_demand_df["station_id"].unique()
    errors = []
    errors_squared = []

    total_time_start = time.time()

    for station in stations:
        print("Doing station "+str(station))

        # isolate the training time series data
        demand = obs_demand_df.loc[obs_demand_df["station_id"] == station]
        demand = demand["bikes_available"]

        start_time = time.time()

        # best-performing hyperparameters, took ~46000 seconds to do fit a model for every station
        # max_p=168, max_P=168, max_q=168, max_Q=168, max_order=168*4, m=8
        arima_model = auto_arima(demand)

        # create a df which is a copy of the fine-grained truth data
        # which we later fill in with ARIMA predictions and write
        # to a predicate file
        arima_pred_df = fine_truth_demand_df.loc[fine_truth_demand_df["station_id"] == station].reset_index()

        # isolate the truth values into a series
        demand_truth_series = arima_pred_df["bikes_available"]

        arima_predictions = arima_model.predict(n_periods=math.floor(demand_truth_series.shape[0]/TIME_GRANULARITY) + (demand_truth_series.shape[0] % TIME_GRANULARITY))

        # compute errors for this run and over all
        temp_errors = compare_arima_predictions(arima_predictions, demand_truth_series, TIME_GRANULARITY)
        temp_errors_squared = arima_squared_errors(arima_predictions, demand_truth_series, TIME_GRANULARITY)
        errors += compare_arima_predictions(arima_predictions, demand_truth_series, TIME_GRANULARITY)
        errors_squared += arima_squared_errors(arima_predictions, demand_truth_series, TIME_GRANULARITY)

        # print stats
        print("model took %s seconds" % (time.time() - start_time))
        print("This run MAE: " + str(statistics.mean(temp_errors)))
        print("This run MSE: " + str(statistics.mean(temp_errors_squared)))
        print(statistics.mean(errors))
        print(statistics.mean(errors_squared))

        # if the granularity is > 1hr, we need to duplicate
        # predictions to appropriately compare against the original data
        # which had a granularity of 1hr
        expanded_preds = expand_predictions(arima_predictions, TIME_GRANULARITY)

        # fill in the dataframe with the duplicated predictions
        for idx in range(arima_pred_df.shape[0]):
            arima_pred_df.iloc[idx, 3] = expanded_preds[idx]

        # convert times to constants and wirte
        # the predictions df to a predicate file
        arima_pred_df["time"] = arima_pred_df["time"].apply(dict_lookup, args=(time_to_constant_dict,))
        arima_predicate_lines += arima_pred_df[["station_id", "time", "bikes_available"]].to_csv(sep="\t", header=False, index=False)

    print("total runtime: " + str(time.time() - total_time_start))

    handle = open(data_path+str(fold)+"/"+setting+"/arima_obs.txt", "w")
    handle.write(arima_predicate_lines)

def compare_arima_predictions(arima_predictions, truth_demand_df, coarse_gran=None):
    errors = []
    errors_squared = []
    if coarse_gran == None:
        for idx, pred in enumerate(arima_predictions):
            errors += [abs(pred - float(truth_demand_df.iloc[idx]))]
        return errors
    else:
        for idx, pred in enumerate(arima_predictions):
            x = 0
            while (coarse_gran * idx) + x < truth_demand_df.shape[0] and x < coarse_gran:
                errors += [abs(pred - float(truth_demand_df.iloc[(coarse_gran * idx) + x]))]
                x += 1
    return errors

def arima_squared_errors(arima_predictions, truth_demand_df, coarse_gran=None):
    errors = []
    errors_squared = []
    if coarse_gran == None:
        for idx, pred in enumerate(arima_predictions):
            errors += [abs(pred - float(truth_demand_df.iloc[idx]))]
        return errors
    else:
        for idx, pred in enumerate(arima_predictions):
            x = 0
            while (coarse_gran * idx) + x < truth_demand_df.shape[0] and x < coarse_gran:
                errors += [abs(pred - float(truth_demand_df.iloc[(coarse_gran * idx) + x])) ** 2]
                x += 1
    return errors
