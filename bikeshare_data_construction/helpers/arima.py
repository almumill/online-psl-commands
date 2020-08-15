from datetime import date, datetime
import time
import numpy as np
import pandas as pd
import statistics
from pmdarima.arima import auto_arima, arima
from helpers.process_demand import status_df_to_demand_df
TIME_GRANULARITY = 3 

def get_arima_params(status_df, obs_start_date, obs_end_date):
    arima_params_dict = dict({})

    status_df.time = pd.to_datetime(status_df.time, format='%Y/%m/%d %H:%M:%S')
    obs_status_df = status_df.loc[(obs_start_date <= status_df["time"]) & (status_df["time"] < obs_end_date)]
    obs_demand_df = status_df_to_demand_df(obs_status_df, TIME_GRANULARITY).to_frame().reset_index()

    stations = obs_demand_df["station_id"].unique()

    total_time_start = time.time()

    for station in stations:
        print("Fitting for station "+str(station))

        # isolate the training time series data
        demand = obs_demand_df.loc[obs_demand_df["station_id"] == station]
        demand = demand["bikes_available"]
        t = time.time()

        model = auto_arima(demand, max_p=168, max_P=168, max_q=168, max_Q=168, max_order=168*4, m=8)
        arima_params_dict[station] = model.get_params()
        print(arima_params_dict[station])

        model_2 = arima.ARIMA(order=arima_params_dict[station]['order'], seasonal_order=arima_params_dict[station]['seasonal_order'])
        print(model_2.get_params())
        print("model fit took " + str(time.time() - t) + " seconds")
    print("total time to fit was " + str(time.time() - total_time_start))

    return arima_params_dict
