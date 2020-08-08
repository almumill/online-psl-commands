import pandas as pd

def get_zipcode_constant_dict(weather_df):
    zipcode_to_constant_dict = dict({})
    for idx, zip in enumerate(weather_df["zip_code"].unique()):
        zipcode_to_constant_dict[zip] = idx
    return zipcode_to_constant_dict