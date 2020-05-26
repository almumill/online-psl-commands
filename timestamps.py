from datetime import datetime

def get_month_and_date(timestamp):
    """
    Return (year, month) of given timestamp
    """
    ttuple = datetime.fromtimestamp(timestamp).timetuple()
    return (ttuple[0], ttuple[1])

def get_months_list(ratings_df):
    """
    Get a list of (year, month) tuples that exist
    in the ratings dataset, sorted by year and then
    month
    """

    timestamps = ratings_df['timestamp'].values
    months_histogram = dict({})

    for ts in timestamps:
        timestamp_tuple = get_month_and_date(ts)
        try:
            months_histogram[timestamp_tuple] += 1
        except:
            months_histogram[timestamp_tuple] = 1

    # sort by year and then month
    tuple_list = sorted(list(months_histogram.keys()), key = lambda x:x[0])

    # get sorted list of unique years
    set_of_years = set()
    for ttuple in tuple_list:
        set_of_years.add(ttuple[0])

    list_of_years = sorted(set_of_years)
    tuples_per_year = dict({})

    # within each year, sort by month and construct a final list
    final_list = []
    for year in list_of_years:
        final_list += sorted([ttuple for ttuple in tuple_list if ttuple[0] == year], key = lambda x:x[1])

    return final_list

def timestamp_matches_month(ratings_df, time_tuple):
    """
    return a boolean array where True indicates
    that the corresponding row of ratings_df has a
    timestamp from the same year/month as time_tuple
    """
    bool_array = []
    for row in ratings_df.iterrows():
         if get_month_and_date(row[1]['timestamp']) == time_tuple:
             bool_array += [True]
         else:
             bool_array += [False]
    return bool_array
