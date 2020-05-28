import sys
import numpy as np
sys.path.insert(0, '..')
from helpers import write


def ratings_predicate(ratings_df, partition='obs', fold='0', setting='eval'):
    """
    Ratings Predicates
    """
    write(ratings_df, 'rating_' + partition, fold, setting)