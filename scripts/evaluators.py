from sklearn.metrics import mean_squared_error
from sklearn.metrics import accuracy_score
from sklearn.metrics import f1_score
from sklearn.metrics import roc_auc_score


def evaluate_mse(predicted_df, truth_df, observed_df, target_df):
    # consider overlap between observed and truths if there is observed truths
    complete_predictions = observed_df.append(predicted_df)
    complete_predictions = complete_predictions.loc[~complete_predictions.index.duplicated(keep='first')]

    # evaluator indices
    evaluator_indices = truth_df.index.intersection(target_df.index)

    # Join predicted_df and truth_df on the arguments
    # TODO: (Charles) filling with the truth assumes the prediction had the truth. This does not scale to experiment 2.
    # experiment_frame = truth_df.loc[evaluator_indices].join(complete_predictions, how="left",
    #                                                         lsuffix='_truth', rsuffix='_predicted')
    # na_entries = experiment_frame[experiment_frame.val_predicted.isna()].index
    # experiment_frame.loc[na_entries, 'val_predicted'] = experiment_frame.loc[na_entries, 'val_truth']

    return mean_squared_error(truth_df.loc[evaluator_indices, 'val'],
                              predicted_df.loc[evaluator_indices, 'val'])


def evaluate_accuracy(predicted_df, truth_df, observed_df, target_df):
    # consider overlap between observed and truths if there is observed truths
    complete_predictions = observed_df.append(predicted_df)
    complete_predictions = complete_predictions.loc[~complete_predictions.index.duplicated(keep='first')]

    # use the category with the highest value as prediction, subset by target index
    predicted_categories_df = complete_predictions.reindex(target_df.index, fill_value=0).groupby(level=0).transform(
        lambda x: x.index.isin(x.iloc[[x.argmax()]].index))

    # boolean for truth df type
    truth_df = (truth_df == 1)

    # Join predicted_df and truth_df on the arguments
    # By right joining and filling with False we are closing the truth since the
    # predicted_categories_df should have all targets and the truth frame may only have the positives
    experiment_frame = truth_df.join(predicted_categories_df, how="right",
                                     lsuffix='_truth', rsuffix='_predicted').fillna(False)

    return accuracy_score(experiment_frame.val_truth, experiment_frame.val_predicted)


def evaluate_f1(predicted_df, truth_df, observed_df, target_df, threshold=0.5):
    # consider overlap between observed and truths if there is observed truths
    complete_predictions = observed_df.append(predicted_df)
    complete_predictions = complete_predictions.loc[~complete_predictions.index.duplicated(keep='first')]
    
    # use the category with the highest value as prediction, subset by target index
    predicted_categories_df = complete_predictions.reindex(target_df.index, fill_value=0).groupby(level=0).transform(
        lambda x: x.index.isin(x.iloc[[x.argmax()]].index))
    
    # boolean for truth df type
    truth_df = (truth_df == 1)
    
    # By right joining and filling with 0 we are closing the truth since the
    # complete_predictions.loc[target_df.index] should have all targets and the
    # truth frame may only have the positives
    experiment_frame = truth_df.join(predicted_categories_df, how="right",
                                     lsuffix='_truth', rsuffix='_predicted').fillna(False)
    
    return f1_score(experiment_frame.val_truth, experiment_frame.val_predicted, pos_label=True)


def evaluate_f1_thresh(predicted_df, truth_df, observed_df, target_df, threshold=0.5):
    # consider overlap between observed and truths if there is observed truths
    complete_predictions = observed_df.append(predicted_df)
    complete_predictions = complete_predictions.loc[~complete_predictions.index.duplicated(keep='first')]
    
    # By right joining and filling with 0 we are closing the truth since the
    # complete_predictions.loc[target_df.index] should have all targets and the
    # truth frame may only have the positives
    experiment_frame = truth_df.join(complete_predictions.reindex(target_df.index, fill_value=0), how="right",
                                     lsuffix='_truth', rsuffix='_predicted').fillna(0)
    rounded_predictions = experiment_frame.val_predicted > threshold
    rounded_truths = experiment_frame.val_truth > threshold
    
    return f1_score(rounded_truths, rounded_predictions, pos_label=True)


def evaluate_roc_auc_score(predicted_df, truth_df, observed_df, target_df, threshold=0.5):
    # consider overlap between observed and truths if there is observed truths
    complete_predictions = observed_df.append(predicted_df)
    complete_predictions = complete_predictions.loc[~complete_predictions.index.duplicated(keep='first')]

    # evaluator indices
    evaluator_indices = truth_df.index.intersection(target_df.index)

    # Join predicted_df and truth_df on the arguments
    # By right joining and filling with 0 we are closing the truth since the
    # complete_predictions.loc[target_df.index] should have all targets and the
    # truth frame may only have the positives
    experiment_frame = truth_df.loc[evaluator_indices].join(complete_predictions.loc[evaluator_indices], how="left",
                                                            lsuffix='_truth', rsuffix='_predicted').fillna(0)

    relevant = experiment_frame.val_truth > threshold
    return roc_auc_score(relevant, experiment_frame.val_predicted)
