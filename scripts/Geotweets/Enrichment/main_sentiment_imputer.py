# usage: python src/main_sentiment_imputer.py --data_path /n/holyscratch01/cga/nicogj/main/ --output_path /n/holyscratch01/cga/nicogj/output/ --dict_methods --emb_methods bert

import pandas as pd
import os
import argparse
import multiprocessing
import time
import glob
import torch
from tqdm.auto import tqdm


def imputer(file, args, imputation_method):
    print("\n{}:".format(imputation_method.upper()))

    if imputation_method not in ['liwc', 'emoji', 'hedono', 'bert']:
        print("Not a proper imputation method. Skipping.")

    elif imputation_method in args.dict_methods:
        df = parallel_imputation(file, args, imputation_method=imputation_method)

    elif imputation_method in args.emb_methods:
        df = embedding_imputation(file, args)

    df.to_csv(
        os.path.join(args.output_path, '{}_sentiment_{}'.format(imputation_method, file)), sep='\t', index=False
    )
    print("Done! Imputed {} scores.".format(df.shape[0]))
    del df


def return_missing_files(path_origin, path_destination):
    files_origin = os.listdir(path_origin)
    files_destination = os.listdir(path_destination)
    files_destination = [s.strip("bert_sentiment_") for s in files_destination]

    files = list(set(files_origin) - set(files_destination))
    return files


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', default='', help='What filename do you want to impute sentiment for?')
    parser.add_argument('--platform', default='', help='Which social media data are we using (twitter, weibo)?')
    parser.add_argument('--data_path', default='', type=str, help='path to data')
    parser.add_argument('--output_path', default='data/sentiment_scores/', type=str, help='path to output')
    parser.add_argument('--dict_methods', nargs='*', default='liwc emoji hedono',
                        help='Which dict techniques do you want to use?')
    parser.add_argument('--emb_methods', nargs='*', default='bert',
                        help='Which embedding techniques do you want to use?')
    parser.add_argument('--random_seed', default=123, type=int, help='random seed')
    parser.add_argument('--score_digits', default=6, type=int, help='how many digits to the output score')

    # Emb based parameters
    parser.add_argument('--batch_size', default=100, type=int, help='batch size')

    # Dict based parameters
    parser.add_argument('--max_rows', default=2500000, type=int, help='Run by chunks of how many rows')
    parser.add_argument('--nb_cores', default=min(16, multiprocessing.cpu_count()), type=int, help='')

    args = parser.parse_args()

    embedding_path = "/n/holylfs/LABS/cga/data/geo-tweets/geotweet-sentiment-geography/training_model/emb.pkl"
    classifier_path = "/n/holylfs/LABS/cga/data/geo-tweets/geotweet-sentiment-geography/training_model/clf.pkl"

    if len(args.dict_methods) > 0:
        from utils.dict_sentiment_imputer import parallel_imputation
    if len(args.emb_methods):
        from utils.emb_sentiment_imputer import embedding_imputation
    if 'bert' in args.emb_methods:
        if torch.cuda.is_available():
            args.emb_model = torch.load(embedding_path)
            args.clf_model = torch.load(classifier_path)
        else:
            print("WARNING: Running on CPU")
            args.emb_model = torch.load(embedding_path, map_location=torch.device('cpu'))
            args.emb_model._target_device = torch.device(type='cpu')
            args.clf_model = torch.load(classifier_path, map_location=torch.device('cpu'))
            args.clf_model._target_device = torch.device(type='cpu')

    if args.filename == '':
        # args.files = sorted([os.path.basename(elem) for elem in glob.glob(os.path.join(args.data_path, "*"))])
        temp_list = return_missing_files(args.data_path, args.output_path)
        args.files = temp_list
        print(f"There are {len(temp_list)} files to process")
    else:
        args.files = [args.filename]

    for file in tqdm(args.files):

        start = time.time()
        print("\n\nRunning for {}".format(file))

        for method in args.dict_methods + args.emb_methods:
            imputer(file, args, method)

        print("Runtime: {} minutes\n\n".format(round((time.time() - start) / 60, 1)))
