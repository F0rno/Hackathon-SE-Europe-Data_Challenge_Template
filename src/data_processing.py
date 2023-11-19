import argparse
import pandas as pd
from os import listdir, remove
from os.path import isfile

def delete_fossil_data():
    green_energy_codes = ['B09', 'B10', 'B11', 'B12', 'B13', 'B14', 'B15', 'B16', 'B18', 'B19']
    # List cvs files of .data/
    for csv_file in listdir('./data/'):
        if not isfile('./data/' + csv_file):
            continue
        if 'test' in csv_file.split('.'):
            continue
        if 'load' in csv_file.split('_'):
            continue
        if csv_file.split('_')[-1].replace('.csv', '') in green_energy_codes:
            continue 
        remove('data/' + csv_file)    

def load_data(data_path):
    # TODO: Load data from CSV file
    dfs = []
    for file_path in listdir(data_path):
        if not isfile(data_path + file_path):
            continue
        if 'test' in file_path.split('.'):
            continue
        dfs.append(pd.read_csv(data_path + file_path))
    return dfs

def clean_data(dfs):
    # TODO: Handle missing values, outliers, etc.
    for dfs in dfs:
        # Drop rows with any missing values
        dfs.dropna()
    return dfs

def preprocess_data(df):
    # TODO: Generate new features, transform existing features, resampling, etc.
    
    return df_processed

def save_data(df, output_file):
    # TODO: Save processed data to a CSV file
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Data processing script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file',
        type=str,
        default='data/raw_data.csv',
        help='Path to the raw data file to process'
    )
    parser.add_argument(
        '--output_file', 
        type=str, 
        default='data/processed_data.csv', 
        help='Path to save the processed data'
    )
    return parser.parse_args()

def main(input_file, output_file):
    df = load_data(input_file)
    df_clean = clean_data(df)
    df_processed = preprocess_data(df_clean)
    save_data(df_processed, output_file)

if __name__ == "__main__":
    clean_data(load_data('./data/'))
    exit(0)
    args = parse_arguments()
    main(args.input_file, args.output_file)