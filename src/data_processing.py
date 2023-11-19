import argparse
import pandas as pd
from os import listdir, remove
from os.path import isfile

country_initials = ['SP', 'UK', 'DE', 'DK', 'HU', 'SE', 'IT', 'PO', 'NE']

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
        df = pd.read_csv(data_path + file_path)
        file_path_splited = file_path.split('_')
        # Adding the country initials in generation_data
        if file_path_splited[1] in country_initials:
            df['country'] = file_path_splited[1]
        # Adding the country initials in load_data
        if file_path_splited[-1].replace('.csv', '') in country_initials:
            df['country'] = file_path_splited[-1].replace('.csv', '')
        dfs.append(df)
    return dfs

def clean_data(dfs):
    # TODO: Handle missing values, outliers, etc.
    df_index_to_delete = []
    for df_index, df in enumerate(dfs):
        if df.empty:
            df_index_to_delete.append(df_index)
        # Drop rows with any missing values
        df.dropna()
    # Delete empty DataFrames
    for index in df_index_to_delete:
        dfs.pop(index)
    return dfs

def preprocess_data(dfs):
    #{
    #    SP: 0, # Spain
    #    UK: 1, # United Kingdom
    #    DE: 2, # Germany
    #    DK: 3, # Denmark
    #    HU: 5, # Hungary
    #    SE: 4, # Sweden
    #    IT: 6, # Italy
    #    PO: 7, # Poland
    #    NE: 8 # Netherlands
    #}
    # TODO: Generate new features, transform existing features, resampling, etc.
    data_processed = {
        "SP": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        },
        "UK": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        },
        "DE": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        },
        "DK": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        },
        "HU": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        },
        "SE": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        },
        "IT": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        },
        "PO": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        },
        "NE": {
            "gen": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "load": []
        }
    }
    # Search for the country initial in the columns to get the country data: gen and load    
    for df in dfs:
        country = df['country'][0]
        if 'quantity' in df.columns:
            # Sum all the quantity of the country by time lapse hour
            # Iterate through the DataFrame in chunks of 4 rows
            for index, chunk in df.groupby(df.index // 4):
                # Sum the quantity of the country in the chunk
                quantity_sum = chunk['quantity'].sum()
                # Access the 24 hours list base in the index of the chunk
                data_processed[country]['gen'][index % 24] = quantity_sum
        if 'Load' in df.columns:
            # Sum all the load of the country by time lapse hour
            # Iterate through the DataFrame in chunks of 4 rows
            for index, chunk in df.groupby(df.index // 4):
                # Sum the load of the country in the chunk
                load_sum = chunk['Load'].sum()
                data_processed[country]['load'].append(load_sum)
    return data_processed

def save_data(df, output_file):
    # TODO: Save processed data to a CSV file
    # Create a all_data.cvs file with this format
    # ,green_energy_SP,green_energy_UK,green_energy_DE,green_energy_DK,green_energy_HU,green_energy_SE,green_energy_IT,green_energy_PO,green_energy_NL,SP_Load,UK_Load,DE_Load,DK_Load,HU_Load,SE_Load,IT_Load,PO_Load
    # and save the data in the file
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Data processing script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_folder',
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

def main(input_folder, output_file):
    df = load_data(input_folder)
    df_clean = clean_data(df)
    df_processed = preprocess_data(df_clean)
    save_data(df_processed, output_file)

if __name__ == "__main__":
    df = load_data('./data/')
    df_clean = clean_data(df)
    df_processed = preprocess_data(df_clean)
    print(df_processed)
    exit(0)
    args = parse_arguments()
    main(args.input_folder, args.output_file)