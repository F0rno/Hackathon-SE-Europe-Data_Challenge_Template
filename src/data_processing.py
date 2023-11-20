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
        if 'processed' in csv_file.split('_'):
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
        if 'processed' in file_path.split('_'):
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
    # TODO: Make the surplus column and add it to the DataFrame
    data_processed = {
        "SP": {"gen": [], "load": [], "surplus": []},
        "UK": {"gen": [], "load": [], "surplus": []},
        "DE": {"gen": [], "load": [], "surplus": []},
        "DK": {"gen": [], "load": [], "surplus": []},
        "HU": {"gen": [], "load": [], "surplus": []},
        "SE": {"gen": [], "load": [], "surplus": []},
        "IT": {"gen": [], "load": [], "surplus": []},
        "PO": {"gen": [], "load": [], "surplus": []},
        "NE": {"gen": [], "load": [], "surplus": []}
    }
    # Init 24 hours list with 0
    for country in data_processed:
        data_processed[country]['gen'] = [0] * 24
        data_processed[country]['load'] = [0] * 24
        data_processed[country]['surplus'] = [0] * 24
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
                data_processed[country]['load'][index % 24] = load_sum
    # Calculate the surplus
    for country in data_processed:
        for index in range(24):
            data_processed[country]['surplus'][index] = data_processed[country]['gen'][index] - data_processed[country]['load'][index]
    return data_processed

def save_data(data_processed, output_file):
    # TODO: Save processed data to a CSV file
    # Create the DataFrame
    df = pd.DataFrame(columns=['green_energy_SP', 'green_energy_UK', 'green_energy_DE', 'green_energy_DK', 'green_energy_HU', 'green_energy_SE', 'green_energy_IT', 'green_energy_PO', 'green_energy_NE', 'SP_Load', 'UK_Load', 'DE_Load', 'DK_Load', 'HU_Load', 'SE_Load', 'IT_Load', 'PO_Load', 'NE_Load'])
    # Iterate through the data_processed dictionary
    for country in data_processed:
        if country == 'SP':
            df['green_energy_SP'] = data_processed[country]['gen']
            df['SP_Load'] = data_processed[country]['load']
        elif country == 'UK':
            df['green_energy_UK'] = data_processed[country]['gen']
            df['UK_Load'] = data_processed[country]['load']
        elif country == 'DE':
            df['green_energy_DE'] = data_processed[country]['gen']
            df['DE_Load'] = data_processed[country]['load']
        elif country == 'DK':
            df['green_energy_DK'] = data_processed[country]['gen']
            df['DK_Load'] = data_processed[country]['load']
        elif country == 'HU':
            df['green_energy_HU'] = data_processed[country]['gen']
            df['HU_Load'] = data_processed[country]['load']
        elif country == 'SE':
            df['green_energy_SE'] = data_processed[country]['gen']
            df['SE_Load'] = data_processed[country]['load']
        elif country == 'IT':
            df['green_energy_IT'] = data_processed[country]['gen']
            df['IT_Load'] = data_processed[country]['load']
        elif country == 'PO':
            df['green_energy_PO'] = data_processed[country]['gen']
            df['PO_Load'] = data_processed[country]['load']
        elif country == 'NE':
            df['green_energy_NE'] = data_processed[country]['gen']
            df['NE_Load'] = data_processed[country]['load']
    # Save the DataFrame in a csv file
    df.to_csv(output_file, index=False)                

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
    #delete_fossil_data()
    #df = load_data('./data/')
    #df_clean = clean_data(df)
    #df_processed = preprocess_data(df_clean)
    #save_data(df_processed, 'data/processed_data.csv')
    #exit(0)
    args = parse_arguments()
    main(args.input_folder, args.output_file)