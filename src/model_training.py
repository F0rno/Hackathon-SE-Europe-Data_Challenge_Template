import pandas as pd
import argparse
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from pickle import dump

def load_data(file_path):
    # TODO: Load processed data from CSV file
    return pd.read_csv(file_path)

def split_data(df):
    # TODO: Split data into training and validation sets (the test set is already provided in data/test_data.csv)    
    X = df[[
        'green_energy_SP', 
        'green_energy_UK', 
        'green_energy_DE', 
        'green_energy_DK', 
        'green_energy_HU', 
        'green_energy_SE', 
        'green_energy_IT', 
        'green_energy_PO', 
        'green_energy_NE', 
        'SP_Load', 
        'UK_Load', 
        'DE_Load', 
        'DK_Load', 
        'HU_Load', 
        'SE_Load', 
        'IT_Load', 
        'PO_Load', 
        'NE_Load'
    ]]
    y = df[[
        'SP_Surplus', 
        'UK_Surplus', 
        'DE_Surplus', 
        'DK_Surplus', 
        'HU_Surplus', 
        'SE_Surplus', 
        'IT_Surplus', 
        'PO_Surplus', 
        'NE_Surplus'
    ]]
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_val, y_train, y_val

def train_model(X_train, y_train):
    # TODO: Initialize your model and train it
    model = LinearRegression()
    # Train the model on the training data
    model.fit(X_train, y_train)
    return model

def save_model(model, model_path):
    # TODO: Save your trained model
    # save the model to disk with pickle
    dump(model, open(model_path, 'wb'))
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Model training script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file', 
        type=str, 
        default='data/processed_data.csv', 
        help='Path to the processed data file to train the model'
    )
    parser.add_argument(
        '--model_file', 
        type=str, 
        default='models/model.pkl', 
        help='Path to save the trained model'
    )
    return parser.parse_args()

def main(input_file, model_file):
    df = load_data(input_file)
    X_train, X_val, y_train, y_val = split_data(df)
    model = train_model(X_train, y_train)
    save_model(model, model_file)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.model_file)