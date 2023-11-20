import pandas as pd
import argparse
from pickle import load

def load_data(file_path):
    # TODO: Load test data from CSV file
    return pd.read_csv(file_path)

def load_model(model_path):
    # TODO: Load the trained model
    loaded_model = load(open(model_path, 'rb'))
    return loaded_model

def make_predictions(df, model):
    # TODO: Use the model to make predictions on the test data
    predictions = model.predict(df)
    return predictions

def save_predictions(predictions, predictions_file):
    # TODO: Save predictions to a JSON file
    # Create a dictionary with the predictions
    predictions_dict = {
        'SP_Surplus': predictions[:,0],
        'UK_Surplus': predictions[:,1],
        'DE_Surplus': predictions[:,2],
        'DK_Surplus': predictions[:,3],
        'HU_Surplus': predictions[:,4],
        'SE_Surplus': predictions[:,5],
        'IT_Surplus': predictions[:,6],
        'PO_Surplus': predictions[:,7],
        'NE_Surplus': predictions[:,8]
    }
    # Create a dataframe with the predictions
    df = pd.DataFrame(predictions_dict)
    # Save the dataframe to a JSON file
    df.to_json(predictions_file, orient='records')
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Prediction script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file', 
        type=str, 
        default='data/test_data.csv', 
        help='Path to the test data file to make predictions'
    )
    parser.add_argument(
        '--model_file', 
        type=str, 
        default='models/model.pkl',
        help='Path to the trained model file'
    )
    parser.add_argument(
        '--output_file', 
        type=str, 
        default='predictions/predictions.json', 
        help='Path to save the predictions'
    )
    return parser.parse_args()

def main(input_file, model_file, output_file):
    df = load_data(input_file)
    model = load_model(model_file)
    predictions = make_predictions(df, model)
    save_predictions(predictions, output_file)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.model_file, args.output_file)
