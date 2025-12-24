import pandas as pd

def extract_data():
    df = pd.read_csv("data/futuristic_city_traffic.csv")
    print("Data Extracted")
    print(df.head())

if __name__ == "__main__":
    extract_data()