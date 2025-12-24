import pandas as pd

def etl():
    # Load dataset
    df = pd.read_csv('data/futuristic_city_traffic.csv')

    # Example Transformation
    result = df.groupby('city')['traffic_density'].mean().reset_index()
    result.columns = ['city', 'avg_traffic_density']

    # Save output
    result.to_csv('data/traffic_summary.csv', index=False)

if __name__ == "__main__":
    etl()
