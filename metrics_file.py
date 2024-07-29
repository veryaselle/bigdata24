import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def load_data(metrics_path):
    """ Load the performance metrics from a CSV file. """
    return pd.read_csv(metrics_path)

def visualize_data(metrics_df):
    """ Generate visualizations for the metrics data. """
    # Ensure model_id is treated as a categorical variable for better plotting
    metrics_df['model_id'] = metrics_df['model_id'].astype(str)

    # Plot Mean Rank across different models
    plt.figure(figsize=(10, 6))
    sns.barplot(x='model_id', y='mean_rank', data=metrics_df, capsize=.1)
    plt.title('Mean Rank Across Different Models')
    plt.xlabel('Model ID')
    plt.ylabel('Mean Rank')
    plt.show()

    # Plot Hits@K metrics
    plt.figure(figsize=(12, 8))
    sns.lineplot(data=metrics_df, x='model_id', y='hits_at_1', label='Hits@1')
    sns.lineplot(data=metrics_df, x='model_id', y='hits_at_3', label='Hits@3')
    sns.lineplot(data=metrics_df, x='model_id', y='hits_at_10', label='Hits@10')
    plt.title('Hits@K Metrics Across Models')
    plt.xlabel('Model ID')
    plt.ylabel('Hits Score')
    plt.legend(title='Metrics')
    plt.show()

def main():
    # Path to the metrics CSV file
    metrics_path = '/path/to/model_performance_metrics.csv'
    
    # Load the metrics data
    metrics_df = load_data(metrics_path)
    
    # Print basic dataframe information and statistics
    print(metrics_df.head())
    print(metrics_df.describe())

    # Visualize the data
    visualize_data(metrics_df)

if __name__ == "__main__":
    main()

