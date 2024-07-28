import os
import pandas as pd
from sklearn.model_selection import train_test_split
from pykeen.triples import TriplesFactory
from pykeen.pipeline import pipeline
import torch
import numpy as np

def load_and_sample_dataset(csv_path, n_samples, test_size=0.1):
    df = pd.read_csv(csv_path)
    subsets = []

    for i in range(n_samples):
        # Split the data randomly
        train_df, _ = train_test_split(df, test_size=test_size, random_state=i)
        subsets.append(train_df)
    
    return subsets

def train_multiple_models(subsets, base_model_config, training_config, optimizer_config, save_path):
    models = []
    metrics = []  # List to store metrics for each model

    for i, subset in enumerate(subsets):
        # Modify model configuration based on the iteration index or a random seed
        model_config = base_model_config.copy()
        model_config['model_kwargs']['embedding_dim'] = 50 + (i * 10)  # Incrementally increase embedding dimension

        # Convert the dataframe subset to TriplesFactory
        train, test = train_test_split(subset, test_size=0.2, random_state=42)
        test, valid = train_test_split(test, test_size=0.5, random_state=42)

        train_factory = TriplesFactory.from_labeled_triples(train[['head', 'relation', 'tail']].values)
        test_factory = TriplesFactory.from_labeled_triples(test[['head', 'relation', 'tail']].values)
        valid_factory = TriplesFactory.from_labeled_triples(valid[['head', 'relation', 'tail']].values)

        # Training the model
        result = pipeline(
            training=train_factory,
            testing=test_factory,
            validation=valid_factory,
            model=model_config['model_name'],
            model_kwargs=model_config['model_kwargs'],
            training_loop=training_config['training_loop'],
            training_kwargs=training_config['training_kwargs'],
            optimizer_kwargs=optimizer_config,
            random_seed=42,
        )
        model_path = os.path.join(save_path, f"{model_config['model_name']}_model_{i}.pth")
        torch.save(result.model.state_dict(), model_path)
        print(f"Model {model_config['model_name']} saved successfully at {model_path}")

        # Collect and store metrics
        metric_result = {
            'model_id': i,
            'embedding_dim': model_config['model_kwargs']['embedding_dim'],  # Store variable model size
            'mean_rank': result.metric_results.get_metric('mean_rank'),
            'hits_at_1': result.metric_results.get_metric('hits@1'),
            'hits_at_3': result.metric_results.get_metric('hits@3'),
            'hits_at_10': result.metric_results.get_metric('hits@10'),
        }
	    metrics.append(metric_result)

       	models.append(result.model)

    # Save metrics to CSV file
    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_csv(os.path.join(save_path, 'model_performance_metrics.csv'), index=False)

    return models

if __name__ == '__main__':
    base_path = '/path/to/directory'

