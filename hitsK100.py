import os
import pandas as pd
from sklearn.model_selection import train_test_split
from pykeen.triples import TriplesFactory
from pykeen.pipeline import pipeline
import torch

def load_and_sample_dataset(csv_path, n_samples, test_size=0.1):
    df = pd.read_csv(csv_path)
    subsets = []
    for i in range(n_samples):
        train_df, _ = train_test_split(df, test_size=test_size, random_state=i)
        subsets.append(train_df)
    return subsets

def train_multiple_models(subsets, base_model_config, training_config, optimizer_config, save_path):
    models = []
    metrics = []  # List to store metrics for each model
    for i, subset in enumerate(subsets):
        model_config = base_model_config.copy()
        model_config['model_kwargs']['embedding_dim'] = 50 + (i * 10)

        train, test = train_test_split(subset, test_size=0.2, random_state=42)
        test, valid = train_test_split(test, test_size=0.5, random_state=42)

        train_factory = TriplesFactory.from_labeled_triples(train[['head', 'relation', 'tail']].values)
        test_factory = TriplesFactory.from_labeled_triples(test[['head', 'relation', 'tail']].values)
        valid_factory = TriplesFactory.from_labeled_triples(valid[['head', 'relation', 'tail']].values)

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
        try:
            hits_at_100 = result.metric_results.get_metric('hits@100')
        except KeyError:
            hits_at_100 = None
            print("hits@100 metric is not available.")

        metric_result = {
            'model_id': i,
            'embedding_dim': model_config['model_kwargs']['embedding_dim'],
            'mean_rank': result.metric_results.get_metric('mean_rank'),
            'hits_at_100': hits_at_100
        }
        metrics.append(metric_result)
        models.append(result.model)

    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_csv(os.path.join(save_path, 'model_performance_metrics.csv'), index=False)
    return models

if __name__ == '__main__':
    base_path = '/path/to/directory'
    csv_path = os.path.join(base_path, 'unique_triples.csv')
    save_path = os.path.join(base_path, 'trained_models')
    os.makedirs(save_path, exist_ok=True)

    n_samples = 10
    base_model_config = {
        'model_name': 'TransE',
        'model_kwargs': {'embedding_dim': 200}
    }
    training_config = {
        'training_loop': 'slcwa',
        'training_kwargs': {'num_epochs': 100, 'batch_size': 128}
    }
    optimizer_config = {'lr': 0.001}

    subsets = load_and_sample_dataset(csv_path, n_samples)
    models = train_multiple_models(subsets, base_model_config, training_config, optimizer_config, save_path)

