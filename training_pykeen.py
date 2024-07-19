import os
import pandas as pd
import torch
from pykeen.pipeline import pipeline
from pykeen.triples import TriplesFactory
from pykeen.datasets import Dataset

class CustomDataset(Dataset):
    """Custom dataset class that wraps a TriplesFactory for use with PyKEEN pipelines."""
    def __init__(self, triples_factory):
        self.training = triples_factory
        self.testing = triples_factory
        self.validation = triples_factory

def load_custom_dataset(csv_path):
    """Load the custom dataset from a CSV file and create a triples factory."""
    df = pd.read_csv(csv_path)
    triples = df[['head', 'relation', 'tail']].values
    return TriplesFactory.from_labeled_triples(triples)

def run_pykeen_pipeline(dataset, save_path, model_config, training_config, optimizer_config):
    """Define and run the PyKEEN pipeline with a custom dataset."""
    result = pipeline(
        dataset=dataset,
        model=model_config['model_name'],
        model_kwargs=model_config['model_kwargs'],
        training_loop=training_config['training_loop'],
        training_kwargs=training_config['training_kwargs'],
        optimizer_kwargs=optimizer_config,
        random_seed=42,
    )
    
    # Ensure the directory exists
    os.makedirs(save_path, exist_ok=True)

    # Save the trained model
    model_path = f"{save_path}/model.pth"
    torch.save(result.model.state_dict(), model_path)
    print(f"Model saved successfully at {model_path}")

    # Print the results of the evaluation
    print("Evaluation Results:", result.metric_results.to_df())

    return result

if __name__ == '__main__':
    csv_path = '/path/to/triples.csv'
    save_path = '/path/to/trained_model'

    model_config = {
        'model_name': 'TransE',
        'model_kwargs': {'embedding_dim': 50},
    }

    training_config = {
        'training_loop': 'slcwa',
        'training_kwargs': {'num_epochs': 100, 'batch_size': 128},
    }

    optimizer_config = {'lr': 0.01}

    try:
        triples_factory = load_custom_dataset(csv_path)
        custom_dataset = CustomDataset(triples_factory)
    except Exception as e:
        print(f"An error occurred while loading the dataset: {e}")
        exit(1)

    result = run_pykeen_pipeline(custom_dataset, save_path, model_config, training_config, optimizer_config)

