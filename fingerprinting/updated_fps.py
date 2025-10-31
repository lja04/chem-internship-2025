import os
import random
import math
import numpy as np
import pandas as pd
from rdkit import Chem
from rdkit.Chem import AllChem
import deepchem as dc
import matplotlib.pyplot as plt
from rdkit import rdBase
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.cluster import AgglomerativeClustering
from multiprocessing import Pool, cpu_count

rdBase.DisableLog('rdApp.warning')

def calculate_ECFPs(smiles_list, radius=3, size=1024):
    """
    Calculate ECFPs for a list of SMILES strings.
    
    Args:
        smiles_list (list): List of SMILES strings.
        radius (int): Radius of the ECFP fingerprint.
        size (int): Size of the ECFP fingerprint vector.

    Returns:
        tuple: A tuple containing:
            - features (np.array): Array of ECFP fingerprints.
            - valid_smiles (list): List of valid SMILES strings.
    """
    featurizer = dc.feat.CircularFingerprint(size=size, radius=radius)
    features = []
    valid_smiles = []

    for smile in smiles_list:
        try:
            mol = Chem.MolFromSmiles(smile)
            if mol is not None:
                feature = featurizer.featurize([smile])
                features.append(feature[0])
                valid_smiles.append(smile)
        except:
            print(f"Error processing SMILES: {smile}")
            continue

    features = np.array(features)
    return features, valid_smiles

def euclidean_distance(fp1, fp2):
    """
    Calculate the Euclidean distance between two fingerprint vectors.

    Args:
        fp1 (np.array): First fingerprint vector.
        fp2 (np.array): Second fingerprint vector.

    Returns:
        float: Euclidean distance between the two fingerprints.
    """
    return np.linalg.norm(fp1 - fp2)

def farthest_point_sampling(num_samples, features, existing_indices=None):
    """
    Performs farthest point sampling on the given fingerprint features,
    excluding existing samples if provided.

    Args:
        num_samples (int): The number of points to sample.
        features (np.array): A list of fingerprint vectors.
        existing_indices (list): List of indices already selected.

    Returns:
        list: A list of sampled indices.
    """
    num_points = len(features)
    if num_samples > num_points:
        raise ValueError("Number of samples cannot be greater than the number of points")

    sampled_indices = []
    
    # If existing indices are provided, start with them
    if existing_indices is not None:
        sampled_indices = existing_indices.copy()
    
    # If we already have enough samples, return them
    if len(sampled_indices) >= num_samples:
        return sampled_indices[:num_samples]
    
    # If no existing samples, pick the first one randomly
    if not sampled_indices:
        # Find all available indices (excluding any that might be in existing_indices)
        available_indices = [i for i in range(num_points) if existing_indices is None or i not in existing_indices]
        if not available_indices:
            raise ValueError("No available molecules to sample")
        first_sample_index = random.choice(available_indices)
        sampled_indices.append(first_sample_index)

    # Iteratively select the farthest point from the existing samples
    for _ in range(num_samples - len(sampled_indices)):
        max_dist = 0
        farthest_index = None

        for i in range(num_points):
            if i in sampled_indices:
                continue         

            min_dist = min(euclidean_distance(features[i], features[idx]) for idx in sampled_indices)    
         
            if min_dist > max_dist:
                max_dist = min_dist
                farthest_index = i

        if farthest_index is None:
            # If no more diverse points found, break early
            print("Warning: Unable to find more diverse points. Returning available samples.")
            break
        
        sampled_indices.append(farthest_index)

    return sampled_indices

def load_existing_samples(existing_file_path):
    """
    Load previously selected molecules from file.
    
    Args:
        existing_file_path (str): Path to file containing existing samples.
        
    Returns:
        list: List of SMILES strings that have already been selected.
    """
    existing_smiles = []
    if os.path.exists(existing_file_path):
        try:
            # Read the file line by line, skipping the header
            with open(existing_file_path, 'r') as f:
                lines = f.readlines()
                
            # Skip the first line (header) and process each line
            for line in lines[1:]:
                line = line.strip()
                if line:
                    # Split by space and take everything after the first token as SMILES
                    parts = line.split()
                    if len(parts) >= 2:
                        # Join all parts after the first one to handle SMILES with spaces
                        smile = ' '.join(parts[1:])
                        existing_smiles.append(smile)
            
            print(f"Successfully loaded {len(existing_smiles)} existing samples")
            
        except Exception as e:
            print(f"Warning: Could not read existing samples file {existing_file_path}: {e}")
    else:
        print(f"No existing samples file found at {existing_file_path}")
    
    return existing_smiles

def plot_sampled_points(features, sampled_indices, tsne_features, cluster_labels, existing_indices=None):
    """
    Plot all points and highlight the sampled points, including the starting point.
    """
    plt.figure(figsize=(12, 8))

    # Plot all points with clustering
    unique_labels = set(cluster_labels)
    colors = [plt.cm.Spectral(each) for each in np.linspace(0, 1, len(unique_labels))]
    for k, col in zip(unique_labels, colors):
        if k == -1:
            # Black for outliers
            col = [0, 0, 0, 1]
        class_member_mask = (cluster_labels == k)
        xy = tsne_features[class_member_mask]
        plt.scatter(xy[:, 0], xy[:, 1], s=50, c=np.array([col]), label=f'Cluster {k}', alpha=0.5)

    # Highlight existing samples if provided (in green)
    if existing_indices:
        existing_points = tsne_features[existing_indices]
        plt.scatter(existing_points[:, 0], existing_points[:, 1], color='green', s=100, 
                   edgecolor='black', linewidth=2, label='Existing Samples', alpha=0.7)

    # Highlight new sampled points (in red)
    new_sampled_indices = [idx for idx in sampled_indices if existing_indices is None or idx not in existing_indices]
    new_sampled_points = tsne_features[new_sampled_indices]
    plt.scatter(new_sampled_points[:, 0], new_sampled_points[:, 1], color='red', s=100, 
               label='New Sampled Points')

    # Label the points
    for i, idx in enumerate(sampled_indices):
        point_type = "E" if existing_indices and idx in existing_indices else "N"
        plt.text(tsne_features[idx, 0], tsne_features[idx, 1], f"{point_type}{i+1}", 
                fontsize=10, color='blue', weight='bold')

    plt.xlabel('t-SNE Dimension 1')
    plt.ylabel('t-SNE Dimension 2')
    plt.legend()
    plt.title('Farthest Point Sampling with Existing Samples')
    plt.savefig('fps_clustering_with_existing.png')
    

def main(smiles_list, refcode_list, output_file, existing_samples_file=None):
    features, valid_smiles = calculate_ECFPs(smiles_list)
    
    # Load existing samples if provided
    existing_smiles = []
    existing_indices = []
    if existing_samples_file:
        existing_smiles = load_existing_samples(existing_samples_file)
        # Find indices of existing samples in current dataset
        existing_indices = [i for i, smile in enumerate(valid_smiles) if smile in existing_smiles]
        print(f"Found {len(existing_indices)} existing samples in current dataset")
    
    # Only perform t-SNE if we have enough samples
    if len(features) > 30:  # t-SNE requires more samples than perplexity (default 30)
        # Perform t-SNE dimensionality reduction
        tsne = TSNE(n_components=2, random_state=1)
        tsne_features = tsne.fit_transform(features)
    else:
        print(f"Warning: Only {len(features)} samples available. Skipping t-SNE.")
        # Use PCA or just use original features for plotting
        pca = PCA(n_components=2)
        tsne_features = pca.fit_transform(features)

    # Perform Agglomerative clustering on the original features
    if len(features) > 1:  # Clustering requires at least 2 samples
        agglomerative = AgglomerativeClustering(n_clusters=min(5, len(features)-1))
        cluster_labels = agglomerative.fit_predict(features)
    else:
        cluster_labels = np.zeros(len(features))  # Single cluster

    # Perform farthest point sampling, starting with existing samples
    total_samples_needed = 4
    sampled_indices = farthest_point_sampling(total_samples_needed, features, existing_indices)
    
    # Get only the new samples (excluding existing ones)
    new_sampled_indices = [idx for idx in sampled_indices if idx not in existing_indices]
    sampled_smiles = [valid_smiles[idx] for idx in sampled_indices]
    sampled_refcodes = [refcode_list.iloc[idx] if hasattr(refcode_list, 'iloc') else refcode_list[idx] for idx in sampled_indices]
    
    # Save all sampled SMILES to file (including existing ones for reference)
    with open(output_file, 'w') as f:
        f.write('REFCODE SMILES\n')
        for smile, refcode in zip(sampled_smiles, sampled_refcodes):
            f.write(f"{refcode} {smile}\n")
    
    print(f"Selected {len(new_sampled_indices)} new samples")
    print(f"Total samples (including existing): {len(sampled_indices)}")

    # Only plot if we have enough samples
    if len(features) > 1:
        plot_sampled_points(features, sampled_indices, tsne_features, cluster_labels, existing_indices)
    else:
        print("Not enough samples for plotting")

def process_directory(input_dir):
    os.chdir(input_dir)
    df = pd.read_csv('energy_rank_4_6.csv', delimiter=',')
    smiles_list = df['SMILES']
    refcode_list = df['REFCODE']
    output_file = 'sampled_smiles.txt'
    
    # Specify the file containing your previously selected molecules
    existing_samples_file = 'sampled_smiles.txt'  # or whatever your previous file was called
    
    main(smiles_list, refcode_list, output_file, existing_samples_file)
    os.chdir('..')

if __name__ == "__main__":
    input_dirs = [
        '/research/GMDayGroup/daygroup/la3g22/internship-fps/data',
    ]

    num_cores = 6
    with Pool(num_cores) as pool:
        pool.map(process_directory, input_dirs)