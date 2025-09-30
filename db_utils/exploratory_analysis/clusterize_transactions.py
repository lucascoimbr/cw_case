#!/usr/bin/env python3

import argparse
import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score


def parse_args():
    p = argparse.ArgumentParser(description='Transaction clustering for fraud EDA.')
    p.add_argument('--csv', required=True, help='Path to CSV (e.g., transactional-sample.csv)')
    p.add_argument('--n_clusters', type=int, default=4, help='Number of clusters for KMeans (default: 4)')
    p.add_argument('--outdir', default='outputs', help='Output directory (default: outputs)')
    return p.parse_args()


def ensure_outdir(path):
    os.makedirs(path, exist_ok=True)


def compute_velocity_feature(df: pd.DataFrame) -> pd.Series:
    """Counts, for each transaction, how many transactions of the SAME user occurred
    in the last 1h BEFORE the current transaction.
    """
    from collections import deque
    df_sorted = df.sort_values(['user_id', 'transaction_date']).copy()
    counts = pd.Series(0, index=df_sorted.index, dtype=int)

    for uid, idx in df_sorted.groupby('user_id').groups.items():
        times = df_sorted.loc[idx, 'transaction_date'].tolist()
        dq = deque()
        vals = []
        for t in times:
            dq.append(t)
            # remove events outside the 1h window
            while dq and (t - dq[0]).total_seconds() > 3600:
                dq.popleft()
            vals.append(len(dq) - 1)  # exclude the current transaction
        counts.loc[list(idx)] = vals

    # reorder back
    return counts.reindex(df.index)


def main():
    args = parse_args()
    ensure_outdir(args.outdir)

    # 1) Load
    df = pd.read_csv(args.csv)
    if 'transaction_date' not in df.columns:
        print("ERROR: column 'transaction_date' not found in CSV.", file=sys.stderr)
        sys.exit(2)
    if 'has_cbk' not in df.columns:
        print("ERROR: column 'has_cbk' not found in CSV.", file=sys.stderr)
        sys.exit(2)

    # basic parsing
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    if df['transaction_date'].isna().any():
        print("Warning: there are invalid dates; they will be ignored in temporal features.", file=sys.stderr)
    df['hour'] = df['transaction_date'].dt.hour.fillna(0).astype(int)
    df['has_cbk'] = df['has_cbk'].astype(bool)

    # Extract BIN (if card_number exists)
    if 'card_number' in df.columns:
        df['bin'] = df['card_number'].astype(str).str[:6]
    else:
        df['bin'] = np.nan

    # 2) Auxiliary aggregated metrics
    if 'user_id' in df.columns:
        user_cbk_rate = df.groupby('user_id')['has_cbk'].mean()
        user_txn_count = df.groupby('user_id').size()
        df['user_cbk_rate'] = df['user_id'].map(user_cbk_rate)
        df['user_txn_count'] = df['user_id'].map(user_txn_count)
    else:
        df['user_cbk_rate'] = 0.0
        df['user_txn_count'] = 0

    if 'device_id' in df.columns:
        device_user_count = df.groupby('device_id')['user_id'].nunique()
        device_cbk_rate = df.groupby('device_id')['has_cbk'].mean()
        df['device_user_count'] = df['device_id'].map(device_user_count)
        df['device_cbk_rate'] = df['device_id'].map(device_cbk_rate)
    else:
        df['device_user_count'] = 0
        df['device_cbk_rate'] = 0.0

    bin_cbk_rate = df.groupby('bin')['has_cbk'].mean() if 'bin' in df.columns else pd.Series(dtype=float)
    df['bin_cbk_rate'] = df['bin'].map(bin_cbk_rate) if 'bin' in df.columns else 0.0

    # Velocity (last 1h per user)
    try:
        df['txns_1h_user'] = compute_velocity_feature(df)
    except Exception as e:
        print(f"Warning: failed to compute velocity: {e}", file=sys.stderr)
        df['txns_1h_user'] = 0

    # 3) Numeric features
    feature_cols = [
        col for col in [
            'transaction_amount',
            'hour',
            'user_cbk_rate',
            'user_txn_count',
            'device_user_count',
            'device_cbk_rate',
            'bin_cbk_rate',
            'txns_1h_user',
        ] if col in df.columns
    ]

    X = df[feature_cols].copy()
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0)

    # 4) Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 5) KMeans
    kmeans = KMeans(n_clusters=args.n_clusters, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)
    df['cluster'] = clusters

    # 6) Silhouette
    sil = None
    try:
        if args.n_clusters > 1:
            sil = silhouette_score(X_scaled, clusters)
    except Exception as e:
        print(f"Warning: could not compute Silhouette: {e}", file=sys.stderr)

    # 7) PCA plot with legend
    try:
        pca = PCA(n_components=2, random_state=42)
        X_pca = pca.fit_transform(X_scaled)
        df['pca1'] = X_pca[:, 0]
        df['pca2'] = X_pca[:, 1]

        plt.figure(figsize=(8, 6))
        scatter = plt.scatter(
            df['pca1'], df['pca2'],
            c=df['cluster'], cmap='tab10', alpha=0.7
        )
        plt.title('Transaction clusters (PCA 2D)')
        plt.xlabel('PCA 1')
        plt.ylabel('PCA 2')

        handles, labels = scatter.legend_elements()
        plt.legend(handles, labels, title="Clusters", loc="best")

        plt.savefig(os.path.join(args.outdir, 'pca_scatter.png'), bbox_inches='tight')
        plt.close()
    except Exception as e:
        print(f"Warning: PCA failed: {e}", file=sys.stderr)

    # 8) Histograms per hour
    tx_per_hour = df['hour'].value_counts().sort_index()
    plt.figure(figsize=(10, 6))
    tx_per_hour.plot(kind='bar', color='steelblue')
    plt.title('Total transactions per hour of day')
    plt.xlabel('Hour of day (0-23)')
    plt.ylabel('# of transactions')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(args.outdir, 'tx_per_hour.png'))
    plt.close()

    cbk_rate_per_hour = df.groupby('hour')['has_cbk'].mean()
    plt.figure(figsize=(10, 6))
    cbk_rate_per_hour.plot(kind='bar', color='firebrick')
    plt.title('Chargeback rate per hour of day')
    plt.xlabel('Hour of day (0-23)')
    plt.ylabel('Proportion of chargebacks')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(args.outdir, 'cbk_rate_per_hour.png'))
    plt.close()

    # 9) Summaries
    summary_cols = feature_cols + ['has_cbk']
    cluster_summary = df.groupby('cluster')[summary_cols].mean().sort_index()

    enriched_path = os.path.join(args.outdir, 'enriched_with_clusters.csv')
    summary_path = os.path.join(args.outdir, 'cluster_summary.csv')
    centers_path = os.path.join(args.outdir, 'kmeans_centers.csv')
    readme_path = os.path.join(args.outdir, 'readme.txt')

    df.to_csv(enriched_path, index=False)
    cluster_summary.to_csv(summary_path)
    pd.DataFrame(kmeans.cluster_centers_, columns=feature_cols).to_csv(centers_path, index_label='cluster')

    with open(readme_path, 'w') as f:
        f.write("Transaction clustering - Generated artifacts\n")
        f.write(f"- Input CSV: {args.csv}\n")
        f.write(f"- n_clusters: {args.n_clusters}\n")
        if sil is not None:
            f.write(f"- Silhouette score: {sil:.4f}\n")
        f.write("\nFiles:\n")
        f.write("- enriched_with_clusters.csv\n")
        f.write("- cluster_summary.csv\n")
        f.write("- kmeans_centers.csv\n")
        f.write("- pca_scatter.png\n")
        f.write("- tx_per_hour.png\n")
        f.write("- cbk_rate_per_hour.png\n")

    print(f"[OK] Saved to: {args.outdir}")
    if sil is not None:
        print(f"Silhouette score: {sil:.4f}")


if __name__ == '__main__':
    main()