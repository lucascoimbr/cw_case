import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("transactional-sample.csv")
df['transaction_date'] = pd.to_datetime(df['transaction_date'])
df['hour'] = df['transaction_date'].dt.hour
df['bin'] = df['card_number'].str[:6]

user_cbk_rate = df.groupby('user_id')['has_cbk'].mean()
device_user_count = df.groupby('device_id')['user_id'].nunique()
device_cbk_rate = df.groupby('device_id')['has_cbk'].mean()
bin_cbk_rate = df.groupby('bin')['has_cbk'].mean()

df['user_cbk_rate'] = df['user_id'].map(user_cbk_rate)
df['device_user_count'] = df['device_id'].map(device_user_count)
df['device_cbk_rate'] = df['device_id'].map(device_cbk_rate)
df['bin_cbk_rate'] = df['bin'].map(bin_cbk_rate)

features = [
    'transaction_amount',
    'hour',
    'user_cbk_rate',
    'device_user_count',
    'device_cbk_rate',
    'bin_cbk_rate'
]
X = df[features].fillna(0)

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
df['cluster'] = kmeans.fit_predict(X_scaled)

pca = PCA(n_components=2)
X_pca = pca.fit_transform(X_scaled)
df['pca1'], df['pca2'] = X_pca[:,0], X_pca[:,1]

plt.figure(figsize=(8,6))
sns.scatterplot(data=df, x='pca1', y='pca2', hue='cluster', style='has_cbk', palette='tab10')
plt.title("Clusters de transações (PCA 2D)")
plt.show()


# Perfil médio de cada cluster
cluster_summary = df.groupby('cluster')[features + ['has_cbk']].mean()
print(cluster_summary)