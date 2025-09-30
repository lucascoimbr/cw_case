# populate_history.py
# Reads transactional-sample.csv and emits history.json used by antifraud_app.py
import pandas as pd
import json
import os

CSV_PATH = 'transactional-sample.csv'

if not os.path.exists(CSV_PATH):
    print('transactional-sample.csv not found in current directory. Please download it from the gist and place it here.')
    raise SystemExit(1)

df = pd.read_csv(CSV_PATH, sep=',')

print('Colunas disponíveis no DataFrame:', df.columns.tolist())

df['has_cbk'] = df['has_cbk'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False})

df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

# USER_HISTORY: cbk_count and simple list of txns (ts, amount) - for simulation
user_groups = df.groupby('user_id')
USER_HISTORY = {}
for uid, g in user_groups:
    cbk_count = int(g['has_cbk'].sum())
    # Corrigir a formatação de datas para o formato ISO usando strftime
    txns = [(ts.strftime('%Y-%m-%dT%H:%M:%S'), float(a)) for ts, a in zip(g['transaction_date'], g['transaction_amount'])]
    USER_HISTORY[str(uid)] = {'cbk_count': cbk_count, 'txns': txns}

# DEVICE_HISTORY: cbk_count and distinct cards used on the device
device_groups = df.groupby('device_id')
DEVICE_HISTORY = {}
for did, g in device_groups:
    cbk_count = int(g['has_cbk'].sum())
    distinct_cards = int(g['card_number'].nunique())
    DEVICE_HISTORY[str(did)] = {'cbk_count': cbk_count, 'distinct_cards_7d': distinct_cards}

out = {'USER_HISTORY': USER_HISTORY, 'DEVICE_HISTORY': DEVICE_HISTORY}
with open('history.json', 'w', encoding='utf-8') as fh:
    json.dump(out, fh, ensure_ascii=False, indent=2)

print('history.json created with {} users and {} devices'.format(len(USER_HISTORY), len(DEVICE_HISTORY)))
