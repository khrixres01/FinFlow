import random
import uuid
import csv
import os
from datetime import datetime, timedelta

# ── Configuration ─────────────────────────────────────────
NUM_TRANSACTIONS = 1000
OUTPUT_FILE      = 'transactions.csv'

# ── Reference Data ────────────────────────────────────────
CUSTOMER_IDS = ['CUST_' + str(i).zfill(4) for i in range(1, 101)]
ACCOUNT_IDS  = ['ACC_'  + str(i).zfill(4) for i in range(1, 201)]
CHANNELS     = ['mobile', 'atm', 'web', 'branch']
TX_TYPES     = ['transfer', 'payment', 'withdrawal', 'deposit']
STATUSES     = ['success', 'success', 'success', 'success', 'failed', 'pending']
CITIES       = ['Lagos', 'Abuja', 'London', 'New York', 'Dubai', 'Accra', 'Nairobi']

def generate_transaction():
    """Generate one realistic bank transaction"""

    customer_id  = random.choice(CUSTOMER_IDS)
    account_from = random.choice(ACCOUNT_IDS)
    account_to   = random.choice([a for a in ACCOUNT_IDS if a != account_from])

    # Most transactions are small, some are large
    amount_type = random.random()
    if amount_type < 0.70:
        amount = round(random.uniform(10, 5000), 2)
    elif amount_type < 0.90:
        amount = round(random.uniform(5000, 100000), 2)
    elif amount_type < 0.97:
        amount = round(random.uniform(100000, 500000), 2)
    else:
        # Occasionally very large — will trigger fraud flag
        amount = round(random.uniform(1000000, 5000000), 2)

    # Occasionally generate round amounts — another fraud signal
    if random.random() < 0.05:
        amount = round(amount / 1000) * 1000

    # Most transactions happen during business hours
    hour_rand = random.random()
    if hour_rand < 0.75:
        hour = random.randint(8, 20)
    elif hour_rand < 0.90:
        hour = random.randint(20, 23)
    else:
        # Off hours — will trigger fraud flag
        hour = random.randint(1, 4)

    now = datetime.now()
    timestamp = now.replace(
        hour=hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59)
    )

    return {
        'transaction_id':   str(uuid.uuid4()),
        'timestamp':        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'customer_id':      customer_id,
        'account_from':     account_from,
        'account_to':       account_to,
        'amount':           amount,
        'transaction_type': random.choice(TX_TYPES),
        'channel':          random.choice(CHANNELS),
        'status':           random.choice(STATUSES),
        'city':             random.choice(CITIES)
    }

def generate_batch(n=NUM_TRANSACTIONS):
    """Generate a batch of n transactions"""
    return [generate_transaction() for _ in range(n)]

def save_to_csv(transactions, filename=OUTPUT_FILE):
    """Save transactions to CSV file"""
    if not transactions:
        return

    fieldnames = transactions[0].keys()
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)

    print('Generated {} transactions → {}'.format(len(transactions), filename))
    return filename

def upload_to_hdfs(local_file):
    """Upload CSV file to HDFS landing zone"""
    now        = datetime.now()
    hdfs_dir   = '/data/raw/transactions/{}/{}/{}/{}/'.format(
        now.year,
        str(now.month).zfill(2),
        str(now.day).zfill(2),
        str(now.hour).zfill(2)
    )
    hdfs_file  = hdfs_dir + 'transactions_' + now.strftime('%Y%m%d_%H%M%S') + '.csv'

    # Create directory in HDFS
    os.system('hdfs dfs -mkdir -p {}'.format(hdfs_dir))

    # Upload file
    os.system('hdfs dfs -put {} {}'.format(local_file, hdfs_file))

    print('Uploaded to HDFS: {}'.format(hdfs_file))
    return hdfs_file

if __name__ == '__main__':
    print('Starting FinFlow transaction generator...')
    print('Generating {} transactions...'.format(NUM_TRANSACTIONS))

    transactions = generate_batch(NUM_TRANSACTIONS)
    local_file   = save_to_csv(transactions)

    print('Sample transaction:')
    for k, v in list(transactions[0].items()):
        print('  {}: {}'.format(k, v))