#!/usr/bin/env python3
"""
Feature Store Query Runner

This script reads the SQL query from feature_store.sql and executes it using
the db_utils PostgresConnectionManager, returning the results as a pandas DataFrame.
"""

import os
import pandas as pd
from pathlib import Path
from db_utils import PostgresConnectionManager

# Load environment variables
from dotenv import load_dotenv
load_dotenv()


def main():
    """
    Main function to run the feature store query and display basic information about the results.
    """
    try:

        conn = PostgresConnectionManager()
    
        query_path = "sql/feature_store.sql"

        with open(query_path, 'r', encoding='utf-8') as file:
            sql_query = file.read().strip()

        sql_query = f"{sql_query} and user_id = 7"

        user_profile = conn.query_to_dataframe(sql_query)

        print(user_profile)

        # Gets specific feature value
        user_profile = user_profile.to_dict(orient='list')

        distinct_cards_2_weeks = user_profile.get('distinct_cards_2_weeks')[0] if user_profile.get('distinct_cards_2_weeks') else 0
        txns_by_user_last_1h_hour = user_profile.get('txns_by_user_last_1h_hour')[0] if user_profile.get('txns_by_user_last_1h_hour') else 0
        num_cbk_card_bin_7d_percent = user_profile.get('num_cbk_card_bin_7d_percent')[0] if user_profile.get('num_cbk_card_bin_7d_percent') else 0
        avg_txns_by_user_1h = user_profile.get('avg_txns_by_user_1h')[0] if user_profile.get('avg_txns_by_user_1h') else 0
        user_cbk_count_lifetime_percent = user_profile.get('user_cbk_count_lifetime_percent')[0] if user_profile.get('user_cbk_count_lifetime_percent') else 0

        if user_cbk_count_lifetime_percent > 0:
            result = (False, "Transaction denied due to high chargeback history")
        elif txns_by_user_last_1h_hour >= 2*avg_txns_by_user_1h:
            result = (False, "Transaction denied due to high transaction volume")
        # elif transaction_amount >= 2 * avg_transaction_amount_7d:
        #     result = (False, "Transaction denied due to high transaction value")
        elif distinct_cards_2_weeks >= 3:
            result = (False, "Transaction denied due to multiple cards used recently")
        elif num_cbk_card_bin_7d_percent >= 0.5:
            result = (False, "Transaction denied due to high chargeback rate for card type")
        else:
            result = (True, "Transaction approved")
 
        return result
        
    except Exception as e:
        print(f"Failed to execute feature store query: {e}")
        return None


if __name__ == "__main__":
    main()
