from flask import Flask, request, jsonify
from datetime import datetime
from db_utils import PostgresConnectionManager
import random
import re

import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

def is_valid_transaction(payload):
    """
    Mock logic to determine if a transaction should be approved or denied.
    This implements various business rules that might be used in real fraud detection.
    """
    try:

        # Extract transaction details
        transaction_amount = payload.get('transaction_amount', 0)
        card_number = payload.get('card_number', '')
        user_id = payload.get('user_id', 0)
        transaction_date = payload.get('transaction_date', '')

        # Gets user_id history
        conn = PostgresConnectionManager()
        query_path = "sql/feature_store.sql"
        with open(query_path, 'r', encoding='utf-8') as file:
            sql_query = file.read().strip()
        sql_query = f"{sql_query} and user_id = {user_id}"
        user_profile = conn.query_to_dataframe(sql_query)    
        
        def safe_get_scalar(series, default=0):
            if series is None or len(series) == 0:
                return default
            if hasattr(series, "iloc"):
                return series.iloc[0] if series.iloc[0] is not None else default
            return series

        distinct_cards_2_weeks = safe_get_scalar(user_profile.get('distinct_cards_2_weeks'),1)
        txns_by_user_last_1h_hour = safe_get_scalar(user_profile.get('txns_by_user_last_1h_hour'),20)
        num_cbk_card_bin_7d_percent = safe_get_scalar(user_profile.get('num_cbk_card_bin_7d_percent'),0)
        avg_txns_by_user_1h = safe_get_scalar(user_profile.get('avg_txns_by_user_1h'),20)
        avg_transaction_amount_7d = safe_get_scalar(user_profile.get('avg_transaction_amount_7d'),10000)
        user_cbk_count_lifetime_percent = safe_get_scalar(user_profile.get('user_cbk_count_lifetime_percent'),0)

        if user_cbk_count_lifetime_percent > 0:
            result = (False, "Transaction denied due to high chargeback history")
        elif txns_by_user_last_1h_hour >= 2*avg_txns_by_user_1h:
            result = (False, "Transaction denied due to high transaction volume")
        elif transaction_amount >= 2 * avg_transaction_amount_7d:
            result = (False, "Transaction denied due to high transaction value")
        elif distinct_cards_2_weeks >= 3:
            result = (False, "Transaction denied due to multiple cards used recently")
        elif num_cbk_card_bin_7d_percent >= 0.5:
            result = (False, "Transaction denied due to high chargeback rate for card type")
        else:
            result = (True, "Transaction approved")
 
        return result[0], result[1]
        
    except Exception as e:
        # If there's any error in processing, deny for safety
        return False, f"Processing error: {str(e)}"

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "transaction-processor"}), 200

@app.route('/transaction/evaluate', methods=['POST'])
def evaluate_transaction():
    """
    Endpoint to evaluate a transaction and return approval/denial recommendation
    """
    try:
        # Validate request has JSON content
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400
        
        payload = request.get_json()
        
        # Validate required fields
        required_fields = ['transaction_id', 'merchant_id', 'user_id', 'card_number', 
                          'transaction_date', 'transaction_amount', 'device_id']
        
        missing_fields = [field for field in required_fields if field not in payload]
        if missing_fields:
            return jsonify({
                "error": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
        
        # Get transaction ID
        transaction_id = payload.get('transaction_id')
        
        # Apply mock fraud detection logic
        is_approved, reason = is_valid_transaction(payload)
        
        # Prepare response
        response = {
            "transaction_id": transaction_id,
            "recommendation": "approve" if is_approved else "deny",
            "reason": reason,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Log the transaction (in a real system, this would go to a database)
        print(f"Transaction {transaction_id}: {response['recommendation']} - {reason}")
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

def main():
    """Main function to start the Flask application"""
    
    # Run the Flask app
    port = int(os.getenv('FLASK_PORT'))
    host = os.getenv('FLASK_HOST')
    
    print(f"Starting transaction evaluation service on {host}:{port}")
    app.run(host=host, port=port, debug=True)

if __name__ == "__main__":
    main()
