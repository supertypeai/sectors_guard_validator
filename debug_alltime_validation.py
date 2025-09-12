"""
Script untuk debugging dan testing algoritma validasi all-time price data
menggunakan data dummy_alltime.json
"""

import json
import pandas as pd
import asyncio
import sys
import os

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.validators.idx_financial_validator import IDXFinancialValidator

async def test_alltime_validation():
    """Test the all-time price validation with sample data"""
    print("=== Testing IDX All-Time Price Validation ===\n")

    # Load sample data
    json_file_path = os.path.join(os.path.dirname(__file__), 'app', 'validators', 'dummy_alltime.json')

    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        print(f"Loaded {len(data)} records from dummy_alltime.json")

        # Convert to DataFrame
        df = pd.DataFrame(data)
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nData types:")
        print(df.dtypes)

        print(f"\nSample data (first record):")
        print(df.iloc[0].to_dict())

        # Check for missing values in key columns
        key_columns = ['symbol', 'type', 'date', 'price']
        print(f"\nMissing values in key columns:")
        for col in key_columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                print(f"  {col}: {missing_count} missing values")
            else:
                print(f"  {col}: COLUMN NOT FOUND")

        # Show unique symbols and date range
        if 'symbol' in df.columns:
            print(f"\nUnique symbols: {df['symbol'].unique()}")
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            print(f"Date range: {df['date'].min()} to {df['date'].max()}")

        # Test the validator
        print(f"\n=== Running All-Time Price Validation ===")
        validator = IDXFinancialValidator()
        result = await validator._validate_all_time_price(df)

        print(f"\nValidation Results:")
        print(f"Number of anomalies found: {len(result['anomalies'])}")
        if result['anomalies']:
            print(f"\nDetailed anomalies:")
            for i, anomaly in enumerate(result['anomalies'], 1):
                print(f"\n{i}. {anomaly['type']}")
                print(f"   Message: {anomaly['message']}")
                print(f"   Severity: {anomaly['severity']}")
                for key, value in anomaly.items():
                    if key not in ['type', 'message', 'severity']:
                        print(f"   {key}: {value}")
        else:
            print("No anomalies detected!")

        # Show some statistics about the data
        print(f"\n=== Data Statistics ===")
        if 'price' in df.columns:
            print(f"\nprice:")
            print(f"  Min: {df['price'].min():,.0f}")
            print(f"  Max: {df['price'].max():,.0f}")
            print(f"  Mean: {df['price'].mean():,.0f}")
    except FileNotFoundError:
        print(f"Error: Could not find dummy_alltime.json file at {json_file_path}")
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_alltime_validation())
