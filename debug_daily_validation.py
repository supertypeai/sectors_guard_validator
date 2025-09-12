"""
Script untuk debugging dan testing algoritma validasi daily financial data
menggunakan data dummy_daily.json
"""

import json
import pandas as pd
import asyncio
import sys
import os

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.validators.idx_financial_validator import IDXFinancialValidator

async def test_daily_validation():
    """Test the daily financial validation with sample data"""
    print("=== Testing IDX Daily Financial Validation ===\n")

    # Load sample data
    json_file_path = os.path.join(os.path.dirname(__file__), 'app', 'validators', 'dummy_daily.json')

    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        print(f"Loaded {len(data)} records from dummy_daily.json")

        # Convert to DataFrame
        df = pd.DataFrame(data)
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nData types:")
        print(df.dtypes)

        print(f"\nSample data (first record):")
        print(df.iloc[0].to_dict())

        # Check for missing values in key columns
        key_columns = ['symbol', 'date', 'close']
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
        print(f"\n=== Running Daily Financial Validation ===")
        validator = IDXFinancialValidator()
        result = await validator._validate_daily_data(df)

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
        numeric_columns = ['close', 'volume', 'market_cap']
        available_numeric = [col for col in numeric_columns if col in df.columns]
        for col in available_numeric:
            if not df[col].isna().all():
                print(f"\n{col}:")
                print(f"  Min: {df[col].min():,.0f}")
                print(f"  Max: {df[col].max():,.0f}")
                print(f"  Mean: {df[col].mean():,.0f}")
                if len(df) > 1:
                    pct_changes = df[col].pct_change() * 100
                    valid_changes = pct_changes.dropna()
                    if len(valid_changes) > 0:
                        print(f"  Daily changes: min={valid_changes.min():.1f}%, max={valid_changes.max():.1f}%, avg={valid_changes.abs().mean():.1f}%")
    except FileNotFoundError:
        print(f"Error: Could not find dummy_daily.json file at {json_file_path}")
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_daily_validation())
