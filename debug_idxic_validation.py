"""
Script untuk debugging dan testing validasi IDXIC classification
Memastikan sector/industry names sesuai dengan IDXIC reference dari CSV file

Requires .env file with:
- SUPABASE_URL
- SUPABASE_KEY
"""

import asyncio
import sys
import os
import io

# Fix Windows console encoding for Unicode
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the app directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from app.validators.idx_financial_validator import IDXFinancialValidator


async def test_idxic_validation():
    print("=" * 60)
    print("Testing IDXIC Classification Validation")
    print("=" * 60)
    print()

    validator = IDXFinancialValidator()

    # Test 1: Company Profile validation
    print("[TEST] idx_company_profile...")
    print("-" * 40)
    try:
        result = await validator.validate_table("idx_company_profile")

        # Filter IDXIC-related anomalies
        idxic_anomalies = [a for a in result.get('anomalies', []) if a.get('type') == 'idxic_name_mismatch']
        other_anomalies = [a for a in result.get('anomalies', []) if a.get('type') != 'idxic_name_mismatch']

        print(f"Status: {result.get('status')}")
        print(f"Total rows checked: {result.get('total_rows', 0)}")
        print(f"IDXIC mismatches found: {len(idxic_anomalies)}")
        print(f"Other anomalies found: {len(other_anomalies)}")

        if idxic_anomalies:
            print("\n[!] IDXIC Name Mismatches:")
            for i, anomaly in enumerate(idxic_anomalies[:10], 1):  # Show first 10
                print(f"  {i}. Symbol: {anomaly.get('symbol')}")
                print(f"     Field: {anomaly.get('field')}")
                print(f"     Value: {anomaly.get('value')}")
                print(f"     Message: {anomaly.get('message')}")
                print()

            if len(idxic_anomalies) > 10:
                print(f"  ... and {len(idxic_anomalies) - 10} more")
        else:
            print("\n[OK] No IDXIC mismatches found in company profiles!")

    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()

    print()
    print("=" * 60)

    # Test 2: Sector Reports validation
    print("\n[TEST] Testing idx_sector_reports...")
    print("-" * 40)
    try:
        result = await validator.validate_table("idx_sector_reports")

        # Filter IDXIC-related anomalies
        idxic_anomalies = [a for a in result.get('anomalies', []) if a.get('type') == 'idxic_name_mismatch']
        other_anomalies = [a for a in result.get('anomalies', []) if a.get('type') != 'idxic_name_mismatch']

        print(f"Status: {result.get('status')}")
        print(f"Total rows checked: {result.get('total_rows', 0)}")
        print(f"IDXIC mismatches found: {len(idxic_anomalies)}")
        print(f"Other anomalies found: {len(other_anomalies)}")

        if idxic_anomalies:
            print("\n[!] IDXIC Name Mismatches:")
            for i, anomaly in enumerate(idxic_anomalies[:10], 1):  # Show first 10
                print(f"  {i}. Sector: {anomaly.get('sector')}")
                print(f"     Sub-sector: {anomaly.get('sub_sector')}")
                print(f"     Field: {anomaly.get('field')}")
                print(f"     Value: {anomaly.get('value')}")
                print(f"     Message: {anomaly.get('message')}")
                print()

            if len(idxic_anomalies) > 10:
                print(f"  ... and {len(idxic_anomalies) - 10} more")
        else:
            print("\n[OK] No IDXIC mismatches found in sector reports!")

    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()

    print()
    print("=" * 60)
    print("Validation complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_idxic_validation())
