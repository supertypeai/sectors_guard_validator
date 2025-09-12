#!/usr/bin/env python3
"""
IDX Data Validation Runner
Standalone script to validate IDX financial tables
"""

import asyncio
import sys
import os
from datetime import datetime
import json

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.validators.idx_financial_validator import IDXFinancialValidator
from app.notifications.email_service import EmailService
from app.database.connection import get_supabase_client, init_database

async def validate_single_table(table_name: str, send_email: bool = True):
    """Validate a single IDX table"""
    print(f"\n🔍 Validating table: {table_name}")
    
    try:
        validator = IDXFinancialValidator()
        result = await validator.validate_table(table_name)
        
        # Print summary
        print(f"✅ Validation completed for {table_name}")
        print(f"   Status: {result.get('status', 'unknown')}")
        print(f"   Total rows: {result.get('total_rows', 0)}")
        print(f"   Anomalies found: {result.get('anomalies_count', 0)}")
        
        # Print anomalies if any
        if result.get('anomalies_count', 0) > 0:
            print(f"\n⚠️  Anomalies detected in {table_name}:")
            for i, anomaly in enumerate(result.get('anomalies', []), 1):
                print(f"   {i}. {anomaly.get('type', 'unknown')}: {anomaly.get('message', 'No message')}")
            
            # Send email if requested
            if send_email:
                try:
                    email_service = EmailService()
                    email_sent = await email_service.send_anomaly_alert(table_name, result)
                    if email_sent:
                        print(f"   📧 Anomaly alert email sent successfully")
                    else:
                        print(f"   ❌ Failed to send anomaly alert email")
                except Exception as e:
                    print(f"   ❌ Email sending error: {e}")
        else:
            print(f"   ✅ No anomalies detected")
        
        return result
        
    except Exception as e:
        print(f"❌ Error validating {table_name}: {e}")
        return None

async def validate_all_idx_tables(send_email: bool = True):
    """Validate all IDX financial tables"""
    print("🚀 Starting IDX Data Validation")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize database connection
    if not init_database():
        print("❌ Failed to initialize database connection")
        return False
    
    # Define IDX tables to validate
    idx_tables = [
        'idx_combine_financials_annual',
        'idx_combine_financials_quarterly',
        'idx_daily_data',
        'idx_dividend',
        'idx_all_time_price'
    ]
    
    results = {}
    total_anomalies = 0
    
    for table_name in idx_tables:
        result = await validate_single_table(table_name, send_email)
        if result:
            results[table_name] = result
            total_anomalies += result.get('anomalies_count', 0)
    
    # Print final summary
    print(f"\n📊 Validation Summary")
    print(f"=" * 50)
    print(f"Tables validated: {len(results)}")
    print(f"Total anomalies: {total_anomalies}")
    
    for table_name, result in results.items():
        status_emoji = "✅" if result.get('status') == 'success' else "⚠️" if result.get('status') == 'warning' else "❌"
        print(f"{status_emoji} {table_name}: {result.get('status', 'unknown')} ({result.get('anomalies_count', 0)} anomalies)")
    
    # Save results to file
    summary_file = f"validation_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(summary_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'total_tables': len(results),
                'total_anomalies': total_anomalies,
                'results': results
            }, f, indent=2, default=str)
        print(f"📄 Results saved to: {summary_file}")
    except Exception as e:
        print(f"❌ Failed to save results: {e}")
    
    return total_anomalies == 0

async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='IDX Data Validation Runner')
    parser.add_argument('--table', type=str, help='Validate specific table only')
    parser.add_argument('--no-email', action='store_true', help='Skip sending email alerts')
    parser.add_argument('--list-tables', action='store_true', help='List available IDX tables')
    
    args = parser.parse_args()
    
    if args.list_tables:
        print("Available IDX tables:")
        tables = [
            'idx_combine_financials_annual',
            'idx_combine_financials_quarterly',
            'idx_daily_data',
            'idx_dividend',
            'idx_all_time_price'
        ]
        for table in tables:
            print(f"  - {table}")
        return
    
    send_email = not args.no_email
    
    if args.table:
        # Validate single table
        result = await validate_single_table(args.table, send_email)
        sys.exit(0 if result and result.get('status') != 'error' else 1)
    else:
        # Validate all tables
        success = await validate_all_idx_tables(send_email)
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Validation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)
