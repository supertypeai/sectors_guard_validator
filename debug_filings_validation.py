import pandas as pd
from app.validators.idx_financial_validator import IDXFinancialValidator
import asyncio

# Load dummy filings data
filings_path = 'app/validators/dummy_filings.json'
with open(filings_path, 'r', encoding='utf-8') as f:
    filings_data = pd.read_json(f)

async def main():
    validator = IDXFinancialValidator()
    result = await validator._validate_filings(filings_data)
    print('Validation result for idx_filings:')
    for anomaly in result['anomalies']:
        print(anomaly)

if __name__ == '__main__':
    asyncio.run(main())
