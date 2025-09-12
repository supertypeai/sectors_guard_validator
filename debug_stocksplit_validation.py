import pandas as pd
from app.validators.idx_financial_validator import IDXFinancialValidator
import asyncio

# Load dummy stock split data
stocksplit_path = 'app/validators/dummy_stocksplit.json'
with open(stocksplit_path, 'r', encoding='utf-8') as f:
    stocksplit_data = pd.read_json(f)

async def main():
    validator = IDXFinancialValidator()
    result = await validator._validate_stock_split(stocksplit_data)
    print('Validation result for idx_stock_split:')
    for anomaly in result['anomalies']:
        print(anomaly)

if __name__ == '__main__':
    asyncio.run(main())
