"""
IDX-specific validation configuration and thresholds
"""

# IDX Financial Data Validation Configuration
IDX_VALIDATION_CONFIG = {
    # Annual financial data validation settings
    'idx_combine_financials_annual': {
        'validation_type': 'financial_annual',
        'description': 'Annual financial statements validation',
        'thresholds': {
            'extreme_change_threshold': 50.0,  # Percentage change > 50%
            'average_change_threshold': 30.0,  # Average change should be > 30% to trigger
            'min_years_required': 2,           # Minimum years of data needed
        },
        'metrics': [
            'revenue', 'net_income', 'total_assets', 'total_equity',
            'gross_profit', 'operating_income', 'ebitda'
        ],
        'email_recipients': [
            'financial-team@supertypeai.com',
            'data-alerts@supertypeai.com'
        ],
        'alert_level': 'warning'
    },
    
    # Quarterly financial data validation settings  
    'idx_combine_financials_quarterly': {
        'validation_type': 'financial_quarterly',
        'description': 'Quarterly financial statements validation',
        'thresholds': {
            'extreme_change_threshold': 50.0,  # Percentage change > 50%
            'average_change_threshold': 25.0,  # Lower threshold for quarterly data
            'min_quarters_required': 2,        # Minimum quarters of data needed
        },
        'metrics': [
            'revenue', 'net_income', 'total_assets', 'total_equity',
            'gross_profit', 'operating_income'
        ],
        'email_recipients': [
            'financial-team@supertypeai.com',
            'data-alerts@supertypeai.com'
        ],
        'alert_level': 'warning'
    },
    
    # Daily stock price data validation settings
    'idx_daily_data': {
        'validation_type': 'daily_price',
        'description': 'Daily stock price movements validation',
        'thresholds': {
            'extreme_price_change': 35.0,      # Price change > 35%
            'volume_spike_threshold': 300.0,   # Volume > 300% of average
            'min_days_required': 2,            # Minimum days of data needed
        },
        'metrics': [
            'close', 'open', 'high', 'low', 'volume'
        ],
        'email_recipients': [
            'trading-team@supertypeai.com',
            'market-alerts@supertypeai.com'
        ],
        'alert_level': 'critical'  # Price movements are more urgent
    },
    
    # Dividend data validation settings
    'idx_dividend': {
        'validation_type': 'dividend',
        'description': 'Dividend yield and changes validation',
        'thresholds': {
            'extreme_yield_threshold': 50.0,    # Yield > 50%
            'yield_change_threshold': 10.0,     # Yield change > 10 percentage points
            'suspicious_yield_threshold': 25.0,  # Yields > 25% are suspicious
        },
        'metrics': [
            'dividend_yield', 'dividend_amount', 'payout_ratio'
        ],
        'email_recipients': [
            'dividend-team@supertypeai.com',
            'income-alerts@supertypeai.com'
        ],
        'alert_level': 'warning'
    },
    
    # All-time price data validation settings
    'idx_all_time_price': {
        'validation_type': 'price_consistency',
        'description': 'All-time and periodic price data consistency',
        'thresholds': {
            'consistency_tolerance': 0.01,      # 1% tolerance for floating point errors
            'min_price_threshold': 0.01,        # Minimum valid price
            'max_price_ratio': 1000.0,          # Max ratio between high and low
        },
        'price_hierarchy': {
            'highs': ['30d_high', '90d_high', '52w_high', 'all_time_high'],
            'lows': ['30d_low', '90d_low', '52w_low', 'all_time_low']
        },
        'email_recipients': [
            'data-quality@supertypeai.com',
            'technical-team@supertypeai.com'
        ],
        'alert_level': 'critical'  # Data consistency is critical
    }
}

# Email notification templates for different alert levels
EMAIL_TEMPLATES = {
    'critical': {
        'subject_prefix': '🚨 CRITICAL',
        'priority': 'high',
        'escalation_minutes': 30
    },
    'warning': {
        'subject_prefix': '⚠️ WARNING',
        'priority': 'normal',
        'escalation_minutes': 120
    },
    'info': {
        'subject_prefix': 'ℹ️ INFO',
        'priority': 'low',
        'escalation_minutes': 480
    }
}

# Validation schedule configuration
VALIDATION_SCHEDULE = {
    'idx_daily_data': {
        'frequency': 'hourly',           # Check hourly during market hours
        'market_hours': {
            'start': '09:00',
            'end': '16:00',
            'timezone': 'Asia/Jakarta'
        },
        'off_hours_frequency': 'daily'   # Once daily outside market hours
    },
    'idx_dividend': {
        'frequency': 'daily',            # Check daily
        'preferred_time': '08:00'
    },
    'idx_combine_financials_quarterly': {
        'frequency': 'weekly',           # Check weekly
        'preferred_day': 'monday',
        'preferred_time': '07:00'
    },
    'idx_combine_financials_annual': {
        'frequency': 'monthly',          # Check monthly
        'preferred_date': 1,
        'preferred_time': '07:00'
    },
    'idx_all_time_price': {
        'frequency': 'daily',            # Check daily for consistency
        'preferred_time': '06:00'
    }
}

# Data quality rules for each table
DATA_QUALITY_RULES = {
    'idx_combine_financials_annual': {
        'required_columns': ['ticker', 'year', 'revenue', 'net_income'],
        'unique_constraints': [['ticker', 'year']],
        'null_tolerance': 0.05,          # Max 5% null values
        'date_range_check': True
    },
    'idx_combine_financials_quarterly': {
        'required_columns': ['ticker', 'year', 'quarter', 'revenue'],
        'unique_constraints': [['ticker', 'year', 'quarter']],
        'null_tolerance': 0.05,
        'date_range_check': True
    },
    'idx_daily_data': {
        'required_columns': ['ticker', 'date', 'close', 'volume'],
        'unique_constraints': [['ticker', 'date']],
        'null_tolerance': 0.01,          # Very low tolerance for daily data
        'date_range_check': True,
        'business_days_only': True
    },
    'idx_dividend': {
        'required_columns': ['ticker', 'ex_date', 'dividend_yield'],
        'unique_constraints': [['ticker', 'ex_date']],
        'null_tolerance': 0.02,
        'date_range_check': True
    },
    'idx_all_time_price': {
        'required_columns': ['ticker', 'all_time_high', 'all_time_low'],
        'unique_constraints': [['ticker']],
        'null_tolerance': 0.0,           # No nulls allowed for price data
        'price_consistency_check': True
    }
}

def get_table_config(table_name: str) -> dict:
    """Get validation configuration for a specific table"""
    return IDX_VALIDATION_CONFIG.get(table_name, {})

def get_validation_schedule(table_name: str) -> dict:
    """Get validation schedule for a specific table"""
    return VALIDATION_SCHEDULE.get(table_name, {'frequency': 'daily'})

def get_data_quality_rules(table_name: str) -> dict:
    """Get data quality rules for a specific table"""
    return DATA_QUALITY_RULES.get(table_name, {})
