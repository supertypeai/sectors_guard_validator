from app.database.connection import get_supabase_client
from datetime import datetime, timedelta

supabase = get_supabase_client()

end = datetime.utcnow().date()
start = end - timedelta(days=6)
start_s = start.isoformat()
end_s = end.isoformat()
print(f"Checking idx_daily_data rows from {start_s} to {end_s}")

try:
    resp = supabase.table('idx_daily_data').select('*').gte('date', start_s).lte('date', end_s).execute()
    data = resp.data or []
    print('raw response length:', len(data))
    if data:
        # print sample first 5 rows (keys only)
        for i, r in enumerate(data[:5]):
            print(f"row {i} keys: {list(r.keys())}")
        # print first row snippet
        print('first row sample:', {k: data[0].get(k) for k in list(data[0].keys())[:8]})
    # Try counting unique (symbol,date) pairs
    try:
        pairs = set((r.get('symbol'), r.get('date')) for r in data)
        print('unique (symbol,date) pairs:', len(pairs))
    except Exception as e:
        print('could not compute unique pairs:', e)

except Exception as e:
    print('Error querying supabase:', e)
