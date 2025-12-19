#!/usr/bin/env bash
set -euo pipefail

python -m tdb build data/raw --db build/school.duckdb --profile .tdb_profile.json
python -m tdb validate --db build/school.duckdb --profile .tdb_profile.json
python -m tdb sql "SELECT 'customer' AS t, COUNT(*) AS n FROM customer UNION ALL SELECT 'orders', COUNT(*) FROM orders;" --db build/school.duckdb
python -m tdb sql "
SELECT c.region_id, COUNT(*) AS orders_n, SUM(o.total_price) AS revenue
FROM orders o
JOIN customer c ON c.customer_id = o.customer_id
GROUP BY c.region_id
ORDER BY revenue DESC
LIMIT 10;
" --db build/school.duckdb
