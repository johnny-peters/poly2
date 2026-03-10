root@iZ6we39og54gbkqhawytaeZ:/opt/git_projects/poly2# cd /opt/git_projects/poly2
./target/release/poly2 healthcheck
healthcheck_passed=true

- [OK] clob_http_markets => status=200 OK, bytes=1821939, latency_ms=186
- [OK] clob_ws_tcp443 => host=ws-subscriptions-clob.polymarket.com, port=443, latency_ms=3
- [OK] polygon_chain_id => chain_id=0x89
- [OK] polygon_latest_block => latest_block=0x501e62a
- [OK] polygon_usdc_code => contract=0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174, code_present=true
