B=http://localhost:8000
K="Authorization: Bearer ep_test_key_1"
ID=$(uuidgen)

# 202 and a row
curl -i -X POST $B/v1/events -H "$K" -H 'content-type: application/json' \
  -d "{\"event_id\":\"$ID\",\"type\":\"order.created\",\"occurred_at\":\"$(date -u +%FT%TZ)\",\"schema_version\":1,\"payload\":{\"a\":1}}"

# same body again: 202, still one row
# bad key: 401 | bad uuid: 400 | big body: 413
curl -i -X POST $B/v1/events -H "Authorization: Bearer nope" -H 'content-type: application/json' -d '{}'
curl -i -X POST $B/v1/events -H "$K" -H 'content-type: application/json' -d '{"event_id":"x"}'
curl -i $B/healthz; curl -i $B/readyz; curl -s $B/debug/stats | jq
curl -s $B/metrics | grep -E '^ep_|^http_requests_total'

docker exec ep-postgres psql -U eventpulse -d eventpulse -c "select * from events"