## Docker management

### Builds container

docker-compose build --no-cache app     
docker-compose up -d                    

#### Cleanup (if needed)

docker-compose down -v --remove-orphans

### Runs the container
docker-compose run --rm app bash

### Compose for dev
docker-compose up -d

## Run test payload

```
docker compose exec app curl -X POST http://localhost:5000/transaction/evaluate \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "tx12345",
    "merchant_id": "mrc6789",
    "user_id": 6,
    "card_number": "123456******7890",
    "transaction_date": "2025-09-29T14:30:00Z",
    "transaction_amount": 250.75,
    "device_id": "dev999"
  }'
```