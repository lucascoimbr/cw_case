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

