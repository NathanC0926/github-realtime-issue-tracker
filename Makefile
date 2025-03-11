# Project directories
DOCKER_COMPOSE = docker-compose -f synapse/docker-compose.yml

# Start Docker Containers
up:
	@echo "🚀 Starting Docker Containers..."
	$(DOCKER_COMPOSE) up -d

# Stop Docker Containers
down:
	@echo "🛑 Stopping Docker Containers..."
	$(DOCKER_COMPOSE) down

# Restart Docker Containers
restart: down up

# Rebuild Docker Images and Restart
rebuild:
	@echo "🔄 Rebuilding Docker Containers..."
	$(DOCKER_COMPOSE) down
	$(DOCKER_COMPOSE) up -d --build

# Check Running Containers
status:
	@echo "📌 Checking running containers..."
	docker ps

# Remove all stopped containers & networks
clean:
	@echo "🧹 Cleaning up stopped containers and unused networks..."
	docker system prune -f
