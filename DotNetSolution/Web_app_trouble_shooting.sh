ECHO "sudo docker compose -f deploy/docker-compose.yml ps command-center"
sudo docker compose -f deploy/docker-compose.yml ps command-center

ECHO "sudo docker compose -f deploy/docker-compose.yml logs --tail=150 -f command-center"
sudo docker compose -f deploy/docker-compose.yml logs --tail=150 -f command-center

ECHO "sudo docker compose -f deploy/docker-compose.yml restart command-center"
docker compose -f deploy/docker-compose.yml restart command-center

ECHO "sudo docker compose -f deploy/docker-compose.yml logs -f --tail=80 command-center"
sudo docker compose -f deploy/docker-compose.yml logs -f --tail=80 command-center

