# Pemetaan

We at [Zalando](https://tech.zalando.com) needed a central interface for working with all things related to the Saiki Project. That included:
- managing Kafka
  - overview on Cluster
  - manage Topics
  - monitor Topics
- creating additional Data in Zookeeper to make it available to other components
- Secure everything with Oauth2

Therefore we wrote Pemetaan (javanese for "Mapping"). Pemetaan is a Python3 Flask Application, wrapped in a WSGI container run by uWSGI. All Web Requests are managed by Nginx. The whole System is secured

## Functions

tbc

## Deployment

We suggest running the Docker Container directly. Build the Container:
```
docker build .
```

and then run the container in an environment where it can reach the Zookeeper Node of Kafka.
```
export CREDENTIALS_DIR=/dir/to/oauth/credentials
docker run \
  -u root \
  -p 80:80 -p 443:443 \
  -d \
  -e OAUTH_API_ENDPOINT=https://auth.example.com \
  -e TEAMCHECK_API=https://teams.example.com/api/teams/ \
  -e TEAMCHECK_ID=123345678 \
  -e ZOOKEEPER_CONN_STRING=zookeeper.example.com:2181 \
  -e APP_URL=https://pemetaan.example.com/ \
  -v $CREDENTIALS_DIR:/meta/credentials \
  -e CREDENTIALS_DIR=/meta/credentials \
  --name pemetaan \
  <PEMETAAN_IMAGE_ID>
```

