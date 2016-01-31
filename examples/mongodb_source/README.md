# Source from MongoDB

## Setup via docker-compose
* See [README](http://../../README.md#docker-container) to build `glow` docker image.
* Create `mongodb_source` container

### OSX
Cross compile artefact for docker
```
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build .
```
Build container with `mongodb_source` tag
```
docker build -t stack_analysis .
```

Start containers via [docker-compose](https://docs.docker.com/compose/)
```
docker-compose up
```
