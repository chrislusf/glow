# Word count example

## Setup via docker-compose
* See [README](http://../../README.md#docker-container) to build `glow` docker image.
* 

### OSX
Cross compile artefact for docker
```
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build .
```
Build container with `word_count` tag
```
docker build -t word_count .
```

Start containers via [docker-compose](https://docs.docker.com/compose/)
```
docker-compose up
```
