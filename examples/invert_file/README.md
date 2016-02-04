# Stack analysis

## Setup via docker-compose
* See [README](http://../../README.md#docker-container) to build `glow` docker image.
* Create `invert_file` container

### OSX
Cross compile artefact for docker
```
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build .
```
Build container with `invert_file` tag
```
docker build -t invert_file .
```

Start containers via [docker-compose](https://docs.docker.com/compose/)
```
docker-compose up
```
