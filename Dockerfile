FROM golang:1.11

COPY . /go/src/github.com/Lookyan/jaeger-dependencies
WORKDIR /go/src/github.com/Lookyan/jaeger-dependencies

RUN go build -o ./bin/jaeger-dependencies ./cmd/main.go
RUN mv ./bin/jaeger-dependencies /usr/local/bin

ENTRYPOINT jaeger-dependencies
