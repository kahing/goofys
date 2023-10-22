FROM golang:1.14-alpine as build

ARG VERSION

WORKDIR /app
COPY . ./

RUN apk update \
    && apk add git

RUN go mod download \
    && go build -ldflags "-X main.Version=${VERSION}" -o goofys

FROM alpine:3.18.4

COPY --from=build /app/goofys /usr/local/bin
