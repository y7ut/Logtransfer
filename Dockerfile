FROM golang:1.16.3-alpine3.13 as build

RUN set -ex \
&& go env -w GO111MODULE=on \
&& go env -w GOPROXY=https://goproxy.cn,direct

ENV CGO_ENABLED=0

WORKDIR /app

COPY . /app

RUN go build -o logtransfer ./

FROM alpine as production

WORKDIR /app

COPY --from=build /app /app

VOLUME /app/log/

ENTRYPOINT ["/app/logtransfer"]
