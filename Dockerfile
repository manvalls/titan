# Titan build

FROM golang:1.9-alpine AS titan
RUN apk update
RUN set -ex
RUN apk add --no-cache --virtual .build-deps gcc libc-dev git

RUN go get github.com/urfave/cli
RUN go get github.com/manvalls/fuse
RUN go get github.com/aws/aws-sdk-go
RUN go get github.com/go-sql-driver/mysql
RUN go get github.com/oklog/ulid

ADD . /go/src/github.com/manvalls/titan
WORKDIR /go/src/github.com/manvalls/titan/cmd/titan
RUN go install --ldflags '-extldflags "-static"'

# Final image

FROM debian
RUN apt-get update && apt-get install -y \
    fuse musl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=titan /go/bin/titan /usr/local/bin

ENV TITAN_CACHE_FOLDER=/titan-cache
ENV TITAN_MOUNT_POINT=/mnt/titan

RUN mkdir -p /titan-cache /mnt/titan
CMD ["/usr/local/bin/titan", "mount"]
