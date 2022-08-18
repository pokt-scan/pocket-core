FROM golang:1.18-alpine as builder

# Install dependencies
RUN apk update && \
    apk -v --update --no-cache add \
		curl \
		git \
		groff \
		less \
		mailcap \
		gcc \
		libc-dev \
		bash \
    	curl \
    	leveldb-dev \
        leveldb-dev && \
    rm /var/cache/apk/* || true

# Environment and system dependencies setup
ENV POCKET_PATH=/go/src/github.com/pokt-network/pocket-core/
ENV GO111MODULE="on"
ENV GOOS=linux
ENV GOARCH=amd64

# Create node root directory
RUN mkdir -p ${POCKET_PATH}
WORKDIR $POCKET_PATH

# Copy source code
COPY . $POCKET_PATH

# Install project dependencies
RUN  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.26.0

RUN go mod vendor

RUN go mod download

# Install project dependencies and builds the binary
RUN go build -tags cleveldb -o ${GOBIN}/bin/pocket ${POCKET_PATH}/app/cmd/pocket_core/main.go
RUN mv ${POCKET_PATH}/docker/entrypoint.sh /bin/entrypoint.sh

FROM alpine:3.13
COPY --from=builder /bin/pocket /bin/pocket
COPY --from=builder /bin/entrypoint.sh /home/app/entrypoint.sh

RUN apk add --update --no-cache \
    expect \
    bash \
    leveldb-dev \
    tzdata \
    curl && \
	rm /var/cache/apk/* || true

RUN cp /usr/share/zoneinfo/America/New_York  /etc/localtime

# Create app user and add permissions
RUN addgroup --gid 1001 -S app \
	&& adduser --uid 1005 -S -G app app

RUN chown app:app /bin/pocket && chown app:app /home/app/entrypoint.sh

USER app

RUN mkdir -p /home/app/.pocket/config && chown -R app:app /home/app/.pocket

ENTRYPOINT ["/usr/bin/expect", "/home/app/entrypoint.sh"]
