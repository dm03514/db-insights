FROM golang:1.14 as builder

WORKDIR /go/src/github.com/dm03514/db-insights
COPY . .

RUN GO111MODULE=on go get -d -v ./...
RUN GO111MODULE=on go install -v ./...


FROM debian:buster-slim

COPY --from=builder /go/bin/dbinsights /usr/local/bin

RUN useradd -m dbi
USER dbi

ENTRYPOINT ["/usr/local/bin/dbinsights"]
