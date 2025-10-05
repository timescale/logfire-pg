FROM --platform=$BUILDPLATFORM golang:1.25.1 AS builder

ARG TARGETOS
ARG TARGETARCH

RUN echo "nobody:x:65534:65534:nobody:/:" > /etc_passwd

WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o logfire_pg \
    ./cmd/logfire_pg


FROM scratch AS final

USER nobody
EXPOSE 5432

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder --chown=nobody /tmp /tmp

COPY --from=builder /build/logfire_pg /logfire_pg

CMD ["/logfire_pg", "--host", "0.0.0.0"]
