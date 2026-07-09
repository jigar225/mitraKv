## Build stage: compile one static binary
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Copy dependency manifests first for layer caching
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary (no libc dependency)
RUN CGO_ENABLED=0 GOOS=linux go build -o /mitrakv ./cmd/mitrakv

## Runtime stage: minimal image with just binary + certs
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /mitrakv /usr/local/bin/mitrakv

VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/mitrakv"]
