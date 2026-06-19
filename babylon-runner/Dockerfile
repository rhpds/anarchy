FROM golang:1.26.0 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags '-s -w' -o babylon-runner ./cmd/babylon-runner/

FROM registry.access.redhat.com/ubi9-micro
COPY --from=builder /app/babylon-runner /babylon-runner
ENTRYPOINT ["/babylon-runner"]
