FROM golang:1.25.9 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o babylon-runner .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/babylon-runner /babylon-runner
ENTRYPOINT ["/babylon-runner"]
