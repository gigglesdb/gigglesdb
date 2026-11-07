FROM golang:1.23-bookworm AS builder
WORKDIR /probe
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /probe/pgx-probe .
FROM debian:bookworm-slim
COPY --from=builder /probe/pgx-probe /probe/pgx-probe
ENTRYPOINT ["/probe/pgx-probe"]
