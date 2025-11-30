FROM --platform=linux/amd64 golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY . .
RUN go build -o galactic-exchange .

FROM --platform=linux/amd64 alpine:latest
WORKDIR /root/
ENV PERSISTENT_DIR=/data
RUN mkdir -p /data
COPY --from=builder /app/galactic-exchange .
EXPOSE 8080
CMD ["./galactic-exchange"]