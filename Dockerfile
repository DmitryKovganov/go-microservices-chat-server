FROM golang:1.20.3-alpine AS builder

COPY . /build/
WORKDIR /build/

RUN go mod download
RUN go build -o ./bin/chat_grpc_server cmd/grpc_server/main.go

FROM alpine:latest

WORKDIR /root/
COPY --from=builder /build/bin/chat_grpc_server .

CMD ["./chat_grpc_server"]