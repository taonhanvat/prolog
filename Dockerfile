FROM golang:1.23-alpine AS builder
RUN apk add git protobuf-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /go/bin/prolog ./cmd/main.go

FROM golang:1.23-alpine
WORKDIR /app
COPY --from=builder /go/bin/prolog /bin/prolog
COPY ./cert /app/cert
COPY ./test /app/test
COPY ./dev.env /app/dev.env
ENTRYPOINT [ "/bin/prolog" ]