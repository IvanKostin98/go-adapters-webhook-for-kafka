FROM golang:1.21-alpine
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY templates ./templates
RUN go build -o app
CMD ["/app/app"]
