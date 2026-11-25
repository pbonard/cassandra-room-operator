# The operator's image: one binary (manager, node-init, agent).
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/cassandra-room-operator ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/cassandra-room-operator /usr/local/bin/cassandra-room-operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/cassandra-room-operator"]
