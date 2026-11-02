FROM golang:1.26-bookworm AS build
# libopus and libsoxr are needed to mix audio when several participants speak at
# once (the same libraries LiveKit's SIP service uses).
RUN apt-get update && apt-get install -y --no-install-recommends libopus-dev libsoxr-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -tags nolibopusfile -trimpath -ldflags="-s -w" -o /out/whatsapp-connector ./cmd/whatsapp-connector

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libopus0 libsoxr0 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/whatsapp-connector /usr/local/bin/whatsapp-connector
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/whatsapp-connector"]
