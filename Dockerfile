FROM golang:alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/vegaserver ./cmd/vegaserver \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/vegactl ./cmd/vegactl
# create the data dir owned by the distroless nonroot user (uid 65532) so a fresh named volume is writable
RUN mkdir -p /out/data && chown 65532:65532 /out/data

# ---- runtime ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/vegaserver /vegaserver
COPY --from=build /out/vegactl /vegactl
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot:nonroot
VOLUME ["/data"]
EXPOSE 50051 8080
ENTRYPOINT ["/vegaserver"]
CMD ["--data-dir=/data"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD ["/vegaserver", "healthcheck", "--addr=127.0.0.1:50051"]
