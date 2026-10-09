# Mirror image for the e2e "registry mirror" spec (test/e2e/mirror_test.go).
# Built by hack/mirror-build.sh, which first pulls the model store into ./store so
# the whole model is baked in and the mirror serves it offline inside kind.
#
# Build context: hack/mirror/ with a populated ./store/models (blobs/ + manifests/).

# --- build the static mirror server (stdlib only; throwaway module) ---
FROM mirror.gcr.io/library/golang:1.27@sha256:e0174e51e81218523251d85d248a90d24c3d5e81543b4f07a5d66229397db190 AS build
WORKDIR /src
COPY main.go .
# main.go carries //go:build ignore so the main repo module never compiles it;
# build it here as its own throwaway module.
RUN go mod init dmo-e2e-mirror \
    && CGO_ENABLED=0 go build -o /mirror main.go

# --- final image: server + baked model store ---
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
COPY --from=build /mirror /mirror
COPY store/models /store/models
ENV MIRROR_STORE=/store/models
ENV MIRROR_ADDR=:8080
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/mirror"]
