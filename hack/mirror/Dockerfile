# Mirror image for the e2e "registry mirror" spec (test/e2e/mirror_test.go).
# Built by hack/mirror-build.sh, which first pulls the model store into ./store so
# the whole model is baked in and the mirror serves it offline inside kind.
#
# Build context: hack/mirror/ with a populated ./store/models (blobs/ + manifests/).

# --- build the static mirror server (stdlib only; throwaway module) ---
FROM golang:1.27 AS build
WORKDIR /src
COPY main.go .
# main.go carries //go:build ignore so the main repo module never compiles it;
# build it here as its own throwaway module.
RUN go mod init dmo-e2e-mirror \
    && CGO_ENABLED=0 go build -o /mirror main.go

# --- final image: server + baked model store ---
FROM gcr.io/distroless/static:nonroot
COPY --from=build /mirror /mirror
COPY store/models /store/models
ENV MIRROR_STORE=/store/models
ENV MIRROR_ADDR=:8080
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/mirror"]
