# Changelog

## [0.3.0](https://github.com/maks3201/decision-model-operator/compare/v0.2.1...v0.3.0) (2026-10-06)


### Features

* **api:** add spec.cache.downloadTokenSecretRef for private or gated weights ([003292f](https://github.com/maks3201/decision-model-operator/commit/003292fc26fe97fe0b38fe920d186ec3610a08e5))
* download model weights from a Hugging Face mirror (--ollaya-hf-endpoint) ([003292f](https://github.com/maks3201/decision-model-operator/commit/003292fc26fe97fe0b38fe920d186ec3610a08e5))
* **ollaya:** default to the Ollaya 0.10.0 runtime ([003292f](https://github.com/maks3201/decision-model-operator/commit/003292fc26fe97fe0b38fe920d186ec3610a08e5))

## [0.2.1](https://github.com/maks3201/decision-model-operator/compare/v0.2.0...v0.2.1) (2026-10-05)


### Features

* **olm:** describe DecisionModel fields and owned resources in the CSV ([e370f06](https://github.com/maks3201/decision-model-operator/commit/e370f0671211b77361959a7bd224d40544573a79))
* **release:** attach SLSA build provenance to the release files ([7a551e9](https://github.com/maks3201/decision-model-operator/commit/7a551e9db6a6317e747931c5eb2c59db4e1353d5))


### Bug Fixes

* **olm:** keep the sample dataset ConfigMap out of the bundle ([9439859](https://github.com/maks3201/decision-model-operator/commit/9439859df561f1003a4cbd6a8436e1ca3e5763bd))


### Documentation

* keep install versions current and describe the roadmap by stage ([#6](https://github.com/maks3201/decision-model-operator/issues/6)) ([cc19c3b](https://github.com/maks3201/decision-model-operator/commit/cc19c3b8db66305c471680228ec28ffe8d6e5c32))

## [0.2.0](https://github.com/maks3201/decision-model-operator/compare/v0.1.1...v0.2.0) (2026-10-05)


### Features

* **olm:** add an OLM bundle for OperatorHub ([20045f4](https://github.com/maks3201/decision-model-operator/commit/20045f4c65bcc69202d927f64fd851c67360a806))
* **release:** sign images, charts and release files with cosign; attach SBOM and provenance ([1bffd1d](https://github.com/maks3201/decision-model-operator/commit/1bffd1d1ff011b62053a6caa216fe04acee36443))


### Bug Fixes

* **engine:** make model name canonicalisation idempotent ([45244a7](https://github.com/maks3201/decision-model-operator/commit/45244a786a569d00e4b71179389333711078dc98))

## [0.1.1](https://github.com/maks3201/decision-model-operator/compare/v0.1.0...v0.1.1) (2026-10-05)


### Bug Fixes

* **chart:** use a valid Artifact Hub category and capability level ([2be718c](https://github.com/maks3201/decision-model-operator/commit/2be718c9b19121fd5f243b6bade0c1a9236e63f4))

## 0.1.0 (2026-10-04)


### Features

* initial release ([605cf4c](https://github.com/maks3201/decision-model-operator/commit/605cf4cc0022ed322f3474c50a0f2e893ba53ac6))
