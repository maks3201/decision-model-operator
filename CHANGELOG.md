# Changelog

## [0.4.0](https://github.com/maks3201/decision-model-operator/compare/v0.3.0...v0.4.0) (2026-10-09)


### ⚠ BREAKING CHANGES

* refuse a new revision with an unpinned runtime image unless --allow-unpinned-runtime-images is set
* bind manual approval to the revision, evaluation policy and dataset

### Features

* 64-bit revision hashes and digest-pinned runtime images; Ollaya 0.12.0 is the default runtime ([a472e7c](https://github.com/maks3201/decision-model-operator/commit/a472e7c46add4e439721b3af1f161c3326537080))
* **api:** add spec.rollout.promotion (Automatic, EvaluationGated, Manual) ([323308f](https://github.com/maks3201/decision-model-operator/commit/323308f85d0cda9fba5f63010399fcc52860a1bb))
* **api:** add spec.runtimeVersion and pin the runtime across operator upgrades ([9128379](https://github.com/maks3201/decision-model-operator/commit/912837945a0707ec8632c6469517e12815a74eb9))
* **chart:** add a values schema and per-release Artifact Hub changes ([#22](https://github.com/maks3201/decision-model-operator/issues/22)) ([f6aea22](https://github.com/maks3201/decision-model-operator/commit/f6aea2213c97f1cc7e6ed0e058a9d48356254fb0))
* **chart:** optional PrometheusRule, Grafana dashboard and runbooks ([ea4024a](https://github.com/maks3201/decision-model-operator/commit/ea4024a22b370f4765ae261abd0014dda155ae53))
* **chart:** optional ServiceMonitor for the metrics Service ([995e621](https://github.com/maks3201/decision-model-operator/commit/995e62122c83cd0211e0fde6b160be83bd9b1d4f))
* **chart:** values for the runtime version policy and the rollout budget ([#42](https://github.com/maks3201/decision-model-operator/issues/42)) ([cc9a44c](https://github.com/maks3201/decision-model-operator/commit/cc9a44c251a6982294895a29565d099dcbb4087e))
* **eval:** per-question precision, recall and macro-F1 ([#34](https://github.com/maks3201/decision-model-operator/issues/34)) ([f03e2cf](https://github.com/maks3201/decision-model-operator/commit/f03e2cfbb41b42a94460ea17fb2761626fab2f21))
* **eval:** scoring helpers for score questions ([#40](https://github.com/maks3201/decision-model-operator/issues/40)) ([093c9d3](https://github.com/maks3201/decision-model-operator/commit/093c9d326e7b042466be1c1a83e5906227ec3f00))
* limit concurrent rollouts with --max-concurrent-rollouts ([9128379](https://github.com/maks3201/decision-model-operator/commit/912837945a0707ec8632c6469517e12815a74eb9))
* **metrics:** add decisionmodel_revision_info ([323308f](https://github.com/maks3201/decision-model-operator/commit/323308f85d0cda9fba5f63010399fcc52860a1bb))
* name models by digest in Events and echo the applied gates in status ([323308f](https://github.com/maks3201/decision-model-operator/commit/323308f85d0cda9fba5f63010399fcc52860a1bb))
* name the revision in promotion and rollback Events ([a472e7c](https://github.com/maks3201/decision-model-operator/commit/a472e7c46add4e439721b3af1f161c3326537080))
* **ollaya:** default resources for jevk5:latest, the first measured GGUF model ([#18](https://github.com/maks3201/decision-model-operator/issues/18)) ([ddf6d81](https://github.com/maks3201/decision-model-operator/commit/ddf6d816d5578fbb919aae7e85b1544f287587c1))
* **ollaya:** fail the prefetch Job fast on permanent download errors ([#43](https://github.com/maks3201/decision-model-operator/issues/43)) ([f673851](https://github.com/maks3201/decision-model-operator/commit/f67385145dc335fe3c69ba0bd5c820d087d97e4c))
* **ollaya:** name both digests when a prefetch finds a moved tag or a wrong manifest ([a472e7c](https://github.com/maks3201/decision-model-operator/commit/a472e7c46add4e439721b3af1f161c3326537080))
* **ollaya:** rebuild a store to the recorded digest from a seeded manifest ([995e621](https://github.com/maks3201/decision-model-operator/commit/995e62122c83cd0211e0fde6b160be83bd9b1d4f))
* **ollaya:** report UpstreamTagMoved when a pinned tag moved upstream ([ea4024a](https://github.com/maks3201/decision-model-operator/commit/ea4024a22b370f4765ae261abd0014dda155ae53))
* persist each revision's manifest so a lost store is rebuilt to the recorded digest ([a472e7c](https://github.com/maks3201/decision-model-operator/commit/a472e7c46add4e439721b3af1f161c3326537080))
* refuse a new revision with an unpinned runtime image unless --allow-unpinned-runtime-images is set ([a472e7c](https://github.com/maks3201/decision-model-operator/commit/a472e7c46add4e439721b3af1f161c3326537080))
* roll back to the previous model if the new one fails during a stabilization window ([#44](https://github.com/maks3201/decision-model-operator/issues/44)) ([b9c531e](https://github.com/maks3201/decision-model-operator/commit/b9c531ebd1b471e9962f9f21430f74ecaac4c186))
* separate Secret labels for API keys, eval datasets and download tokens ([9128379](https://github.com/maks3201/decision-model-operator/commit/912837945a0707ec8632c6469517e12815a74eb9))
* show active and candidate model, accuracy and reason in kubectl get ([323308f](https://github.com/maks3201/decision-model-operator/commit/323308f85d0cda9fba5f63010399fcc52860a1bb))
* show why a prefetch failed in status and Events ([#48](https://github.com/maks3201/decision-model-operator/issues/48)) ([050e50e](https://github.com/maks3201/decision-model-operator/commit/050e50ee3c7ae0152762b40b35cd7e2b0b189a7d))


### Bug Fixes

* a refused first rollout releases its in-flight candidate's workloads and rollout slot ([a472e7c](https://github.com/maks3201/decision-model-operator/commit/a472e7c46add4e439721b3af1f161c3326537080))
* bind manual approval to the revision, evaluation policy and dataset ([9128379](https://github.com/maks3201/decision-model-operator/commit/912837945a0707ec8632c6469517e12815a74eb9))
* count stabilizing revisions in the rollout budget; keep rollback protection until admitted ([995e621](https://github.com/maks3201/decision-model-operator/commit/995e62122c83cd0211e0fde6b160be83bd9b1d4f))
* **deps:** patch Go 1.26.9 and golang.org/x/net v0.60.0 security fixes ([#78](https://github.com/maks3201/decision-model-operator/issues/78)) ([3af48b2](https://github.com/maks3201/decision-model-operator/commit/3af48b20f7e96dc3ded5175322e93ab53c31ca84))
* do not roll back a healthy stable while its Deployment is rolling out ([7f83f7d](https://github.com/maks3201/decision-model-operator/commit/7f83f7d56c711f67abb73bc4fac5675fcbc4ded5))
* **eval:** hold the candidate while its dataset is missing instead of rolling it back ([323308f](https://github.com/maks3201/decision-model-operator/commit/323308f85d0cda9fba5f63010399fcc52860a1bb))
* **eval:** invalidate a held result when the dataset content changes ([9128379](https://github.com/maks3201/decision-model-operator/commit/912837945a0707ec8632c6469517e12815a74eb9))
* **eval:** score questions are evaluated instead of always counted wrong ([9128379](https://github.com/maks3201/decision-model-operator/commit/912837945a0707ec8632c6469517e12815a74eb9))
* fail a calibration gate with no calibrated cases and version the scorer ([995e621](https://github.com/maks3201/decision-model-operator/commit/995e62122c83cd0211e0fde6b160be83bd9b1d4f))
* keep maintaining the serving stable when the new spec fails preflight ([#55](https://github.com/maks3201/decision-model-operator/issues/55)) ([f1791e6](https://github.com/maks3201/decision-model-operator/commit/f1791e6541e48aafa8bd4d7be09973418383e0e7))
* make the score tolerance part of the evaluation identity ([#54](https://github.com/maks3201/decision-model-operator/issues/54)) ([47e245c](https://github.com/maks3201/decision-model-operator/commit/47e245c15fbc9146210e3508bc0885bc07407827))
* never collect the store or the rollback target the stable still needs ([7f83f7d](https://github.com/maks3201/decision-model-operator/commit/7f83f7d56c711f67abb73bc4fac5675fcbc4ded5))
* **ollaya:** keep score probabilities and mark rejected decide requests ([634fe14](https://github.com/maks3201/decision-model-operator/commit/634fe14c68e90e218883f6dddc3d40531ba95e6f))
* **ollaya:** refuse runtime redirects and classify a registry 404 before reading the body ([#53](https://github.com/maks3201/decision-model-operator/issues/53)) ([66f5f21](https://github.com/maks3201/decision-model-operator/commit/66f5f21ceb75a775c6f6b8074276538b2d774a7d))
* PDB before the traffic switch, missing Secrets as conditions, one-shot retry token, registry flag validation ([4975939](https://github.com/maks3201/decision-model-operator/commit/4975939ce22cefa0ac19df25ad15a49c7fb56553))
* race-free rollout budget admission; no PVC before admission ([995e621](https://github.com/maks3201/decision-model-operator/commit/995e62122c83cd0211e0fde6b160be83bd9b1d4f))
* record a candidate failure before deleting it; keep the store recovery bound in status ([995e621](https://github.com/maks3201/decision-model-operator/commit/995e62122c83cd0211e0fde6b160be83bd9b1d4f))
* record a post-promotion rollback in status before moving traffic ([634fe14](https://github.com/maks3201/decision-model-operator/commit/634fe14c68e90e218883f6dddc3d40531ba95e6f))
* reject an API-key Secret without the referenced key ([9128379](https://github.com/maks3201/decision-model-operator/commit/912837945a0707ec8632c6469517e12815a74eb9))
* report a rejected candidate on Resolved and abandon a superseded candidate ([ea4024a](https://github.com/maks3201/decision-model-operator/commit/ea4024a22b370f4765ae261abd0014dda155ae53))
* spec.image sets the serving image and is part of the revision, not only the prefetch Job ([a472e7c](https://github.com/maks3201/decision-model-operator/commit/a472e7c46add4e439721b3af1f161c3326537080))
* stop publishing a hash of the API key; hold the rollout on a rejected golden case ([4975939](https://github.com/maks3201/decision-model-operator/commit/4975939ce22cefa0ac19df25ad15a49c7fb56553))
* trust a prefetch Pod only through its Job UID ([995e621](https://github.com/maks3201/decision-model-operator/commit/995e62122c83cd0211e0fde6b160be83bd9b1d4f))

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
