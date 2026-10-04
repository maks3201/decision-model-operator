# Security Policy

## Reporting a vulnerability

Please report security issues **privately** via GitHub Security Advisories:
open a report at
<https://github.com/maks3201/decision-model-operator/security/advisories/new>.
Do not open a public issue for a suspected vulnerability.

Include, where possible: affected version, a description and impact, and steps
to reproduce (a minimal `DecisionModel` manifest and operator logs help). You
will get an acknowledgement, and a fix or mitigation will be coordinated before
any public disclosure.

## Supported versions

Only the latest minor release receives security fixes.

| Version        | Supported |
|----------------|-----------|
| latest minor   | yes       |
| older          | no        |

## Scope

In scope:

- the operator (controller, engine, CRD) in this repository;
- the Helm chart and release manifests published from this repository.

Out of scope (report upstream):

- the **Ollaya runtime** image (`ghcr.io/ollaya-dev/ollaya`) — report at
  <https://github.com/ollaya-dev/ollaya>;
- the **models** served (Laya, Kev, …) and the model registry — report to their
  respective projects.

If you are unsure whether an issue is in scope, report it privately here and it
will be triaged or redirected.
