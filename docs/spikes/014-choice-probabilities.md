# Spike 014 - Does Ollaya 0.18.0 always return `probabilities` for a choice answer?

Question: `scorePrediction` (`internal/controller/evaluator.go`) scores a `choice` answer with no
`probabilities` entry for the chosen label as confidence 0, and that 0 feeds ECE/Brier. If the runtime
never omits it, that path is unreachable for real answers. This spike measures it.

## Verdict

**Not observed in 48 choice answers: `probabilities` was always present, and the chosen label was always a
key of it (byte-for-byte, same case).** The sum of each distribution was 0.9999 to 1.0001 (rounding).
The zero-confidence fallback was never needed for runtime output in these runs. This is a sample, not a
proof of "never": CPU only, `laya:en` only, option counts 2 to 12.

Separate finding (not a bug in the scorer): the `confidence` field is **not** the probability of the chosen
label. It is a normalised margin above the uniform distribution: `(p_max - 1/K) / (1 - 1/K)` with K the
number of options. Verified on two responses below (K=2 and K=3). The scorer uses `probabilities[choice]`
for choice answers, so it is not affected; anyone reading `confidence` as a probability would be.

## Environment

- Image: `ghcr.io/ollaya-dev/ollaya:0.18.0`, CPU, Docker 29.8.2, macOS arm64 (OrbStack).
- Container: `--cpus=2 --memory=6g`, model `laya:en` (pulled, warmed with `keep_alive=-1`).
  `/api/ps` after warm-up: `name: laya:en`, `device: cpu`, `quantization_level: F32`.
- Port: the probe used host port 11445. Host port 11435 was already published by another container
  (`dmo-keda-ollaya`), which was not touched.
- Container removed after the run (`docker stop` and `docker rm`).

## Requests (trimmed)

All requests: `POST http://127.0.0.1:11445/v1/systemone`, body `{"model":"laya:en","state":…,"questions":{"q":…}}`.

- 2 options: state `"I was charged twice, please refund."`, criteria `opt0..opt1`.
- 5 options: state `"The app crashes when I open settings."`, criteria `opt0..opt4`.
- 12 options: state `"Please change my delivery address to the office."`, criteria `opt0..opt11`.
- Unicode and case labels: state `"Mein Konto ist gesperrt."`, criteria `Billing Dept`, `техподдержка`,
  `ÉQUIPE-2`, `MixedCase`.
- Low confidence: state `"qwerty zzz blorp 0x7f"`, criteria `billing`, `technical`, `sales`.
- Empty-ish state: state `""`, criteria `billing`, `technical`.
- Randomised sweep: 40 requests, 2 to 8 labels drawn from ten labels (including `Technical`, `sales & marketing`,
  `Ärger`, `漢字`, `x.y`, `quo"te`, `a-b_c`, `UPPER`, `tab\tlabel`), 8 different states (EN, DE, JA), fixed seed 117.
- Golden choice examples from `docs/evaluation.md` (the only golden examples in the repo; there is no
  `samples/` directory): 2 requests.
- Comparison: one `noul` (`{"type":"noul"}`) and one `score` (`criteria` of three levels), once each.

## Results

| Set | Requests | HTTP 200 | `probabilities` present | chosen label is a key | sum in [0.998, 1.002] |
|---|---|---|---|---|---|
| Base cases (2, 5, 12 options, unicode, low confidence, empty state) | 6 | 6 | 6 | 6 | 6 |
| Randomised sweep (2 to 8 labels) | 40 | 40 | 40 | 40 | 40 |
| Golden examples (`docs/evaluation.md`) | 2 | 2 | 2 | 2 | 2 |
| **Choice total** | **48** | **48** | **48** | **48** | **48** |
| `noul` (once) | 1 | 1 | n/a | n/a | n/a |
| `score` (once) | 1 | 1 | `probabilities` `{"0","1","2"}` present, sum 1.0 (0.1727 + 0.777 + 0.0503) | n/a | n/a |

Confidence semantics, checked on two responses (K = number of options):

| Response | K | chosen `p` | `confidence` | `(p - 1/K)/(1 - 1/K)` |
|---|---|---|---|---|
| 2 options | 2 | 0.6367 | 0.2734 | 0.2734 |
| low confidence | 3 | 0.3814 | 0.0721 | 0.0721 |

## Verbatim responses

Normal (2 options, `"I was charged twice, please refund."`):

```json
{"model": "laya:en", "answers": {"q": {"type": "choice", "choice": "opt0", "confidence": 0.2734, "probabilities": {"opt0": 0.6367, "opt1": 0.3633}}}, "usage": {"input_tokens": 32, "output_tokens": 0}}
```

Edge (low confidence, `"qwerty zzz blorp 0x7f"`, 3 options):

```json
{"model": "laya:en", "answers": {"q": {"type": "choice", "choice": "technical", "confidence": 0.0721, "probabilities": {"billing": 0.3395, "technical": 0.3814, "sales": 0.2792}}}, "usage": {"input_tokens": 34, "output_tokens": 0}}
```

Comparison responses (verbatim answer objects):

```json
{"type": "noul", "noul": 0.8032}
{"type": "score", "score": 0.8777, "confidence": 0.6655, "legend": {"0": "minor", "1": "major", "2": "critical"}, "probabilities": {"0": 0.1727, "1": 0.777, "2": 0.0503}}
```

## Requests

- None for code. The current scorer already reads `probabilities[choice]` for choice answers and does not
  read `confidence`, so there is no change needed for the measured behaviour.
- If a dashboard or doc shows `confidence` as "probability of the answer", it is wrong for
  choice answers. The margin formula should be stated in `docs/evaluation.md` (not changed here; out of scope).

## Not checked

- Other models (`laya:multilingual`, `nli`, `gliclass`, `decider`) and other runtimes.
- GPU / `-cuda` image.
- Concurrency, 503 `QUEUE_FULL`, and option counts near the model limit (about 125 for `laya:en`, per docs).
- Prompts designed to make the model refuse or emit an unlisted label. The sweep never produced an unlisted
  label, so this case is untested.
- The golden set in `docs/evaluation.md` has only 2 cases, so "20 golden examples" was not possible; the
  sweep was used to reach the sample size.

## Verification (commands and key output)

- `docker run -d --name a117-ollaya --cpus=2 --memory=6g -p 127.0.0.1:11445:11435 ghcr.io/ollaya-dev/ollaya:0.18.0`
  → `Ollaya is running`, `ollaya version is 0.18.0`.
- `docker exec a117-ollaya ollaya pull laya:en` → exit 0.
- `POST /api/decide {"model":"laya:en","keep_alive":-1}` → HTTP 200.
- Probe and sweep (Python `urllib`, outside the repo): `choice rows=42 ok=42 bad=0`, plus the 6 base cases
  (48 choice responses in total, 0 bad).
- `docker stop a117-ollaya && docker rm a117-ollaya`; `docker ps` lists only the pre-existing containers.
