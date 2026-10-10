/*
Copyright 2026 maks3201.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ollaya

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// This guard catches the class of bug where a shipped golden dataset uses a
// question shape the Ollaya runtime rejects (e.g. choice with "options" instead
// of "criteria"), which otherwise only surfaces at runtime as a 422 that parks
// the evaluation in DatasetInvalid forever. It scans every embedded dataset in
// config/samples/*.yaml and docs/*.md (any JSON object carrying a "questions"
// key), and validates each question against the documented /v1/systemone request
// shape the engine's Decider sends (questions are passed through verbatim —
// internal/engine/ollaya/decide.go systemOneRequest.Questions is json.RawMessage
// — so the shape must be correct at authoring time).
//
// Documented shapes (source: docs/evaluation.md):
//   - "choice": requires a non-empty "criteria" object (label -> description map),
//     e.g. {"type":"choice","criteria":{"billing":"payments and refunds",...}}
//     (evaluation.md:16-17, 78-79). "options" is NOT accepted by the runtime.
//   - "score":  {"type":"score"} with no criteria/options (evaluation.md:34).
//   - "noul":   {"type":"noul"} (a bool/probability question) with no
//     criteria/options (evaluation.md:34, 169-170 "bool (noul)").
// The three types are exactly the ones the engine maps (decide.go mapAnswers:
// choice/score/noul); any other type is flagged so a typo cannot ship.

// validateQuestion checks one question object against the documented shape.
// Returns "" when valid, else a human-readable reason.
func validateQuestion(id string, q map[string]json.RawMessage) string {
	rawType, ok := q["type"]
	if !ok {
		return "question " + id + ": missing \"type\""
	}
	var typ string
	if err := json.Unmarshal(rawType, &typ); err != nil {
		return "question " + id + ": \"type\" is not a string"
	}
	switch typ {
	case "choice":
		if _, hasOptions := q["options"]; hasOptions {
			if _, hasCriteria := q["criteria"]; !hasCriteria {
				return "question " + id + ": choice uses \"options\" but the runtime requires \"criteria\" (a label->description map); rename options -> criteria"
			}
		}
		crit, ok := q["criteria"]
		if !ok {
			return "question " + id + ": choice requires a non-empty \"criteria\" object"
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(crit, &m); err != nil {
			return "question " + id + ": choice \"criteria\" must be a JSON object (label->description)"
		}
		if len(m) == 0 {
			return "question " + id + ": choice \"criteria\" must be non-empty"
		}
	case "score", "noul":
		// No criteria/options expected; flag an options/criteria that looks copied
		// from a choice question (a likely authoring mistake), but do not hard-fail
		// a stray key the runtime ignores — the documented shape is just {"type":…}.
		if _, hasOptions := q["options"]; hasOptions {
			return "question " + id + ": " + typ + " must not carry \"options\" (only choice has criteria)"
		}
	default:
		return "question " + id + ": unknown type " + strconv.Quote(typ) + " (want choice, score or noul)"
	}
	return ""
}

// datasetCase is one golden-set line / request body: we only need questions.
type datasetCase struct {
	Questions map[string]map[string]json.RawMessage `json:"questions"`
}

// validateDatasetJSON validates every question in one JSON object that carries a
// "questions" key. Returns a slice of problems (empty when all valid).
func validateDatasetJSON(obj []byte) []string {
	var c datasetCase
	if err := json.Unmarshal(obj, &c); err != nil {
		// Not a well-formed case object; skip (the extractor only passes objects
		// that already parsed as JSON with a "questions" key).
		return nil
	}
	var problems []string
	for id, q := range c.Questions {
		if why := validateQuestion(id, q); why != "" {
			problems = append(problems, why)
		}
	}
	return problems
}

// repoRoot returns the repository root relative to this test's package dir
// (internal/engine/ollaya -> ../../..).
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

// extractDatasetObjects pulls every JSON object that has a "questions" key out of
// text: both standalone JSONL lines and pretty-printed/`--data-binary` blobs. It
// does a brace-matched scan so a multi-line request body (quickstart curl) is
// found as one object. Returns (object, 1-based line number).
func extractDatasetObjects(text string) []datasetHit {
	var hits []datasetHit
	b := []byte(text)
	for i := 0; i < len(b); i++ {
		if b[i] != '{' {
			continue
		}
		depth, inStr, esc := 0, false, false
		j := i
		for ; j < len(b); j++ {
			ch := b[j]
			switch {
			case esc:
				esc = false
			case ch == '\\' && inStr:
				esc = true
			case ch == '"':
				inStr = !inStr
			case inStr:
				// skip
			case ch == '{':
				depth++
			case ch == '}':
				depth--
				if depth == 0 {
					goto done
				}
			}
		}
	done:
		if depth != 0 || j >= len(b) {
			continue // unbalanced from here; move on
		}
		obj := b[i : j+1]
		if json.Valid(obj) && strings.Contains(string(obj), "\"questions\"") {
			var probe map[string]json.RawMessage
			if json.Unmarshal(obj, &probe) == nil {
				if _, ok := probe["questions"]; ok {
					hits = append(hits, datasetHit{obj: obj, line: 1 + strings.Count(text[:i], "\n")})
					i = j // skip past this object
					continue
				}
			}
		}
	}
	return hits
}

type datasetHit struct {
	obj  []byte
	line int
}

func TestShippedDatasetsMatchRuntimeSchema(t *testing.T) {
	root := repoRoot(t)
	var files []string
	for _, pat := range []string{
		filepath.Join(root, "config", "samples", "*.yaml"),
		filepath.Join(root, "docs", "*.md"),
	} {
		matches, err := filepath.Glob(pat)
		if err != nil {
			t.Fatalf("glob %s: %v", pat, err)
		}
		files = append(files, matches...)
	}
	if len(files) == 0 {
		t.Fatal("no sample/doc files found; check the repo-root path")
	}

	totalCases := 0
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		rel, _ := filepath.Rel(root, f)
		for _, hit := range extractDatasetObjects(string(data)) {
			totalCases++
			for _, why := range validateDatasetJSON(hit.obj) {
				t.Errorf("%s:%d: %s", rel, hit.line, why)
			}
		}
	}
	if totalCases == 0 {
		t.Error("found no dataset objects (a JSON object with a \"questions\" key) in config/samples or docs; the extractor or the datasets changed")
	}
	t.Logf("validated %d dataset case objects across %d files", totalCases, len(files))
}
