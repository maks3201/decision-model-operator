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
	"strings"
	"testing"

	"github.com/maks3201/decision-model-operator/internal/eval"
)

// This guard catches the class of bug where a shipped golden dataset uses a
// question shape the Ollaya runtime rejects (e.g. choice with "options" instead
// of "criteria"), which otherwise only surfaces at runtime as a 422 that parks
// the evaluation in DatasetInvalid. It scans every embedded dataset in
// config/samples/*.yaml and docs/*.md (any JSON object carrying a "questions"
// key) and validates each case with eval.ValidateQuestions — the SAME validator
// the controller runs at dataset-parse time (internal/eval/dataset.go), so this
// test and production share one source of truth for the documented shapes
// (docs/evaluation.md): choice needs a non-empty "criteria" map; score/noul carry
// no criteria/options; any other type is rejected.

// datasetHit is one extracted dataset object and its 1-based source line.
type datasetHit struct {
	obj  []byte
	line int
}

// extractDatasetObjects pulls every JSON object that has a "questions" key out of
// text: both standalone JSONL lines and pretty-printed / `--data-binary` blobs. It
// brace-matches with string/escape awareness so a multi-line request body (the
// quickstart curl) is captured as one object and a "}" inside a string does not
// truncate it.
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
				// inside a string: ignore structural chars
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
			continue
		}
		obj := b[i : j+1]
		if json.Valid(obj) && strings.Contains(string(obj), "\"questions\"") {
			var probe map[string]json.RawMessage
			if json.Unmarshal(obj, &probe) == nil {
				if _, ok := probe["questions"]; ok {
					hits = append(hits, datasetHit{obj: obj, line: 1 + strings.Count(text[:i], "\n")})
					i = j
					continue
				}
			}
		}
	}
	return hits
}

// questionsOf returns the raw `questions` field of a dataset object.
func questionsOf(obj []byte) json.RawMessage {
	var c struct {
		Questions json.RawMessage `json:"questions"`
	}
	_ = json.Unmarshal(obj, &c)
	return c.Questions
}

func TestShippedDatasetsMatchRuntimeSchema(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	var files []string
	for _, pat := range []string{
		filepath.Join(root, "config", "samples", "*.yaml"),
		filepath.Join(root, "docs", "*.md"),
	} {
		matches, gerr := filepath.Glob(pat)
		if gerr != nil {
			t.Fatalf("glob %s: %v", pat, gerr)
		}
		files = append(files, matches...)
	}
	if len(files) == 0 {
		t.Fatal("no sample/doc files found; check the repo-root path")
	}

	totalCases := 0
	for _, f := range files {
		data, rerr := os.ReadFile(f)
		if rerr != nil {
			t.Fatalf("read %s: %v", f, rerr)
		}
		rel, _ := filepath.Rel(root, f)
		for _, hit := range extractDatasetObjects(string(data)) {
			totalCases++
			if verr := eval.ValidateQuestions(questionsOf(hit.obj)); verr != nil {
				t.Errorf("%s:%d: %v", rel, hit.line, verr)
			}
		}
	}
	if totalCases == 0 {
		t.Error("found no dataset objects (a JSON object with a \"questions\" key) in config/samples or docs")
	}
	t.Logf("validated %d dataset case objects across %d files", totalCases, len(files))
}
