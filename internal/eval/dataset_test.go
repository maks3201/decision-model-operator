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

package eval

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateQuestions(t *testing.T) {
	tests := []struct {
		name       string
		questions  string
		wantErr    bool
		wantSubstr string // substring the error must contain (id / hint)
	}{
		{"valid choice", `{"q":{"type":"choice","criteria":{"a":"desc a","b":"desc b"}}}`, false, ""},
		{"valid score", `{"sev":{"type":"score"}}`, false, ""},
		{"valid noul", `{"urgent":{"type":"noul"}}`, false, ""},
		{"valid mixed", `{"q1":{"type":"choice","criteria":{"a":"x"}},"q2":{"type":"score"},"q3":{"type":"noul"}}`, false, ""},
		{"empty questions object", `{}`, false, ""},
		{"absent questions (null)", `null`, false, ""},

		{"choice with options not criteria", `{"q1":{"type":"choice","options":["a","b"]}}`, true, "rename options -> criteria"},
		{"choice missing criteria", `{"q1":{"type":"choice"}}`, true, "requires a non-empty \"criteria\""},
		{"choice empty criteria", `{"q1":{"type":"choice","criteria":{}}}`, true, "must be non-empty"},
		{"choice criteria not an object", `{"q1":{"type":"choice","criteria":["a","b"]}}`, true, "must be a JSON object"},
		{"missing type", `{"q1":{"criteria":{"a":"x"}}}`, true, "missing \"type\""},
		{"type not a string", `{"q1":{"type":123}}`, true, "must be a string"},
		{"unknown type", `{"q1":{"type":"freeform"}}`, true, "unknown type"},
		{"score with options", `{"q1":{"type":"score","options":["1","2"]}}`, true, "must not carry \"options\""},
		{"noul with options", `{"q1":{"type":"noul","options":["t","f"]}}`, true, "must not carry \"options\""},
		{"questions not an object", `["q1"]`, true, "not a JSON object"},
		{"error names the question id", `{"dept":{"type":"choice","options":["a"]}}`, true, `"dept"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateQuestions(json.RawMessage(tt.questions))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ValidateQuestions(%s) = nil, want error", tt.questions)
				}
				if tt.wantSubstr != "" && !strings.Contains(err.Error(), tt.wantSubstr) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantSubstr)
				}
			} else if err != nil {
				t.Errorf("ValidateQuestions(%s) = %v, want nil", tt.questions, err)
			}
		})
	}
}

// The first error is deterministic (questions validated in sorted id order), so a
// case with two bad questions always reports the lexicographically-first id.
func TestValidateQuestionsDeterministicOrder(t *testing.T) {
	q := `{"zeta":{"type":"choice","options":["a"]},"alpha":{"type":"freeform"}}`
	err := ValidateQuestions(json.RawMessage(q))
	if err == nil || !strings.Contains(err.Error(), `"alpha"`) {
		t.Fatalf("want the error to name the first id \"alpha\", got %v", err)
	}
}
