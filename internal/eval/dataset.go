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
	"fmt"
	"sort"
	"strconv"
)

// ValidateQuestions checks the `questions` object of one golden-dataset case
// against the question shapes the Ollaya runtime accepts on /v1/systemone, so a
// malformed dataset is rejected when it is parsed (surfaced as DatasetInvalid)
// instead of as a runtime 422 that parks the evaluation in a DatasetInvalid hold
// until the Evaluating timeout.
//
// questions is the raw JSON of the case's `questions` field: an object keyed by
// question id. An empty/absent questions object is allowed here (the dataset
// parser enforces that a case has `expected` answers; a case with no questions is
// not this function's concern). Shapes (source: docs/evaluation.md):
//
//   - "choice": requires a non-empty "criteria" object (label -> description).
//     "options" is NOT accepted by the runtime; it is reported with a rename hint.
//   - "score":  {"type":"score"} with no criteria/options.
//   - "noul":   {"type":"noul"} (a bool/probability question) with no
//     criteria/options.
//
// Any other type is rejected (the engine maps only choice/score/noul). The error
// names the offending question id; the dataset parser adds the case line number.
func ValidateQuestions(questions json.RawMessage) error {
	if len(questions) == 0 {
		return nil
	}
	var qs map[string]map[string]json.RawMessage
	if err := json.Unmarshal(questions, &qs); err != nil {
		return fmt.Errorf("questions is not a JSON object: %w", err)
	}
	// Validate in a stable id order so the first error is deterministic.
	ids := make([]string, 0, len(qs))
	for id := range qs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := validateQuestion(id, qs[id]); err != nil {
			return err
		}
	}
	return nil
}

// validateQuestion checks one question object against the documented shape.
func validateQuestion(id string, q map[string]json.RawMessage) error {
	rawType, ok := q["type"]
	if !ok {
		return fmt.Errorf("question %q: missing \"type\"", id)
	}
	var typ string
	if err := json.Unmarshal(rawType, &typ); err != nil {
		return fmt.Errorf("question %q: \"type\" must be a string", id)
	}
	switch typ {
	case "choice":
		if _, hasOptions := q["options"]; hasOptions {
			if _, hasCriteria := q["criteria"]; !hasCriteria {
				return fmt.Errorf("question %q: choice uses \"options\" but the runtime requires "+
					"\"criteria\" (a label->description map); rename options -> criteria", id)
			}
		}
		crit, ok := q["criteria"]
		if !ok {
			return fmt.Errorf("question %q: choice requires a non-empty \"criteria\" object", id)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(crit, &m); err != nil {
			return fmt.Errorf("question %q: choice \"criteria\" must be a JSON object (label->description)", id)
		}
		if len(m) == 0 {
			return fmt.Errorf("question %q: choice \"criteria\" must be non-empty", id)
		}
	case "score", "noul":
		// {"type":…} only; a stray "options" is a likely copy from a choice question.
		if _, hasOptions := q["options"]; hasOptions {
			return fmt.Errorf("question %q: %s must not carry \"options\" (only choice has criteria)", id, typ)
		}
	default:
		return fmt.Errorf("question %q: unknown type %s (want choice, score or noul)", id, strconv.Quote(typ))
	}
	return nil
}
