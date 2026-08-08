package lsm

import "testing"

func TestNewCompactionPlanReturnsNilForFewerThanTwoTables(t *testing.T) {
	tests := []struct {
		name     string
		manifest *Manifest
	}{
		{
			name: "empty manifest",
			manifest: &Manifest{
				Version: 1,
				Epoch:   1,
			},
		},
		{
			name: "one table",
			manifest: &Manifest{
				Version: 1,
				Epoch:   1,
				Tables: []ManifestTable{
					{ID: 1, File: "000001.sst"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := newCompactionPlan(test.manifest)
			if err != nil {
				t.Fatalf("newCompactionPlan: %v", err)
			}
			if plan != nil {
				t.Fatalf("expected no plan, got %+v", plan)
			}
		})
	}
}

func TestNewCompactionPlanSelectsTwoOldestTables(t *testing.T) {
	manifest := &Manifest{
		Version: 1,
		Epoch:   7,
		Tables: []ManifestTable{
			{ID: 4, File: "000004.sst"}, // newest
			{ID: 3, File: "000003.sst"},
			{ID: 2, File: "000002.sst"},
			{ID: 1, File: "000001.sst"}, // oldest
		},
	}

	plan, err := newCompactionPlan(manifest)
	if err != nil {
		t.Fatalf("newCompactionPlan: %v", err)
	}

	if plan == nil {
		t.Fatal("expected a compaction plan, got nil")
	}

	if len(plan.Inputs) != 2 {
		t.Fatalf("expected 2 input tables, got %d", len(plan.Inputs))
	}

	if plan.Inputs[0].ID != 2 || plan.Inputs[1].ID != 1 {
		t.Fatalf(
			"expected inputs table IDs [2 1], got [%d %d]",
			plan.Inputs[0].ID,
			plan.Inputs[1].ID,
		)
	}

	if plan.OutputID != 5 {
		t.Fatalf("expected output ID 5, got %d", plan.OutputID)
	}

	if plan.OutputFile != "000005.sst" {
		t.Fatalf("expected output file 000005.sst, got %q", plan.OutputFile)
	}
}

func TestNewCompactionPlanRejectsNilManifest(t *testing.T) {
	_, err := newCompactionPlan(nil)
	if err == nil {
		t.Fatal("expected an error for a nil manifest")
	}
}
