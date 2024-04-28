package jamdb

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDatabase(t *testing.T) {
	db := NewDatabase()

	tab := db.CreateTable("users", []Column{
		{"id", ColumnInt},
		{"name", ColumnVarchar},
		{"age", ColumnInt},
	})

	tab.Insert(1, "Juros", 32)
	tab.Insert(2, "Meladi", 45)
	tab.Insert(3, "Kolinsa", 26)

	if diff := cmp.Diff(len(db.tables), 1); diff != "" {
		t.Errorf("Select: (-want +got)\n%s", diff)
	}

	var got, want [][]any

	// SELECT * FROM users;
	got = tab.Select([]Expression{{"*"}}, nil)
	want = [][]any{{1, "Juros", 32}, {2, "Meladi", 45}, {3, "Kolinsa", 26}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Select: (-want +got)\n%s", diff)
	}

	// SELECT age FROM users WHERE name = "Kolinsa";
	got = tab.Select(
		[]Expression{{"age"}},
		[]Condition{{"name", ConditionEq, "Kolinsa"}},
	)
	want = [][]any{{26}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Select: (-want +got)\n%s", diff)
	}

	// SELECT * FROM use WHERE name != "Juros" AND age = 26;
	got = tab.Select(
		[]Expression{{"*"}},
		[]Condition{{"name", ConditionNe, "Juros"}, {"age", ConditionEq, 26}},
	)
	want = [][]any{{3, "Kolinsa", 26}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Select: (-want +got)\n%s", diff)
	}

	// UPDATE users SET age = 25 WHERE name = "Kolinsa";
	tab.Update(
		[]Set{{"age", 25}},
		[]Condition{{"name", ConditionEq, "Kolinsa"}},
	)

	got = tab.Select([]Expression{{"*"}}, nil)
	want = [][]any{{1, "Juros", 32}, {2, "Meladi", 45}, {3, "Kolinsa", 25}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Select: (-want +got)\n%s", diff)
	}

	// DELETE FROM users WHERE age != 32;
	tab.Delete([]Condition{{"age", ConditionNe, 32}})

	got = tab.Select([]Expression{{"*"}}, nil)
	want = [][]any{{1, "Juros", 32}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Select: (-want +got)\n%s", diff)
	}
}
