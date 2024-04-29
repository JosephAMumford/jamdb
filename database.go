package jamdb

type Database struct {
	Tables map[string]Table
}

type Table struct {
	Name        string
	columns     []Column
	rows        [][]any
	columnIndex map[string]int
}

type Column struct {
	Name string
	Int  int
}

func NewDatabase() *Database {
	return &Database{
		Tables: make(map[string]Table),
	}
}

func (db *Database) CreateTable(name string, columns []Column) *Table {
	table := NewTable(name, columns)
	db.Tables[table.Name] = table
	return &table
}

func NewTable(name string, columns []Column) Table {
	table := Table{
		Name:    name,
		columns: columns,
	}
	table.columnIndex = make(map[string]int, len(columns))
	for i, v := range columns {
		table.columnIndex[v.Name] = i
	}

	return table
}

const (
	ColumnInt = 1 << iota
	ColumnVarchar
)

const (
	ConditionEq = "="
	ConditionNe = "!="
)

type Condition struct {
	Column string
	Eq     string
	Value  any
}

type Expression struct {
	Column string
}

func (t *Table) Insert(data ...any) {
	t.rows = append(t.rows, data)
}

func (t *Table) Select(cols []Expression, cond []Condition) [][]any {
	res := [][]any{}
	for i := 0; i < len(t.rows); i++ {
		if t.rowMatch(cond, i) {
			res = append(res, t.filterColumns(cols, i))
		}
	}
	return res
}

type Set struct {
	Column string
	Value  any
}

func (c Condition) Eval(val any) bool {
	if c.Eq == ConditionEq {
		return c.Value == val
	} else if c.Eq == ConditionNe {
		return c.Value != val
	}
	return false
}

func (t *Table) rowMatch(cond []Condition, i int) bool {
	// Skip deleted row
	if t.rows[i] == nil {
		return false
	}

	if cond == nil {
		return true
	}

	for _, c := range cond {
		j := t.getColumnIndex(c.Column)
		if !c.Eval(t.rows[i][j]) {
			return false
		}
	}
	return true
}

func (t *Table) Update(set []Set, cond []Condition) {
	for i := 0; i < len(t.rows); i++ {
		if t.rowMatch(cond, i) {
			for _, s := range set {
				t.rows[i][t.getColumnIndex(s.Column)] = s.Value
			}
		}
	}
}

func (t *Table) Delete(cond []Condition) {
	for i := 0; i < len(t.rows); i++ {
		if t.rowMatch(cond, i) {
			t.rows[i] = nil
		}
	}
}

func (t *Table) filterColumns(cols []Expression, i int) []any {
	row := t.rows[i]

	if cols[0].Column == "*" {
		return append([]any(nil), row...)
	}

	res := make([]any, len(cols))
	for i, c := range cols {
		res[i] = row[t.getColumnIndex(c.Column)]
	}
	return res
}

func (t *Table) getColumnIndex(name string) int {
	return t.columnIndex[name]
}