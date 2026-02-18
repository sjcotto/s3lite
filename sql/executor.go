// Package sql - executor.go executes parsed SQL statements against the engine.
package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/sjcotto/s3lite/engine"
	"github.com/sjcotto/s3lite/manifest"
	"github.com/sjcotto/s3lite/page"
)

// Result represents the result of executing a SQL statement.
type Result struct {
	Columns  []string
	Rows     [][]interface{}
	Message  string
	RowCount int
}

// Executor executes SQL statements.
type Executor struct {
	engine *engine.Engine
	inTx   bool // in explicit transaction
}

// NewExecutor creates a new executor.
func NewExecutor(e *engine.Engine) *Executor {
	return &Executor{engine: e}
}

// Execute runs a SQL statement and returns the result.
func (ex *Executor) Execute(ctx context.Context, sqlStr string) (*Result, error) {
	stmt, err := ParseSQL(sqlStr)
	if err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *CreateTableStmt:
		return ex.execCreate(ctx, s)
	case *DropTableStmt:
		return ex.execDrop(ctx, s)
	case *InsertStmt:
		return ex.execInsert(ctx, s)
	case *SelectStmt:
		return ex.execSelect(ctx, s)
	case *DeleteStmt:
		return ex.execDelete(ctx, s)
	case *UpdateStmt:
		return ex.execUpdate(ctx, s)
	case *CreateIndexStmt:
		return ex.execCreateIndex(ctx, s)
	case *DropIndexStmt:
		return ex.execDropIndex(ctx, s)
	case *BeginStmt:
		return ex.execBegin(ctx)
	case *CommitStmt:
		return ex.execCommitTx(ctx)
	case *RollbackStmt:
		return ex.execRollback(ctx)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (ex *Executor) execCreate(_ context.Context, stmt *CreateTableStmt) (*Result, error) {
	if stmt.IfNotExists && ex.engine.TableExists(stmt.Name) {
		return &Result{Message: fmt.Sprintf("Table %s already exists", stmt.Name)}, nil
	}

	var columns []manifest.ColumnMeta
	for _, col := range stmt.Columns {
		columns = append(columns, manifest.ColumnMeta{
			Name:     col.Name,
			Type:     col.Type,
			PK:       col.PK,
			Nullable: col.Nullable,
		})
	}

	ctx := context.Background()
	if err := ex.engine.CreateTable(ctx, stmt.Name, columns); err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("Table %s created", stmt.Name)}, nil
}

func (ex *Executor) execDrop(_ context.Context, stmt *DropTableStmt) (*Result, error) {
	if stmt.IfExists && !ex.engine.TableExists(stmt.Name) {
		return &Result{Message: fmt.Sprintf("Table %s does not exist", stmt.Name)}, nil
	}

	if err := ex.engine.DropTable(stmt.Name); err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("Table %s dropped", stmt.Name)}, nil
}

func (ex *Executor) execCreateIndex(ctx context.Context, stmt *CreateIndexStmt) (*Result, error) {
	if err := ex.engine.CreateIndex(ctx, stmt.Name, stmt.Table, stmt.Columns, stmt.Unique); err != nil {
		if stmt.IfNotExists {
			return &Result{Message: fmt.Sprintf("Index %s already exists", stmt.Name)}, nil
		}
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Index %s created on %s(%s)", stmt.Name, stmt.Table, strings.Join(stmt.Columns, ", "))}, nil
}

func (ex *Executor) execDropIndex(_ context.Context, stmt *DropIndexStmt) (*Result, error) {
	if err := ex.engine.DropIndex(stmt.Name); err != nil {
		if stmt.IfExists {
			return &Result{Message: fmt.Sprintf("Index %s does not exist", stmt.Name)}, nil
		}
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Index %s dropped", stmt.Name)}, nil
}

func (ex *Executor) execBegin(_ context.Context) (*Result, error) {
	if ex.inTx {
		return nil, fmt.Errorf("already in a transaction")
	}
	ex.engine.BeginTx()
	ex.inTx = true
	return &Result{Message: "BEGIN"}, nil
}

func (ex *Executor) execCommitTx(ctx context.Context) (*Result, error) {
	if !ex.inTx {
		// Just commit dirty pages (same as .commit)
		if err := ex.engine.Commit(ctx); err != nil {
			return nil, err
		}
		return &Result{Message: "COMMIT"}, nil
	}
	if err := ex.engine.CommitTx(ctx); err != nil {
		return nil, err
	}
	ex.inTx = false
	return &Result{Message: "COMMIT"}, nil
}

func (ex *Executor) execRollback(_ context.Context) (*Result, error) {
	if !ex.inTx {
		return nil, fmt.Errorf("not in a transaction")
	}
	ex.engine.RollbackTx()
	ex.inTx = false
	return &Result{Message: "ROLLBACK"}, nil
}

func (ex *Executor) execInsert(ctx context.Context, stmt *InsertStmt) (*Result, error) {
	meta, err := ex.engine.TableMeta(stmt.Table)
	if err != nil {
		return nil, err
	}

	// Determine column order
	columns := stmt.Columns
	if len(columns) == 0 {
		for _, col := range meta.Columns {
			columns = append(columns, col.Name)
		}
	}

	// Find PK column
	pkIdx := -1
	for i, col := range meta.Columns {
		if col.PK {
			pkIdx = i
			break
		}
	}
	if pkIdx == -1 {
		pkIdx = 0
	}

	pkColName := meta.Columns[pkIdx].Name

	// Find PK position in the insert columns
	pkInsertIdx := -1
	for i, c := range columns {
		if strings.EqualFold(c, pkColName) {
			pkInsertIdx = i
			break
		}
	}

	count := 0
	for _, values := range stmt.Values {
		if len(values) != len(columns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(columns), len(values))
		}

		// Build row map
		row := make(map[string]interface{})
		for i, col := range columns {
			val, err := evalLiteral(values[i])
			if err != nil {
				return nil, err
			}
			row[col] = val
		}

		// Serialize key
		var key []byte
		if pkInsertIdx >= 0 {
			key = serializeValue(row[pkColName])
		} else {
			key = serializeValue(row[columns[0]])
		}

		// Serialize value as JSON
		rowJSON, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}

		if err := ex.engine.Insert(ctx, stmt.Table, key, rowJSON); err != nil {
			return nil, err
		}

		// Update secondary indexes
		ex.engine.UpdateIndexes(ctx, stmt.Table, key, row)

		count++
	}

	return &Result{
		Message:  fmt.Sprintf("Inserted %d row(s)", count),
		RowCount: count,
	}, nil
}

func (ex *Executor) execSelect(ctx context.Context, stmt *SelectStmt) (*Result, error) {
	// Handle JOINs
	if len(stmt.Joins) > 0 {
		return ex.execSelectJoin(ctx, stmt)
	}

	meta, err := ex.engine.TableMeta(stmt.From)
	if err != nil {
		return nil, err
	}

	// Check if we can use an index for the WHERE clause
	cells, err := ex.fetchWithIndex(ctx, stmt)
	if err != nil {
		return nil, err
	}

	// Check for aggregate functions (with or without GROUP BY)
	hasAgg := hasAggregates(stmt)

	if hasAgg {
		return ex.execSelectAggregate(ctx, stmt, meta, cells)
	}

	// Determine output columns
	var outputCols []string
	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		for _, col := range meta.Columns {
			outputCols = append(outputCols, col.Name)
		}
	} else {
		for _, col := range stmt.Columns {
			if col.Alias != "" {
				outputCols = append(outputCols, col.Alias)
			} else if ref, ok := col.Expr.(*ColumnRef); ok {
				outputCols = append(outputCols, ref.Name)
			} else {
				outputCols = append(outputCols, col.Expr.String())
			}
		}
	}

	// Build result rows
	var rows [][]interface{}
	for _, cell := range cells {
		row, err := deserializeRow(cell.Value)
		if err != nil {
			continue
		}

		// Apply WHERE
		if stmt.Where != nil {
			match, err := evalExpr(stmt.Where, row)
			if err != nil {
				continue
			}
			if !isTruthy(match) {
				continue
			}
		}

		// Project columns
		var outRow []interface{}
		if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
			for _, col := range meta.Columns {
				outRow = append(outRow, row[col.Name])
			}
		} else {
			for _, col := range stmt.Columns {
				val, err := evalExpr(col.Expr, row)
				if err != nil {
					outRow = append(outRow, nil)
				} else {
					outRow = append(outRow, val)
				}
			}
		}
		rows = append(rows, outRow)
	}

	// DISTINCT
	if stmt.Distinct {
		rows = deduplicateRows(rows)
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		sortRows(rows, outputCols, stmt.OrderBy)
	}

	// OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(rows) {
		rows = rows[stmt.Offset:]
	} else if stmt.Offset >= len(rows) {
		rows = nil
	}

	// LIMIT
	if stmt.HasLimit && stmt.Limit < len(rows) {
		rows = rows[:stmt.Limit]
	}

	return &Result{
		Columns:  outputCols,
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// execSelectAggregate handles SELECT with aggregate functions and/or GROUP BY.
func (ex *Executor) execSelectAggregate(ctx context.Context, stmt *SelectStmt, meta *manifest.TableMeta, cells []page.LeafCell) (*Result, error) {
	_ = ctx

	// Collect matching rows
	var matchingRows []map[string]interface{}
	for _, cell := range cells {
		row, err := deserializeRow(cell.Value)
		if err != nil {
			continue
		}
		if stmt.Where != nil {
			match, err := evalExpr(stmt.Where, row)
			if err != nil || !isTruthy(match) {
				continue
			}
		}
		matchingRows = append(matchingRows, row)
	}

	// Group rows
	type group struct {
		key  string
		vals map[string]interface{} // representative row for group key values
		rows []map[string]interface{}
	}

	var groups []*group
	groupIndex := make(map[string]*group) // groupKey -> group

	if len(stmt.GroupBy) > 0 {
		for _, row := range matchingRows {
			gk := groupKey(row, stmt.GroupBy)
			g, ok := groupIndex[gk]
			if !ok {
				g = &group{key: gk, vals: row}
				groupIndex[gk] = g
				groups = append(groups, g)
			}
			g.rows = append(g.rows, row)
		}
	} else {
		// No GROUP BY — all rows are one group
		g := &group{key: "", rows: matchingRows}
		if len(matchingRows) > 0 {
			g.vals = matchingRows[0]
		} else {
			g.vals = make(map[string]interface{})
		}
		groups = []*group{g}
	}

	// Apply HAVING
	if stmt.Having != nil {
		var filtered []*group
		for _, g := range groups {
			val, err := evalAggExpr(stmt.Having, g.rows, g.vals)
			if err != nil {
				continue
			}
			if isTruthy(val) {
				filtered = append(filtered, g)
			}
		}
		groups = filtered
	}

	// Compute output columns
	var outputCols []string
	for _, col := range stmt.Columns {
		if col.Alias != "" {
			outputCols = append(outputCols, col.Alias)
		} else if ref, ok := col.Expr.(*ColumnRef); ok {
			outputCols = append(outputCols, ref.Name)
		} else if fn, ok := col.Expr.(*FuncExpr); ok {
			outputCols = append(outputCols, fn.String())
		} else {
			outputCols = append(outputCols, col.Expr.String())
		}
	}

	// Compute result rows
	var rows [][]interface{}
	for _, g := range groups {
		var outRow []interface{}
		for _, col := range stmt.Columns {
			val, err := evalAggExpr(col.Expr, g.rows, g.vals)
			if err != nil {
				outRow = append(outRow, nil)
			} else {
				outRow = append(outRow, val)
			}
		}
		rows = append(rows, outRow)
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		sortRows(rows, outputCols, stmt.OrderBy)
	}

	// OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(rows) {
		rows = rows[stmt.Offset:]
	} else if stmt.Offset >= len(rows) {
		rows = nil
	}

	// LIMIT
	if stmt.HasLimit && stmt.Limit < len(rows) {
		rows = rows[:stmt.Limit]
	}

	_ = meta

	return &Result{
		Columns:  outputCols,
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// execSelectJoin handles SELECT with JOINs.
func (ex *Executor) execSelectJoin(ctx context.Context, stmt *SelectStmt) (*Result, error) {
	// Load left table
	leftCells, err := ex.engine.Scan(ctx, stmt.From)
	if err != nil {
		return nil, err
	}

	leftAlias := stmt.From

	// Convert to row maps with table prefix
	var leftRows []map[string]interface{}
	for _, cell := range leftCells {
		row, err := deserializeRow(cell.Value)
		if err != nil {
			continue
		}
		// Add both unqualified and qualified keys
		qualified := make(map[string]interface{})
		for k, v := range row {
			qualified[k] = v
			qualified[leftAlias+"."+k] = v
		}
		leftRows = append(leftRows, qualified)
	}

	// Process each JOIN
	currentRows := leftRows
	for _, join := range stmt.Joins {
		rightCells, err := ex.engine.Scan(ctx, join.Table)
		if err != nil {
			return nil, err
		}

		rightAlias := join.Table
		if join.Alias != "" {
			rightAlias = join.Alias
		}

		var rightRows []map[string]interface{}
		for _, cell := range rightCells {
			row, err := deserializeRow(cell.Value)
			if err != nil {
				continue
			}
			qualified := make(map[string]interface{})
			for k, v := range row {
				qualified[k] = v
				qualified[rightAlias+"."+k] = v
			}
			rightRows = append(rightRows, qualified)
		}

		// Nested loop join
		var joined []map[string]interface{}
		switch join.Type {
		case "INNER":
			for _, lr := range currentRows {
				for _, rr := range rightRows {
					merged := mergeRows(lr, rr)
					if join.On != nil {
						match, err := evalExpr(join.On, merged)
						if err != nil || !isTruthy(match) {
							continue
						}
					}
					joined = append(joined, merged)
				}
			}
		case "LEFT":
			for _, lr := range currentRows {
				matched := false
				for _, rr := range rightRows {
					merged := mergeRows(lr, rr)
					if join.On != nil {
						match, err := evalExpr(join.On, merged)
						if err != nil || !isTruthy(match) {
							continue
						}
					}
					joined = append(joined, merged)
					matched = true
				}
				if !matched {
					// Add left row with NULLs for right columns
					joined = append(joined, lr)
				}
			}
		case "RIGHT":
			for _, rr := range rightRows {
				matched := false
				for _, lr := range currentRows {
					merged := mergeRows(lr, rr)
					if join.On != nil {
						match, err := evalExpr(join.On, merged)
						if err != nil || !isTruthy(match) {
							continue
						}
					}
					joined = append(joined, merged)
					matched = true
				}
				if !matched {
					joined = append(joined, rr)
				}
			}
		}
		currentRows = joined
	}

	// Apply WHERE
	if stmt.Where != nil {
		var filtered []map[string]interface{}
		for _, row := range currentRows {
			match, err := evalExpr(stmt.Where, row)
			if err != nil || !isTruthy(match) {
				continue
			}
			filtered = append(filtered, row)
		}
		currentRows = filtered
	}

	// Handle aggregates on joined rows
	if hasAggregates(stmt) {
		return ex.execJoinAggregate(ctx, stmt, currentRows)
	}

	// Determine output columns
	var outputCols []string
	isStar := len(stmt.Columns) == 1 && stmt.Columns[0].Star
	if isStar {
		// Collect all unique non-qualified column names
		seen := make(map[string]bool)
		for _, row := range currentRows {
			for k := range row {
				if !strings.Contains(k, ".") && !seen[k] {
					seen[k] = true
					outputCols = append(outputCols, k)
				}
			}
			break
		}
		sort.Strings(outputCols)
	} else {
		for _, col := range stmt.Columns {
			if col.Alias != "" {
				outputCols = append(outputCols, col.Alias)
			} else if ref, ok := col.Expr.(*ColumnRef); ok {
				if ref.Table != "" {
					outputCols = append(outputCols, ref.Table+"."+ref.Name)
				} else {
					outputCols = append(outputCols, ref.Name)
				}
			} else {
				outputCols = append(outputCols, col.Expr.String())
			}
		}
	}

	// Project
	var rows [][]interface{}
	for _, row := range currentRows {
		var outRow []interface{}
		if isStar {
			for _, col := range outputCols {
				outRow = append(outRow, row[col])
			}
		} else {
			for _, col := range stmt.Columns {
				val, err := evalExpr(col.Expr, row)
				if err != nil {
					outRow = append(outRow, nil)
				} else {
					outRow = append(outRow, val)
				}
			}
		}
		rows = append(rows, outRow)
	}

	// DISTINCT
	if stmt.Distinct {
		rows = deduplicateRows(rows)
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		sortRows(rows, outputCols, stmt.OrderBy)
	}

	// OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(rows) {
		rows = rows[stmt.Offset:]
	} else if stmt.Offset >= len(rows) {
		rows = nil
	}

	// LIMIT
	if stmt.HasLimit && stmt.Limit < len(rows) {
		rows = rows[:stmt.Limit]
	}

	return &Result{
		Columns:  outputCols,
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// execJoinAggregate handles GROUP BY + aggregates on joined row sets.
func (ex *Executor) execJoinAggregate(_ context.Context, stmt *SelectStmt, rows []map[string]interface{}) (*Result, error) {
	type group struct {
		key  string
		vals map[string]interface{}
		rows []map[string]interface{}
	}

	var groups []*group
	groupIndex := make(map[string]*group)

	if len(stmt.GroupBy) > 0 {
		for _, row := range rows {
			gk := groupKeyJoin(row, stmt.GroupBy)
			g, ok := groupIndex[gk]
			if !ok {
				g = &group{key: gk, vals: row}
				groupIndex[gk] = g
				groups = append(groups, g)
			}
			g.rows = append(g.rows, row)
		}
	} else {
		g := &group{key: "", rows: rows}
		if len(rows) > 0 {
			g.vals = rows[0]
		} else {
			g.vals = make(map[string]interface{})
		}
		groups = []*group{g}
	}

	// HAVING
	if stmt.Having != nil {
		var filtered []*group
		for _, g := range groups {
			val, err := evalAggExpr(stmt.Having, g.rows, g.vals)
			if err != nil {
				continue
			}
			if isTruthy(val) {
				filtered = append(filtered, g)
			}
		}
		groups = filtered
	}

	// Compute output columns
	var outputCols []string
	for _, col := range stmt.Columns {
		if col.Alias != "" {
			outputCols = append(outputCols, col.Alias)
		} else if ref, ok := col.Expr.(*ColumnRef); ok {
			if ref.Table != "" {
				outputCols = append(outputCols, ref.Table+"."+ref.Name)
			} else {
				outputCols = append(outputCols, ref.Name)
			}
		} else if fn, ok := col.Expr.(*FuncExpr); ok {
			outputCols = append(outputCols, fn.String())
		} else {
			outputCols = append(outputCols, col.Expr.String())
		}
	}

	// Compute result rows
	var resultRows [][]interface{}
	for _, g := range groups {
		var outRow []interface{}
		for _, col := range stmt.Columns {
			val, err := evalAggExprJoin(col.Expr, g.rows, g.vals)
			if err != nil {
				outRow = append(outRow, nil)
			} else {
				outRow = append(outRow, val)
			}
		}
		resultRows = append(resultRows, outRow)
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		sortRows(resultRows, outputCols, stmt.OrderBy)
	}

	// OFFSET / LIMIT
	if stmt.Offset > 0 && stmt.Offset < len(resultRows) {
		resultRows = resultRows[stmt.Offset:]
	} else if stmt.Offset >= len(resultRows) {
		resultRows = nil
	}
	if stmt.HasLimit && stmt.Limit < len(resultRows) {
		resultRows = resultRows[:stmt.Limit]
	}

	return &Result{
		Columns:  outputCols,
		Rows:     resultRows,
		RowCount: len(resultRows),
	}, nil
}

// fetchWithIndex tries to use a secondary index to narrow the scan.
func (ex *Executor) fetchWithIndex(ctx context.Context, stmt *SelectStmt) ([]page.LeafCell, error) {
	if stmt.Where != nil {
		// Try to find a usable index for simple equality: col = val
		if be, ok := stmt.Where.(*BinaryExpr); ok && be.Op == "=" {
			if ref, ok := be.Left.(*ColumnRef); ok {
				if lit, ok := be.Right.(*LiteralExpr); ok {
					indexName := ex.engine.FindIndex(stmt.From, ref.Name)
					if indexName != "" {
						key := serializeValue(lit.Value)
						cells, err := ex.engine.ScanIndex(ctx, indexName, key, key)
						if err == nil {
							return cells, nil
						}
						// Fall through to full scan
					}
				}
			}
		}
	}

	// Full scan
	return ex.engine.Scan(ctx, stmt.From)
}

func (ex *Executor) execDelete(ctx context.Context, stmt *DeleteStmt) (*Result, error) {
	cells, err := ex.engine.Scan(ctx, stmt.Table)
	if err != nil {
		return nil, err
	}

	count := 0
	for _, cell := range cells {
		if stmt.Where != nil {
			row, err := deserializeRow(cell.Value)
			if err != nil {
				continue
			}
			match, err := evalExpr(stmt.Where, row)
			if err != nil || !isTruthy(match) {
				continue
			}
		}

		if err := ex.engine.Delete(ctx, stmt.Table, cell.Key); err != nil {
			return nil, err
		}
		count++
	}

	return &Result{
		Message:  fmt.Sprintf("Deleted %d row(s)", count),
		RowCount: count,
	}, nil
}

func (ex *Executor) execUpdate(ctx context.Context, stmt *UpdateStmt) (*Result, error) {
	cells, err := ex.engine.Scan(ctx, stmt.Table)
	if err != nil {
		return nil, err
	}

	count := 0
	for _, cell := range cells {
		row, err := deserializeRow(cell.Value)
		if err != nil {
			continue
		}

		if stmt.Where != nil {
			match, err := evalExpr(stmt.Where, row)
			if err != nil || !isTruthy(match) {
				continue
			}
		}

		// Apply SET clauses
		for _, sc := range stmt.Set {
			val, err := evalExpr(sc.Value, row)
			if err != nil {
				return nil, err
			}
			row[sc.Column] = val
		}

		// Serialize and reinsert
		rowJSON, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}

		if err := ex.engine.Insert(ctx, stmt.Table, cell.Key, rowJSON); err != nil {
			return nil, err
		}
		count++
	}

	return &Result{
		Message:  fmt.Sprintf("Updated %d row(s)", count),
		RowCount: count,
	}, nil
}

// --- Aggregate helpers ---

func hasAggregates(stmt *SelectStmt) bool {
	if len(stmt.GroupBy) > 0 {
		return true
	}
	for _, col := range stmt.Columns {
		if containsAggregate(col.Expr) {
			return true
		}
	}
	return false
}

func containsAggregate(expr Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *FuncExpr:
		switch e.Name {
		case "count", "sum", "avg", "min", "max":
			return true
		}
	case *BinaryExpr:
		return containsAggregate(e.Left) || containsAggregate(e.Right)
	case *UnaryExpr:
		return containsAggregate(e.Expr)
	}
	return false
}

func groupKeyJoin(row map[string]interface{}, groupBy []string) string {
	var parts []string
	for _, col := range groupBy {
		val := lookupColumn(row, col)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "\x00")
}

// evalAggExprJoin is like evalAggExpr but handles qualified column refs in join context.
func evalAggExprJoin(expr Expr, groupRows []map[string]interface{}, representative map[string]interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *FuncExpr:
		return computeAggregateJoin(e, groupRows)
	case *ColumnRef:
		name := e.Name
		if e.Table != "" {
			name = e.Table + "." + e.Name
		}
		return lookupColumn(representative, name), nil
	case *LiteralExpr:
		return e.Value, nil
	case *BinaryExpr:
		left, err := evalAggExprJoin(e.Left, groupRows, representative)
		if err != nil {
			return nil, err
		}
		right, err := evalAggExprJoin(e.Right, groupRows, representative)
		if err != nil {
			return nil, err
		}
		return evalBinaryOp(e.Op, left, right)
	case *UnaryExpr:
		val, err := evalAggExprJoin(e.Expr, groupRows, representative)
		if err != nil {
			return nil, err
		}
		switch e.Op {
		case "NOT":
			return !isTruthy(val), nil
		case "-":
			return negateValue(val)
		}
	}
	return nil, fmt.Errorf("cannot evaluate join aggregate expression: %T", expr)
}

func computeAggregateJoin(fn *FuncExpr, rows []map[string]interface{}) (interface{}, error) {
	return computeAggregate(fn, rows)
}

func groupKey(row map[string]interface{}, groupBy []string) string {
	var parts []string
	for _, col := range groupBy {
		val := lookupColumn(row, col)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "\x00")
}

// evalAggExpr evaluates an expression that may contain aggregate functions.
func evalAggExpr(expr Expr, groupRows []map[string]interface{}, representative map[string]interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *FuncExpr:
		return computeAggregate(e, groupRows)
	case *ColumnRef:
		return lookupColumn(representative, e.Name), nil
	case *LiteralExpr:
		return e.Value, nil
	case *BinaryExpr:
		left, err := evalAggExpr(e.Left, groupRows, representative)
		if err != nil {
			return nil, err
		}
		right, err := evalAggExpr(e.Right, groupRows, representative)
		if err != nil {
			return nil, err
		}
		return evalBinaryOp(e.Op, left, right)
	case *UnaryExpr:
		val, err := evalAggExpr(e.Expr, groupRows, representative)
		if err != nil {
			return nil, err
		}
		switch e.Op {
		case "NOT":
			return !isTruthy(val), nil
		case "-":
			return negateValue(val)
		}
	}
	return nil, fmt.Errorf("cannot evaluate aggregate expression: %T", expr)
}

func computeAggregate(fn *FuncExpr, rows []map[string]interface{}) (interface{}, error) {
	switch fn.Name {
	case "count":
		if fn.Star {
			return int64(len(rows)), nil
		}
		if len(fn.Args) == 0 {
			return int64(len(rows)), nil
		}
		count := int64(0)
		if fn.Distinct {
			seen := make(map[string]bool)
			for _, row := range rows {
				val, err := evalExpr(fn.Args[0], row)
				if err != nil || val == nil {
					continue
				}
				key := fmt.Sprintf("%v", val)
				if !seen[key] {
					seen[key] = true
					count++
				}
			}
		} else {
			for _, row := range rows {
				val, err := evalExpr(fn.Args[0], row)
				if err != nil || val == nil {
					continue
				}
				count++
			}
		}
		return count, nil

	case "sum":
		if len(fn.Args) == 0 {
			return nil, fmt.Errorf("SUM requires an argument")
		}
		sum := 0.0
		hasValue := false
		for _, row := range rows {
			val, err := evalExpr(fn.Args[0], row)
			if err != nil || val == nil {
				continue
			}
			if f, ok := toNumeric(val); ok {
				sum += f
				hasValue = true
			}
		}
		if !hasValue {
			return nil, nil
		}
		// Return int if result is whole number
		if sum == math.Trunc(sum) {
			return int64(sum), nil
		}
		return sum, nil

	case "avg":
		if len(fn.Args) == 0 {
			return nil, fmt.Errorf("AVG requires an argument")
		}
		sum := 0.0
		count := 0
		for _, row := range rows {
			val, err := evalExpr(fn.Args[0], row)
			if err != nil || val == nil {
				continue
			}
			if f, ok := toNumeric(val); ok {
				sum += f
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil

	case "min":
		if len(fn.Args) == 0 {
			return nil, fmt.Errorf("MIN requires an argument")
		}
		var minVal interface{}
		for _, row := range rows {
			val, err := evalExpr(fn.Args[0], row)
			if err != nil || val == nil {
				continue
			}
			if minVal == nil || compareValues(val, minVal) < 0 {
				minVal = val
			}
		}
		return minVal, nil

	case "max":
		if len(fn.Args) == 0 {
			return nil, fmt.Errorf("MAX requires an argument")
		}
		var maxVal interface{}
		for _, row := range rows {
			val, err := evalExpr(fn.Args[0], row)
			if err != nil || val == nil {
				continue
			}
			if maxVal == nil || compareValues(val, maxVal) > 0 {
				maxVal = val
			}
		}
		return maxVal, nil
	}

	return nil, fmt.Errorf("unknown aggregate function: %s", fn.Name)
}

// --- Helper functions ---

func mergeRows(left, right map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{}, len(left)+len(right))
	for k, v := range left {
		merged[k] = v
	}
	for k, v := range right {
		merged[k] = v
	}
	return merged
}

func lookupColumn(row map[string]interface{}, name string) interface{} {
	// Exact match
	if v, ok := row[name]; ok {
		return v
	}
	// Case-insensitive
	for k, v := range row {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return nil
}

func deduplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]bool)
	var result [][]interface{}
	for _, row := range rows {
		key := fmt.Sprintf("%v", row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

func sortRows(rows [][]interface{}, outputCols []string, orderBy []OrderByClause) {
	colIdxMap := make(map[string]int)
	for i, c := range outputCols {
		colIdxMap[strings.ToLower(c)] = i
	}

	sort.SliceStable(rows, func(a, b int) bool {
		for _, ob := range orderBy {
			colName := ob.Column
			if colName == "" && ob.Expr != nil {
				if ref, ok := ob.Expr.(*ColumnRef); ok {
					colName = ref.Name
				}
			}
			idx, ok := colIdxMap[strings.ToLower(colName)]
			if !ok {
				continue
			}
			cmp := compareValues(rows[a][idx], rows[b][idx])
			if cmp == 0 {
				continue
			}
			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

func deserializeRow(data []byte) (map[string]interface{}, error) {
	var row map[string]interface{}
	if err := json.Unmarshal(data, &row); err != nil {
		return nil, err
	}
	return row, nil
}

func serializeValue(v interface{}) []byte {
	switch val := v.(type) {
	case string:
		return []byte(val)
	case int64:
		return []byte(fmt.Sprintf("%020d", val))
	case float64:
		if val == float64(int64(val)) {
			return []byte(fmt.Sprintf("%020d", int64(val)))
		}
		return []byte(fmt.Sprintf("%v", val))
	case nil:
		return []byte("NULL")
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}

func evalLiteral(expr Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *LiteralExpr:
		return e.Value, nil
	case *UnaryExpr:
		if e.Op == "-" {
			val, err := evalLiteral(e.Expr)
			if err != nil {
				return nil, err
			}
			switch v := val.(type) {
			case int64:
				return -v, nil
			case float64:
				return -v, nil
			}
		}
		return nil, fmt.Errorf("cannot evaluate unary %s as literal", e.Op)
	default:
		return nil, fmt.Errorf("expected literal, got %T", expr)
	}
}

func evalExpr(expr Expr, row map[string]interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *LiteralExpr:
		return e.Value, nil

	case *ColumnRef:
		name := e.Name
		if e.Table != "" {
			name = e.Table + "." + e.Name
		}
		return lookupColumn(row, name), nil

	case *BinaryExpr:
		left, err := evalExpr(e.Left, row)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(e.Right, row)
		if err != nil {
			return nil, err
		}
		return evalBinaryOp(e.Op, left, right)

	case *UnaryExpr:
		val, err := evalExpr(e.Expr, row)
		if err != nil {
			return nil, err
		}
		switch e.Op {
		case "NOT":
			return !isTruthy(val), nil
		case "-":
			return negateValue(val)
		}
		return nil, fmt.Errorf("unknown unary op: %s", e.Op)

	case *FuncExpr:
		// Single-row aggregate context — evaluate on single row
		return computeAggregate(e, []map[string]interface{}{row})

	case *IsNullExpr:
		val, err := evalExpr(e.Expr, row)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return val != nil, nil
		}
		return val == nil, nil

	case *InExpr:
		val, err := evalExpr(e.Expr, row)
		if err != nil {
			return nil, err
		}
		found := false
		for _, v := range e.Values {
			cmp, err := evalExpr(v, row)
			if err != nil {
				continue
			}
			if compareValues(val, cmp) == 0 {
				found = true
				break
			}
		}
		if e.Not {
			return !found, nil
		}
		return found, nil
	}

	return nil, fmt.Errorf("cannot evaluate expression: %T", expr)
}

func evalBinaryOp(op string, left, right interface{}) (interface{}, error) {
	switch op {
	case "AND":
		return isTruthy(left) && isTruthy(right), nil
	case "OR":
		return isTruthy(left) || isTruthy(right), nil
	case "=":
		return compareValues(left, right) == 0, nil
	case "!=", "<>":
		return compareValues(left, right) != 0, nil
	case "<":
		return compareValues(left, right) < 0, nil
	case ">":
		return compareValues(left, right) > 0, nil
	case "<=":
		return compareValues(left, right) <= 0, nil
	case ">=":
		return compareValues(left, right) >= 0, nil
	case "LIKE":
		return matchLike(toString(left), toString(right)), nil
	case "+":
		return addValues(left, right)
	case "-":
		return subValues(left, right)
	case "*":
		return mulValues(left, right)
	case "/":
		return divValues(left, right)
	}
	return nil, fmt.Errorf("unknown operator: %s", op)
}

func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	case json.Number:
		f, _ := val.Float64()
		return f
	}
	return 0
}

func toInt(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case float64:
		return int64(val)
	case string:
		n, _ := strconv.ParseInt(val, 10, 64)
		return n
	case json.Number:
		n, _ := val.Int64()
		return n
	}
	return 0
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != "" && val != "0" && val != "false"
	}
	return true
}

func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	af, aOk := toNumeric(a)
	bf, bOk := toNumeric(b)
	if aOk && bOk {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	as := toString(a)
	bs := toString(b)
	return page.CompareBytes([]byte(as), []byte(bs))
}

func toNumeric(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	case json.Number:
		f, err := val.Float64()
		return f, err == nil
	}
	return 0, false
}

func negateValue(v interface{}) (interface{}, error) {
	switch val := v.(type) {
	case int64:
		return -val, nil
	case float64:
		return -val, nil
	}
	return nil, fmt.Errorf("cannot negate %T", v)
}

func addValues(a, b interface{}) (interface{}, error) {
	af, aOk := toNumeric(a)
	bf, bOk := toNumeric(b)
	if aOk && bOk {
		return af + bf, nil
	}
	return toString(a) + toString(b), nil
}

func subValues(a, b interface{}) (interface{}, error) {
	return toFloat(a) - toFloat(b), nil
}

func mulValues(a, b interface{}) (interface{}, error) {
	return toFloat(a) * toFloat(b), nil
}

func divValues(a, b interface{}) (interface{}, error) {
	bv := toFloat(b)
	if bv == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return toFloat(a) / bv, nil
}

func matchLike(s, pattern string) bool {
	s = strings.ToLower(s)
	pattern = strings.ToLower(pattern)
	return matchLikeHelper(s, pattern)
}

func matchLikeHelper(s, p string) bool {
	for len(p) > 0 {
		switch p[0] {
		case '%':
			for len(p) > 0 && p[0] == '%' {
				p = p[1:]
			}
			if len(p) == 0 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if matchLikeHelper(s[i:], p) {
					return true
				}
			}
			return false
		case '_':
			if len(s) == 0 {
				return false
			}
			s = s[1:]
			p = p[1:]
		default:
			if len(s) == 0 || s[0] != p[0] {
				return false
			}
			s = s[1:]
			p = p[1:]
		}
	}
	return len(s) == 0
}

// FormatResult formats a result as a text table.
func FormatResult(r *Result) string {
	if r.Message != "" && len(r.Rows) == 0 {
		return r.Message
	}

	if len(r.Columns) == 0 {
		return r.Message
	}

	// Calculate column widths
	widths := make([]int, len(r.Columns))
	for i, col := range r.Columns {
		widths[i] = len(col)
	}
	for _, row := range r.Rows {
		for i, val := range row {
			if i < len(widths) {
				s := formatValue(val)
				if len(s) > widths[i] {
					widths[i] = len(s)
				}
			}
		}
	}

	// Cap column widths
	for i := range widths {
		if widths[i] > 50 {
			widths[i] = 50
		}
	}

	var sb strings.Builder

	// Header
	for i, col := range r.Columns {
		if i > 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(padRight(col, widths[i]))
	}
	sb.WriteString("\n")

	// Separator
	for i := range r.Columns {
		if i > 0 {
			sb.WriteString("-+-")
		}
		sb.WriteString(strings.Repeat("-", widths[i]))
	}
	sb.WriteString("\n")

	// Rows
	for _, row := range r.Rows {
		for i, val := range row {
			if i > 0 {
				sb.WriteString(" | ")
			}
			s := formatValue(val)
			if len(s) > 50 {
				s = s[:47] + "..."
			}
			if i < len(widths) {
				sb.WriteString(padRight(s, widths[i]))
			} else {
				sb.WriteString(s)
			}
		}
		sb.WriteString("\n")
	}

	sb.WriteString(fmt.Sprintf("(%d rows)\n", len(r.Rows)))

	return sb.String()
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case float64:
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}
