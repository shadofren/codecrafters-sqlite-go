package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

const (
	InteriorIndexPage = 0x02
	InteriorTablePage = 0x05
	LeafIndexPage     = 0x0a
	LeafTablePage     = 0x0d
)

const (
	NULL    = 0x00
	INT8    = 0x01
	INT16   = 0x02
	INT24   = 0x03
	INT32   = 0x04
	INT48   = 0x05
	INT64   = 0x06
	FLOAT64 = 0x07
	ZERO    = 0x08
	ONE     = 0x09
	BLOB    = 0xc
	TEXT    = 0xd
)

type DBInfo struct {
	Header        []byte
	PageSize      uint16
	ReservedSpace uint8 // usually 0, but we need to account for this
	Tables        []*SQLiteSchema
	PageReadCount int
}

type SQLiteSchema struct {
	Type     string
	Name     string
	TblName  string
	RootPage int64
	Sql      string
}

type BTreePageHeader struct {
	PageType                    uint8
	StartOfFirstFree            uint16
	NumberOfCells               uint16
	StartOfCellContent          uint16
	NumberOfFragmentedFreeBytes uint8
	// The rightmost pointer points to the child page that contains all keys greater than or equal to the maximum key in the current interior page.
	RightMostPointer uint32
}

type Cell struct {
	LeftChildPage int64  // actually only 4 byte but to be consistent
	Value         string // i assume
	RowId         int64  // varint, The keys in an interior page represent the upper bounds for the ranges of keys in the child pages.
	Payload       []any  // for the table leaf cell
}

func DBInfoCmd(dbPath string) {
	info, pageHeader := readInfo(dbPath)

	fmt.Println("database page size:", info.PageSize)
	fmt.Println("number of tables:", pageHeader.NumberOfCells)
}

func DBTablesCmd(dbPath string) {
	info, _ := readInfo(dbPath)
	for _, table := range info.Tables {
		fmt.Printf("%s ", table.TblName)
	}
	fmt.Println()
}

func DBSelectCmd(dbPath string, stmt *sqlparser.Select) {
	tableExpr := stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
	tableName := tableExpr.Name.String()
	where := stmt.Where
	info, _ := readInfo(dbPath)

	// check the where clause to see if we are using a table with index
	if where != nil {
		// find an index
		var index, table *SQLiteSchema
		for _, t := range info.Tables {
			if t.TblName == tableName {
				if t.Type == "index" {
					index = t
				} else if t.Type == "table" {
					table = t
				}
				// assume this is the correct index for column country
			}
		}
		if index != nil {
			selectIndex(dbPath, stmt, info, index, table)
		} else {
			selectScan(dbPath, stmt, info, table)
		}
		return
	}
	for _, table := range info.Tables {
		if table.Name != tableName || table.Type != "table" {
			continue
		}
		// sequential scan
		selectScan(dbPath, stmt, info, table)
	}
}

func selectIndex(dbPath string, stmt *sqlparser.Select, info *DBInfo, index *SQLiteSchema, table *SQLiteSchema) {
	indexedValue := string(stmt.Where.Expr.(*sqlparser.ComparisonExpr).Right.(*sqlparser.SQLVal).Val)
	_ = indexedValue
	isCount := false
	_ = isCount
	outputCols := make([]string, 0)
	/* should be an AliasedExpr, work on 1 column right now */
	for _, expr := range stmt.SelectExprs {
		expr := expr.(*sqlparser.AliasedExpr).Expr
		if _, ok := expr.(*sqlparser.FuncExpr); ok {
			isCount = true
			break
		}
		colName := expr.(*sqlparser.ColName).Name.String()
		outputCols = append(outputCols, colName)
	}

	// do the recursion here because we could be reading interior table
	// filter the cells
	rowIds := make([]int64, 0)

	queue := []int64{index.RootPage}
	i := 0
	for i < len(queue) {
		page := queue[i]

		pageHeader, pageCells := readPage(dbPath, info, page)
		switch pageHeader.PageType {
		case InteriorIndexPage:
			// first cell
			low := ""
			for _, cell := range pageCells {
				if low <= indexedValue && indexedValue <= cell.Value {
					queue = append(queue, cell.LeftChildPage)
				}
				// the interior cell also contains some row
				if indexedValue == cell.Value {
					rowIds = append(rowIds, cell.RowId)
				}
				low = cell.Value
			}
			// should  we continue with the right most page?
			if low <= indexedValue && pageHeader.RightMostPointer != 0 {
				queue = append(queue, int64(pageHeader.RightMostPointer))
			}
		case LeafIndexPage:
			for _, cell := range pageCells {
				if cell.Value == indexedValue {
					rowIds = append(rowIds, cell.RowId)
				}
			}
		default:
			fmt.Println("SOMETHING WRONG")
		}
		i++
	}

	if isCount {
		fmt.Println(len(rowIds))
		return
	}

	sort.Slice(rowIds, func(i, j int) bool {
		return rowIds[i] < rowIds[j]
	})

	rows := make([][]any, 0)
	var dfs func(startRow, pageId int64, lookups []int64)
	dfs = func(startRow, pageId int64, lookups []int64) {

		pageHeader, pageCells := readPage(dbPath, info, pageId)
		switch pageHeader.PageType {
		case InteriorTablePage:
			i := 0
			for _, cell := range pageCells {
				endRow := cell.RowId
				subLookups := make([]int64, 0)
				for i < len(lookups) {
					if lookups[i] < startRow {
						i++
						continue
					} else if lookups[i] > endRow {
						break
					} else {
						subLookups = append(subLookups, lookups[i])
						i++
					}
				}
				if len(subLookups) > 0 {
					dfs(startRow, cell.LeftChildPage, subLookups)
				}
			}
			if i < len(lookups) {
				// more item on the RightMostPointer
				dfs(startRow, int64(pageHeader.RightMostPointer), lookups[i:])
			}
		case LeafTablePage:
			i := 0
			for _, cell := range pageCells {
				if cell.RowId == lookups[i] {
					rows = append(rows, cell.Payload)
					i++
				}
				if i == len(lookups) {
					break
				}
			}
		default:
			fmt.Println("SOMETHING WRONG")
		}
		i++
	}
	dfs(1, table.RootPage, rowIds)

	sql := cleanSql(table.Sql)
	tableSql, err := sqlparser.Parse(sql)
	must(err)
	// the columns in order, then we parse the data
	columns := tableSql.(*sqlparser.DDL).TableSpec.Columns
	tableCols := make(map[string]int)
	for i, col := range columns {
		tableCols[col.Name.String()] = i
	}

	results := make([][]string, len(rows))
	for _, outCol := range outputCols {
		if idx, ok := tableCols[outCol]; ok {
			for i, row := range rows {
				switch r := row[idx].(type) {
				case int64:
					results[i] = append(results[i], strconv.FormatInt(r, 10))
				case string:
					results[i] = append(results[i], r)
				default:
					fmt.Println("not supported type")
				}
			}
		}
	}

	for _, res := range results {
		fmt.Println(strings.Join(res, "|"))
	}

}

func selectScan(dbPath string, stmt *sqlparser.Select, info *DBInfo, table *SQLiteSchema) {
	isCount := false
	outputCols := make([]string, 0)
	/* should be an AliasedExpr, work on 1 column right now */
	for _, expr := range stmt.SelectExprs {
		expr := expr.(*sqlparser.AliasedExpr).Expr
		if _, ok := expr.(*sqlparser.FuncExpr); ok {
			isCount = true
			break
		}
		colName := expr.(*sqlparser.ColName).Name.String()
		outputCols = append(outputCols, colName)
	}

	// do the recursion here because we could be reading interior table
	// filter the cells
	filteredCells := make([]*Cell, 0)
	sql := cleanSql(table.Sql)
	tableSql, err := sqlparser.Parse(sql)
	must(err)
	// the columns in order, then we parse the data
	columns := tableSql.(*sqlparser.DDL).TableSpec.Columns
	tableCols := make(map[string]int)
	for i, col := range columns {
		tableCols[col.Name.String()] = i
	}

	queue := []int64{table.RootPage}
	i := 0
	for i < len(queue) {
		page := queue[i]
		pageHeader, pageCells := readPage(dbPath, info, page)
		switch pageHeader.PageType {
		case InteriorTablePage:
			for _, cell := range pageCells {
				queue = append(queue, cell.LeftChildPage)
			}
		case LeafTablePage:
			for _, cell := range pageCells {
				if filter(cell, tableCols, stmt.Where) {
					filteredCells = append(filteredCells, cell)
				}
			}
		default:
			fmt.Println("SOMETHING WRONG")
		}
		i++
	}

	if isCount {
		fmt.Println(len(filteredCells))
		return
	}

	results := make([][]string, len(filteredCells))
	for _, outCol := range outputCols {
		if idx, ok := tableCols[outCol]; ok {
			for i, row := range filteredCells {
				switch r := row.Payload[idx].(type) {
				case int64:
					results[i] = append(results[i], strconv.FormatInt(r, 10))
				case string:
					results[i] = append(results[i], r)
				default:
					fmt.Println("not supported type")
				}
			}
		}
	}

	for _, res := range results {
		fmt.Println(strings.Join(res, "|"))
	}

}

func cleanSql(sql string) string {
	sql = strings.ReplaceAll(sql, "autoincrement", "")
	sql = strings.ReplaceAll(sql, "\"", "")    // table name has doublequote
	sql = strings.ReplaceAll(sql, "range", "") // not a keyword
	return sql
}

func filter(row *Cell, tableCols map[string]int, where *sqlparser.Where) bool {
	if where == nil {
		return true
	}

	if where, ok := where.Expr.(*sqlparser.ComparisonExpr); ok {
		// assume to be equal
		colName := where.Left.(*sqlparser.ColName).Name.String()
		colValue := string(where.Right.(*sqlparser.SQLVal).Val)
		if idx, ok := tableCols[colName]; ok {
			value := row.Payload[idx]
			return value == colValue
		}
	}
	return false
}

func readInfo(dbPath string) (*DBInfo, *BTreePageHeader) {

	dbFile, err := os.Open(dbPath)
	must(err)
	defer dbFile.Close()

	header := make([]byte, 100)
	_, err = dbFile.Read(header)
	must(err)

	dbinfo := &DBInfo{Header: header[:12]}
	err = binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &dbinfo.PageSize)
	must(err)
	dbinfo.ReservedSpace = header[20]

	// create buffer for full page
	pageContent := make([]byte, dbinfo.PageSize)
	_, err = dbFile.Read(pageContent[100:]) // read the content, skipping the header
	must(err)

	reader := bytes.NewReader(pageContent)
	reader.Seek(100, io.SeekStart) // skip the first 100 bytes of header

	pageHeader, _ := readPageHeader(reader)

	cellStarts := []int64{}
	// start of cell pointer array
	for i := 0; i < int(pageHeader.NumberOfCells); i++ {
		var p uint16
		binary.Read(reader, binary.BigEndian, &p)
		cellStarts = append(cellStarts, int64(p))
	}

	tables := make([]*SQLiteSchema, 0)
	for _, idx := range cellStarts {
		reader.Seek(idx, io.SeekStart)
		row := readTableLeafCell(reader)
		table := &SQLiteSchema{}
		table.Type, _ = row.Payload[0].(string)
		table.Name, _ = row.Payload[1].(string)
		table.TblName, _ = row.Payload[2].(string)
		table.RootPage, _ = row.Payload[3].(int64)
		table.Sql, _ = row.Payload[4].(string)
		tables = append(tables, table)
	}

	dbinfo.Tables = tables
	return dbinfo, pageHeader
}

// non-recursive, recursion will be done by the caller
func readPage(dbPath string, dbinfo *DBInfo, page int64) (*BTreePageHeader, []*Cell) {
	dbFile, err := os.Open(dbPath)
	must(err)
	defer dbFile.Close()

	dbinfo.PageReadCount++
	offset := (page - 1) * int64(dbinfo.PageSize)

	dbFile.Seek(offset, io.SeekStart)
	content := make([]byte, dbinfo.PageSize)
	dbFile.Read(content)

	reader := bytes.NewReader(content)
	pageHeader, _ := readPageHeader(reader)

	cellStarts := []int64{}
	// start of cell pointer array
	for i := 0; i < int(pageHeader.NumberOfCells); i++ {
		var p uint16
		binary.Read(reader, binary.BigEndian, &p)
		cellStarts = append(cellStarts, int64(p))
	}
	cells := make([]*Cell, 0)
	switch pageHeader.PageType {
	case LeafTablePage:
		for _, offset := range cellStarts {
			reader.Seek(offset, io.SeekStart)
			cell := readTableLeafCell(reader)
			cells = append(cells, cell)
		}
	case InteriorTablePage:
		for _, offset := range cellStarts {
			reader.Seek(offset, io.SeekStart)
			cell := readTableInteriorCell(reader)
			cells = append(cells, cell)
		}
	case InteriorIndexPage:
		for _, offset := range cellStarts {
			reader.Seek(offset, io.SeekStart)
			cell := readIndexInteriorCell(reader)
			cells = append(cells, cell)
		}
	case LeafIndexPage:
		for _, offset := range cellStarts {
			reader.Seek(offset, io.SeekStart)
			cell := readIndexLeafCell(reader)
			cells = append(cells, cell)
		}
	default:
		fmt.Println("index not supported", pageHeader.PageType)
	}
	return pageHeader, cells
}

func readTableInteriorCell(reader *bytes.Reader) *Cell {

	cell := &Cell{}
	var leftPage uint32 // 4 byte int
	binary.Read(reader, binary.BigEndian, &leftPage)
	cell.LeftChildPage = int64(leftPage)
	rowId, _ := readVarint(reader)
	cell.RowId = rowId
	return cell
}

func readTableLeafCell(reader *bytes.Reader) *Cell {
	_, _ = readVarint(reader)
	rowId, _ := readVarint(reader)
	data := readPayload(reader)
	if data[0] == nil {
		data[0] = rowId
	}
	return &Cell{
		RowId:   rowId,
		Payload: data,
	}
}

func readIndexInteriorCell(reader *bytes.Reader) *Cell {
	var leftChildPointer uint32
	binary.Read(reader, binary.BigEndian, &leftChildPointer)

	readVarint(reader) // ignore
	data := readPayload(reader)
	value, rowId := data[0], data[1].(int64)
	if value == nil {
		return &Cell{
			LeftChildPage: int64(leftChildPointer),
			RowId:         rowId,
		}
	} else {
		return &Cell{
			LeftChildPage: int64(leftChildPointer),
      Value: value.(string),
			RowId:         rowId,
		}
	}
}

func readIndexLeafCell(reader *bytes.Reader) *Cell {
	readVarint(reader) // ignore
	data := readPayload(reader)
	value, rowId := data[0].(string), data[1].(int64)
	return &Cell{
		Value: value,
		RowId: rowId,
	}
}

func readPayload(reader *bytes.Reader) []any {
	totalHeaderSize, offset := readVarint(reader)
	// read the column Type
	colTypes := make([]int64, 0)
	for offset < int(totalHeaderSize) {
		columnType, m := readVarint(reader)
		colTypes = append(colTypes, columnType)
		offset += m
	}

	data := make([]any, 0)
	// read the data
	for _, t := range colTypes {
		switch t {
		case NULL:
			// this is place holder for the row Id primary index?
			data = append(data, nil)
		case INT8:
			v, _ := reader.ReadByte()
			data = append(data, int64(v))
		case INT16:
			var v uint16
			err := binary.Read(reader, binary.BigEndian, &v)
			must(err)
			data = append(data, int64(v))
		case INT24:
			var v int64
			for i := 0; i < 3; i++ {
				b, _ := reader.ReadByte()
				v = v<<8 + int64(b)
			}
			data = append(data, int64(v))
		case INT32:
			fmt.Println("INT32")
		case INT48:
			fmt.Println("INT48")
		case INT64:
			fmt.Println("INT64")
		case FLOAT64:
			fmt.Println("FLOAT64")
		case ZERO:
			data = append(data, 0)
		case ONE:
			data = append(data, 1)
		default:
			if t&1 == 0 {
				fmt.Println("a blob of size", (t-12)/2)
			} else {
				size := (t - 13) / 2
				text := make([]byte, size)
				reader.Read(text)
				data = append(data, string(text))
			}
		}
	}
	return data
}

var intMask byte = 0x7F

func readVarint(reader *bytes.Reader) (int64, int) {

	byteRead := 1
	var ans int64 = 0
	cur, _ := reader.ReadByte()
	for {
		ans <<= 7
		ans += int64(cur & intMask)
		if (cur >> 7) == 0x00 {
			break
		}
		cur, _ = reader.ReadByte()
		byteRead++
	}

	return ans, byteRead
}

func readPageHeader(reader *bytes.Reader) (*BTreePageHeader, int) {
	header := BTreePageHeader{}
	header.PageType, _ = reader.ReadByte() // single byte
	binary.Read(reader, binary.BigEndian, &header.StartOfFirstFree)
	binary.Read(reader, binary.BigEndian, &header.NumberOfCells)
	binary.Read(reader, binary.BigEndian, &header.StartOfCellContent)
	binary.Read(reader, binary.BigEndian, &header.NumberOfFragmentedFreeBytes)
	n := 8
	if header.PageType == InteriorIndexPage || header.PageType == InteriorTablePage {
		binary.Read(reader, binary.BigEndian, &header.RightMostPointer)
		n += 4
	}
	return &header, n
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
