package dsql

import (
	"strings"

	"github.com/juju/errors"

	"github.com/pingcap/parser/ast"
	pmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

const (
	CerrOk                         int = 0
	CerrKeyNotExists               int = 1
	CerrGetValueNotOk              int = 2
	CerrConvertInterfaceValueNotOk int = 3

	CprimaryKeyName     string = "primary"
	CalterColumnTypeAdd int    = iota + 1
	CalterColumnTypeDrop
	CalterColumnTypeChange
	CalterColumnTypeModify
)

//create database statment
func (this *CurrentDatabase) GetDatabaseFromCreateDatabase(stm *ast.CreateDatabaseStmt) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	this.CreateDatabase = stm.Name
	this.Sql = stm.Text()
}

// use database statement
func (this *CurrentDatabase) GetDatabaseFromUseDatabase(stm *ast.UseStmt) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	this.UseDatabase = stm.DBName
	this.Sql = stm.Text()

}

// get table schema from create table ... as ...
// not supported, too complicate
/*
func (this *TableCache) GetTblDefFromCreateTableAs(tbSchema *TableSchemaVersion, currentDb string, stm *ast.CreateTableStmt) error {
	var (
		db     string = stm.Table.Schema.L
		tb     string = stm.Table.Name.L
		fullTb string
	)
	if currentDb != "" {
		if db == "" {
			db = currentDb
		}
	}
	fullTb = GetFullTableName(false, db, tb)
	tbSchema.GetTblDefFromCreateTableDirectly(currentDb, stm)
	selectStm, ok := stm.Select.(*ast.SelectStmt)
	if !ok {
		return errors.Errorf("error to convert CreateTableStmt.Select into *SelectStmt: ", stm.Text())
	}

	// to be implemented

}
*/

// get table schema from create table ... like ...
func (this *TableCache) GetTblDefFromCreateTableLike(tbSchema *TableSchemaVersion, currentDb string, stm *ast.CreateTableStmt) error {
	var (
		db       string = stm.Table.Schema.L
		tb       string = stm.Table.Name.L
		fullTb   string
		dbAs     string = stm.ReferTable.Schema.L
		tbAs     string = stm.ReferTable.Name.L
		fullTbAs string
	)
	if currentDb != "" {
		if db == "" {
			db = currentDb
		}
		if dbAs == "" {
			dbAs = currentDb
		}
	}

	fullTb = GetFullTableName(false, db, tb)
	fullTbAs = GetFullTableName(false, dbAs, tbAs)
	asTbSchema, err := this.GetTableScheme(fullTbAs, true)
	if err != nil {
		return err
	}
	tbSchema.Info.Def = asTbSchema.Info.Def.Copy()
	tbSchema.GetSrc = CgetsrcBinDdl
	tbSchema.Ddl = stm.Text()
	tbSchema.Info.Table.Database = db
	tbSchema.Info.Table.TableName = tb
	tbSchema.IfHasDef = true
	tbSchema.IfHasPrimaryKey = asTbSchema.IfHasPrimaryKey
	tbSchema.IfHasUniqueKey = asTbSchema.IfHasUniqueKey
	this.TableDefs.Set(fullTb, tbSchema)
	return nil
}

// only for create table directly, not for create table like/as
// return NotFound Error if column of key not found in table def
func (this *TableDef) GetTblDefFromCreateTableDirectly(fullTbName string, stm *ast.CreateTableStmt) error {

	for idx, oneCol := range stm.Cols {
		this.AddColDefFromOneParsedCol(fullTbName, oneCol, idx)

	}

	for _, oneK := range stm.Constraints {
		err := this.AddPrimayUniqueKeyFromConstraint(fullTbName, oneK)
		if err != nil {
			return err
		}

	}
	return nil
}

// create index. primary key cannot be created by create index
// return NotValidError if it is not unique index or no colunms
func (this *TableDef) AddIndexFromCreateIndex(fullTbName string, stm *ast.CreateIndexStmt) error {

	if !stm.Unique {
		// we only care about unique key
		return errors.NotValidf("not a unique index: %s", stm.Text())
	}
	ukey := NewEmptyKeyDef()
	ukey.Name = stm.IndexName
	for _, oneCol := range stm.IndexColNames {
		colName := oneCol.Column.Name.O
		idx := this.GetColIndxByName(colName)
		if idx < 0 {
			return errors.NotFoundf("column %s not found in table def of %s : %s", colName, fullTbName, stm.Text())
		}
		ukey.ColumnNames = append(ukey.ColumnNames, colName)
		ukey.ColumnIndices = append(ukey.ColumnIndices, idx)
	}
	if len(ukey.ColumnNames) > 0 {
		this.UniqueKeys[ukey.Name] = ukey
		return nil
	} else {
		return errors.NotValidf("strange, no columns for index: %s", stm.Text())
	}
}

// add new column at position idx
func (this *TableDef) AddColDefFromOneParsedCol(fullTbName string, colParsed *ast.ColumnDef, idx int) error {
	colName := colParsed.Name.Name.String()
	if this.GetColIndxByName(colName) >= 0 {
		return errors.AlreadyExistsf("column %s already exists in table %s", colName, fullTbName)
	}

	col := &ColDef{
		Name:          colName,
		TypeName:      types.TypeToStr(colParsed.Tp.Tp, colParsed.Tp.Charset),
		TypeCode:      colParsed.Tp.Tp,
		Notnull:       pmysql.HasNotNullFlag(colParsed.Tp.Flag),
		Unsigned:      pmysql.HasUnsignedFlag(colParsed.Tp.Flag),
		AutoIncrement: pmysql.HasUnsignedFlag(colParsed.Tp.Flag),
		Elements:      append([]string{}, colParsed.Tp.Elems...),
	}

	for _, opt := range colParsed.Options {
		if opt.Tp == ast.ColumnOptionAutoIncrement {
			col.AutoIncrement = true
		} else if opt.Tp == ast.ColumnOptionNotNull {
			col.Notnull = true
		} else if opt.Tp == ast.ColumnOptionPrimaryKey {
			this.PrimaryKey = &KeyDef{Name: CprimaryKeyName, ColumnNames: []string{colName}, ColumnIndices: []int{idx}}
		} else if opt.Tp == ast.ColumnOptionUniqKey {
			this.UniqueKeys[colName] = &KeyDef{
				Name:          colParsed.Name.Name.String(),
				ColumnNames:   []string{colName},
				ColumnIndices: []int{idx},
			}
		}
	}

	ifNeedResetColumnPositionOfIndex := false
	colCnt := len(this.Columns)
	if colCnt == 0 || colCnt == idx {
		this.Columns = append(this.Columns, col)
	} else if idx == 0 {
		ifNeedResetColumnPositionOfIndex = true
		this.Columns = append([]*ColDef{col}, this.Columns...)
	} else {
		ifNeedResetColumnPositionOfIndex = true
		this.Columns = append(this.Columns[:idx], append([]*ColDef{col}, this.Columns[idx:]...)...)
	}
	if ifNeedResetColumnPositionOfIndex {
		err := this.SetNewColumnPositionOfIndex(fullTbName)
		if err != nil {
			return err
		}
	}

	if pmysql.HasPriKeyFlag(colParsed.Tp.Flag) {
		this.PrimaryKey = &KeyDef{Name: CprimaryKeyName, ColumnNames: []string{colName}, ColumnIndices: []int{idx}}
	} else if pmysql.HasUniKeyFlag(colParsed.Tp.Flag) {
		// the key only have one column and name is the column name
		this.UniqueKeys[colName] = &KeyDef{
			Name:          colParsed.Name.Name.String(),
			ColumnNames:   []string{colName},
			ColumnIndices: []int{idx},
		}
	}
	return nil
}

// return NotFound Error if column of key not found in table def
func (this *TableDef) AddPrimayUniqueKeyFromConstraint(tbFullName string, cst *ast.Constraint) error {
	for _, k := range cst.Keys {
		idx := this.GetColIndxByName(k.Column.Name.String())
		//strange, can't find the column in table def
		if idx < 0 {
			return errors.NotFoundf("cannot find column %s for index %s in table def of %s", k.Column.Name.String(), cst.Name, tbFullName)
		}
		if cst.Tp == ast.ConstraintPrimaryKey {
			//primary key
			this.PrimaryKey.Name = CprimaryKeyName
			this.PrimaryKey.ColumnNames = append(this.PrimaryKey.ColumnNames, k.Column.Name.String())
			this.PrimaryKey.ColumnIndices = append(this.PrimaryKey.ColumnIndices, idx)

		} else if cst.Tp == ast.ConstraintUniqKey || cst.Tp == ast.ConstraintUniq || cst.Tp == ast.ConstraintUniqIndex {
			//unique key
			if _, ok := this.UniqueKeys[cst.Name]; ok {
				this.UniqueKeys[cst.Name].ColumnNames = append(this.UniqueKeys[cst.Name].ColumnNames, k.Column.Name.String())
				this.UniqueKeys[cst.Name].ColumnIndices = append(this.UniqueKeys[cst.Name].ColumnIndices, idx)

			} else {
				this.UniqueKeys[cst.Name] = &KeyDef{
					Name:          cst.Name,
					ColumnNames:   []string{k.Column.Name.String()},
					ColumnIndices: []int{idx},
				}
			}

		}
	}
	return nil
}

//  modify column name of primary/unique key info because of drop/change column
func (this *TableDef) ModifyColumnNameOfIndexByColumn(opType int, oldColName string, newColName string) {
	var (
		ifContain bool = false
	)

	// handle unique keys
	for k := range this.UniqueKeys {
		ifContain = false
		for i, oneColName := range this.UniqueKeys[k].ColumnNames {
			if oneColName == oldColName {
				// only drop and change make the column name change
				ifContain = true
				if opType == CalterColumnTypeChange {
					if newColName != oldColName && newColName != "" {
						// column name changed
						this.UniqueKeys[k].ColumnNames[i] = newColName
					}
				}
			}
		}
		if opType == CalterColumnTypeDrop && ifContain {
			// delete any unique key contain the column
			delete(this.UniqueKeys, k)
		}
	}
	if len(this.UniqueKeys) == 0 {
		this.UniqueKeys = nil
	}

	// handle primary key
	ifContain = false
	for i, oneColName := range this.PrimaryKey.ColumnNames {
		if oneColName == oldColName {
			// only drop and change make the column name change
			ifContain = true
			if opType == CalterColumnTypeChange {
				if newColName != oldColName && newColName != "" {
					// column name changed
					this.PrimaryKey.ColumnNames[i] = newColName
				}
			}
		}
	}
	if opType == CalterColumnTypeDrop && ifContain {
		// delete primary key
		this.PrimaryKey = nil
	}
}

//  modify column name of primary/unique key info because of drop/change column
func (this *TableDef) SetNewColumnPositionOfIndex(fullTbName string) error {
	for i, oneColName := range this.PrimaryKey.ColumnNames {
		newIdx := this.GetColIndxByName(oneColName)
		if newIdx < 0 {
			return errors.NotFoundf("column %s not found in table def of %s", oneColName, fullTbName)
		}
		this.PrimaryKey.ColumnIndices[i] = newIdx
	}

	for k := range this.UniqueKeys {
		for i, oneColName := range this.UniqueKeys[k].ColumnNames {
			newIdx := this.GetColIndxByName(oneColName)
			if newIdx < 0 {
				return errors.NotFoundf("column %s not found in table def of %s", oneColName, fullTbName)
			}
			this.UniqueKeys[k].ColumnIndices[i] = newIdx
		}
	}
	return nil
}

// column name/index and unique/primay key info changed because drop, change, modify column
// alter table add column not handled by this func
// column name and column index are only cared about
// this method must be called after modify this.Columns
func (this *TableDef) ModifyIndexInfoByAlterColumnWithoutAdd(fullTbName string, opType int, oldColName string, newColName string) error {
	this.ModifyColumnNameOfIndexByColumn(opType, oldColName, newColName)
	return this.SetNewColumnPositionOfIndex(fullTbName)
}

// drop column from table def
func (this *TableDef) DropColumn(fullTbName, colName string) error {

	idx := this.GetColIndxByName(colName)
	if idx < 0 {
		return errors.NotFoundf("column %s not found in table def %s", colName, fullTbName)
	}
	newColumns := []*ColDef{}
	for i := range this.Columns {
		if i != idx {
			newColumns = append(newColumns, this.Columns[i].Copy())
		}
	}
	this.Columns = newColumns
	return this.ModifyIndexInfoByAlterColumnWithoutAdd(fullTbName, CalterColumnTypeDrop, colName, "")
}

func (this *TableDef) GetPositionForColumn(fullTbName string, tp ast.ColumnPositionType, relativeColumn string) (int, error) {
	var (
		idx int
	)
	if tp == ast.ColumnPositionNone {
		idx = len(this.Columns)
	} else if tp == ast.ColumnPositionFirst {
		idx = 0
	} else {
		idx = this.GetColIndxByName(relativeColumn)
		if idx < 0 {
			return -1, errors.Errorf("column %s not found in TableDef.Columns of %s",
				relativeColumn, fullTbName)
		}
		idx++
	}
	return idx, nil
}

func (this *TableDef) DropPrimaryUniqueIndex(idxName string) bool {
	if idxName == CprimaryKeyName {
		this.PrimaryKey = nil
		return true
	} else {
		_, ok := this.UniqueKeys[idxName]
		if ok {
			delete(this.UniqueKeys, idxName)
			return true
		}
		return false
	}
}

// alter table. not handle rename table here
func (this *TableDef) UpdateTableSchemaFromAlterTable(fullTbName string, stm *ast.AlterTableStmt) error {
	var (
		idx int
		err error
	)

	for _, oneAlter := range stm.Specs {

		switch oneAlter.Tp {
		case ast.AlterTableAddColumns:
			idx, err = this.GetPositionForColumn(fullTbName, oneAlter.Position.Tp, oneAlter.Position.RelativeColumn.Name.O)
			if err != nil {
				return errors.Annotate(err, "DDL: "+stm.Text())
			}
			err = this.AddColDefFromOneParsedCol(fullTbName, oneAlter.NewColumns[0], idx)
			if err != nil {
				errors.Annotate(err, "DDL: "+stm.Text())
			}
		case ast.AlterTableAddConstraint:
			err = this.AddPrimayUniqueKeyFromConstraint(fullTbName, oneAlter.Constraint)
			if err != nil {
				return err
			}
		case ast.AlterTableDropColumn:
			err = this.DropColumn(fullTbName, oneAlter.OldColumnName.Name.O)
			if err != nil {
				errors.Annotate(err, "DDL: "+stm.Text())
			}
		case ast.AlterTableDropPrimaryKey:
			this.DropPrimaryUniqueIndex(CprimaryKeyName)
		case ast.AlterTableDropIndex:
			this.DropPrimaryUniqueIndex(oneAlter.Name)
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			colName := ""
			if oneAlter.Tp == ast.AlterTableModifyColumn {
				colName = oneAlter.NewColumns[0].Name.Name.O
			} else {
				colName = oneAlter.OldColumnName.Name.O
			}
			oldColIdx := this.GetColIndxByName(colName)
			if oldColIdx < 0 {
				return errors.NotFoundf("column %s not found in TableDef.Columns of %s: %s", colName, fullTbName, stm.Text())
			}
			err = this.DropColumn(fullTbName, colName)
			if err != nil {
				errors.Annotate(err, "DDL: "+stm.Text())
			}

			idx, err = this.GetPositionForColumn(fullTbName, oneAlter.Position.Tp, oneAlter.Position.RelativeColumn.Name.O)
			if err != nil {
				errors.Annotate(err, "DDL: "+stm.Text())
			}
			if oneAlter.Position.Tp == ast.ColumnPositionNone {
				idx = oldColIdx
			}
			err = this.AddColDefFromOneParsedCol(fullTbName, oneAlter.NewColumns[0], idx)
			if err != nil {
				errors.Annotate(err, "DDL: "+stm.Text())
			}
		case ast.AlterTableRenameIndex:
			idxDef, ok := this.UniqueKeys[oneAlter.FromKey.O]
			if ok {
				this.UniqueKeys[oneAlter.ToKey.O] = idxDef.Copy()
				delete(this.UniqueKeys, oneAlter.FromKey.O)
			}

		}
	}
	return nil
}

func (this *TableCache) RenameTable(oldTb *TableId, newTb *TableId) int {
	oldFull := oldTb.FullName()
	if this.TableDefs.Has(oldFull) {
		val, ok := this.TableDefs.Get(oldFull)
		if !ok {
			return CerrGetValueNotOk
		}
		tb, ok := val.(*TableSchemaVersion)
		if !ok {
			return CerrConvertInterfaceValueNotOk
		}
		tb.Info.Table = newTb
		tb.GetSrc = CgetsrcBinDdl
		this.TableDefs.Remove(oldFull)
		this.TableDefs.Set(newTb.FullName(), tb)
		return CerrOk
	} else {
		return CerrKeyNotExists
	}

}

func (this *TableCache) DropTable(dbName, tbName string) int {
	tbFull := GetFullTableName(false, dbName, tbName)
	if this.TableDefs.Has(tbFull) {
		this.TableDefs.Remove(tbFull)
		return CerrOk
	} else {
		return CerrKeyNotExists
	}
}

func (this *TableCache) DropDatabase(dbName string) int {
	var result int = CerrKeyNotExists
	for _, oneK := range this.TableDefs.Keys() {
		if strings.HasPrefix(oneK, dbName+".") {
			this.TableDefs.Remove(oneK)
			result = CerrOk
		}
	}
	return result
}
