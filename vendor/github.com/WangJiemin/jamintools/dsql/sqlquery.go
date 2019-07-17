package dsql

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

const (
	// sql which changes table
	CsqlTypeSelect int = iota // start from 0
	CsqlTypeInsert
	CsqlTypeDelete
	CsqlTypeUpdate
	CsqlTypeReplace
	CsqlTypeLoadData
	CsqlTypeAlterTable
	CsqlTypeAlterDatabase
	CsqlTypeCreateTable
	CsqlTypeCreateDatabase
	CsqlTypeCreateIndex
	CsqlTypeDropTable
	CsqlTypeDropDatabase
	CsqlTypeDropIndex
	CsqlTypeTruncateTable
	CsqlTypeRenameTable
)

type DbTable struct {
	Database string
	Table    string
}

func (this DbTable) Copy() DbTable {
	return DbTable{
		Database: this.Database,
		Table:    this.Table,
	}
}

type SqlInfo struct {
	Tables      []DbTable
	UseDatabase string
	SqlStr      string
	SqlType     int
}

func (this *SqlInfo) GetFullTablesAll(sep string) string {
	arr := make([]string, len(this.Tables))
	for i := range this.Tables {
		arr[i] = this.Tables[i].Database + "." + this.Tables[i].Table
	}
	return strings.Join(arr, sep)
}

func (this *SqlInfo) GetTablesAll(sep string) string {
	arr := make([]string, len(this.Tables))
	for i := range this.Tables {
		arr[i] = this.Tables[i].Table
	}
	return strings.Join(arr, sep)
}

func (this *SqlInfo) GetDatabasesAll(sep string) string {
	arr := make([]string, len(this.Tables))
	for i := range this.Tables {
		arr[i] = this.Tables[i].Database
	}
	return strings.Join(arr, sep)
}

func (this *SqlInfo) Copy() *SqlInfo {
	result := &SqlInfo{
		SqlStr:      this.SqlStr,
		SqlType:     this.SqlType,
		UseDatabase: this.UseDatabase,
	}
	for _, tb := range this.Tables {
		result.Tables = append(result.Tables, tb.Copy())
	}
	return result
}

func (this *SqlInfo) IsInsertAll() bool {
	if this.SqlType == CsqlTypeInsert || this.SqlType == CsqlTypeReplace || this.SqlType == CsqlTypeLoadData {
		return true
	} else {
		return false
	}
}
func (this *SqlInfo) GetDmlName() string {
	if this.IsInsertAll() {
		return "insert"
	}
	if this.IsDelete() {
		return "delete"
	}
	if this.IsUpdate() {
		return "update"
	}
	return ""
}

func (this *SqlInfo) IsInsert() bool {
	return this.SqlType == CsqlTypeInsert
}

func (this *SqlInfo) IsReplace() bool {
	return this.SqlType == CsqlTypeReplace
}

func (this *SqlInfo) IsDelete() bool {
	return this.SqlType == CsqlTypeDelete
}

func (this *SqlInfo) IsUpdate() bool {
	return this.SqlType == CsqlTypeUpdate
}

func (this *SqlInfo) IsLoaddata() bool {
	return this.SqlType == CsqlTypeLoadData
}

func (this *SqlInfo) IsDml() bool {
	if this.SqlType == CsqlTypeInsert || this.SqlType == CsqlTypeDelete {
		return true
	}
	if this.SqlType == CsqlTypeUpdate || this.SqlType == CsqlTypeReplace {
		return true
	}
	if this.SqlType == CsqlTypeLoadData {
		return true
	}
	return false
}

func (this *SqlInfo) IsDdl() bool {
	if this.SqlType == CsqlTypeAlterTable || this.SqlType == CsqlTypeAlterDatabase {
		return true
	}
	if this.SqlType == CsqlTypeCreateTable || this.SqlType == CsqlTypeCreateDatabase {
		return true
	}
	if this.SqlType == CsqlTypeCreateIndex || this.SqlType == CsqlTypeDropTable {
		return true
	}
	if this.SqlType == CsqlTypeDropDatabase || this.SqlType == CsqlTypeDropIndex {
		return true
	}
	if this.SqlType == CsqlTypeTruncateTable || this.SqlType == CsqlTypeRenameTable {
		return true
	}
	return false
}

func (this *SqlInfo) IsDatabaseDDL() bool {
	if this.SqlType == CsqlTypeCreateDatabase || this.SqlType == CsqlTypeDropDatabase || this.SqlType == CsqlTypeAlterDatabase {
		return true
	} else {
		return false
	}
}

func (this *SqlInfo) GetTablesFromTbRef(tbRef *ast.Join) error {
	if tbRef.Left != nil {
		tbSrc, ok := tbRef.Left.(*ast.TableSource)
		if !ok {
			newTbRef, ok := tbRef.Left.(*ast.Join)
			if !ok {
				return errors.Errorf("error to convert Left into *ast.Join")
			} else {
				this.GetTablesFromTbRef(newTbRef)
			}
		} else {
			tbName, ok := tbSrc.Source.(*ast.TableName)
			if !ok {
				return errors.Errorf("error to convert Source into *ast.TableName")
			}
			this.Tables = append(this.Tables, DbTable{Table: tbName.Name.O, Database: tbName.Schema.O})
		}

	}
	if tbRef.Right != nil {
		tbSrc, ok := tbRef.Right.(*ast.TableSource)
		if !ok {
			newTbRef, ok := tbRef.Right.(*ast.Join)
			if !ok {
				return errors.Errorf("error to convert Left into *ast.Join")
			} else {
				this.GetTablesFromTbRef(newTbRef)
			}
		} else {
			tbName, ok := tbSrc.Source.(*ast.TableName)
			if !ok {
				return errors.Errorf("error to convert Source into *ast.TableName")
			}
			this.Tables = append(this.Tables, DbTable{Table: tbName.Name.O, Database: tbName.Schema.O})
		}
	}
	return nil
}

func ParseSqlsForSqlInfo(sParser *parser.Parser, sqlStr string, usedatabase string) ([]*SqlInfo, string, error) {
	var (
		result     []*SqlInfo
		lastUsedDb string = ""
	)
	sts, _, err := sParser.Parse(sqlStr, "", "")
	if err != nil {
		return nil, lastUsedDb, errors.Annotatef(err, "error to parse sql: %s", sqlStr)
	}

	for _, oneS := range sts {
		oneResult := &SqlInfo{SqlStr: oneS.Text()}
		switch oneSt := oneS.(type) {
		case *ast.UseStmt:
			// in binlog, query event may be composed of use database, DML/DDL sql
			usedatabase = oneSt.DBName
			lastUsedDb = oneSt.DBName
			oneResult.UseDatabase = usedatabase
		case *ast.InsertStmt:
			if oneSt.IsReplace {
				oneResult.SqlType = CsqlTypeReplace
			} else {
				oneResult.SqlType = CsqlTypeInsert
			}
			tbSrc, ok := oneSt.Table.TableRefs.Left.(*ast.TableSource)
			if !ok {
				return nil, lastUsedDb, errors.Errorf("error to convert Table.TableRefs.Left into *ast.TableSource: %s", oneS.Text())
			}
			tbName, ok := tbSrc.Source.(*ast.TableName)
			if !ok {
				return nil, lastUsedDb, errors.Errorf("error to convert Source into *ast.TableName: %s", oneS.Text())
			}
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: tbName.Schema.O, Table: tbName.Name.O})

		case *ast.DeleteStmt:
			oneResult.SqlType = CsqlTypeDelete
			err = oneResult.GetTablesFromTbRef(oneSt.TableRefs.TableRefs)
			if err != nil {
				return nil, lastUsedDb, err
			}
		case *ast.UpdateStmt:
			oneResult.SqlType = CsqlTypeUpdate
			err = oneResult.GetTablesFromTbRef(oneSt.TableRefs.TableRefs)
			if err != nil {
				return nil, lastUsedDb, err
			}
		case *ast.LoadDataStmt:
			oneResult.SqlType = CsqlTypeLoadData
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneSt.Table.Schema.O, Table: oneSt.Table.Name.O})

		case *ast.AlterTableStmt:
			oneResult.SqlType = CsqlTypeAlterTable
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneSt.Table.Schema.O, Table: oneSt.Table.Name.O})
			for _, spec := range oneSt.Specs {
				if spec.Tp == ast.AlterTableRenameTable {
					if usedatabase != "" && spec.NewTable.Schema.O == "" {
						oneResult.Tables = append(oneResult.Tables, DbTable{Database: usedatabase, Table: spec.NewTable.Name.O})
					} else {
						oneResult.Tables = append(oneResult.Tables, DbTable{Database: spec.NewTable.Schema.O, Table: spec.NewTable.Name.O})
					}
				}
			}

		case *ast.CreateTableStmt:
			oneResult.SqlType = CsqlTypeCreateTable
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneSt.Table.Schema.O, Table: oneSt.Table.Name.O})
		case *ast.CreateDatabaseStmt:
			oneResult.SqlType = CsqlTypeCreateDatabase
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneSt.Name})
		case *ast.CreateIndexStmt:
			oneResult.SqlType = CsqlTypeCreateIndex
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneSt.Table.Schema.O, Table: oneSt.Table.Name.O})
		case *ast.DropTableStmt:
			oneResult.SqlType = CsqlTypeDropTable
			for _, oneDrop := range oneSt.Tables {
				if usedatabase != "" && oneDrop.Schema.O == "" {
					oneResult.Tables = append(oneResult.Tables, DbTable{Database: usedatabase, Table: oneDrop.Name.O})
				} else {
					oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneDrop.Schema.O, Table: oneDrop.Name.O})
				}

			}
		case *ast.DropDatabaseStmt:
			oneResult.SqlType = CsqlTypeDropDatabase
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneSt.Name})

		case *ast.DropIndexStmt:
			oneResult.SqlType = CsqlTypeDropIndex
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneSt.Table.Schema.O, Table: oneSt.Table.Name.O})
		case *ast.TruncateTableStmt:
			oneResult.SqlType = CsqlTypeTruncateTable
			oneResult.Tables = append(oneResult.Tables, DbTable{Database: oneSt.Table.Schema.O, Table: oneSt.Table.Name.O})
		case *ast.RenameTableStmt:
			oneResult.SqlType = CsqlTypeRenameTable
			for _, oneRename := range oneSt.TableToTables {
				newdb := usedatabase
				if oneRename.OldTable.Schema.O != "" {
					newdb = oneRename.OldTable.Schema.O
				}
				olddb := usedatabase
				if oneRename.NewTable.Schema.O != "" {
					olddb = oneRename.NewTable.Schema.O
				}
				oneResult.Tables = append(oneResult.Tables, DbTable{Database: newdb, Table: oneRename.OldTable.Name.O},
					DbTable{Database: olddb, Table: oneRename.NewTable.Name.O})
			}

		}
		if len(oneResult.Tables) > 0 {

			if usedatabase != "" && oneResult.Tables[0].Database == "" {
				oneResult.Tables[0].Database = usedatabase
			}
			if usedatabase != "" && oneResult.UseDatabase == "" {
				oneResult.UseDatabase = usedatabase
			}
			result = append(result, oneResult)
		}
	}
	return result, lastUsedDb, nil
}
