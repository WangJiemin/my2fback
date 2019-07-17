package dsql

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/toolkits/slice"

	"github.com/juju/errors"
	"github.com/orcaman/concurrent-map"
	"github.com/WangJiemin/jamintools/constvar"
	"github.com/WangJiemin/jamintools/dbes"
)

type CurrentDatabase struct {
	Lock           *sync.RWMutex
	UseDatabase    string
	CreateDatabase string
	EventTime      time.Time
	Sql            string
	BinPos         *BinlogPosition
}

func (this *CurrentDatabase) Database() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	if this.UseDatabase != "" {
		return this.UseDatabase
	}
	if this.CreateDatabase != "" {
		return this.CreateDatabase
	}
	return ""
}

func (this *CurrentDatabase) String() string {
	return fmt.Sprintf("Usedatabase=%s CreateDatabase=%s EventTime=%s BinPos=%s Sql=%s",
		this.UseDatabase, this.CreateDatabase, this.EventTime.Format(constvar.DATETIME_FORMAT_NOSPACE),
		this.BinPos.String(), this.Sql)
}

type TableSyncRegStr struct {
	DatabaseRegStr string `toml:"database_regexp"`
	TableRegStr    string `toml:"table_regexp"`
}

type TableSyncRegStrAll struct {
	Includes []TableSyncRegStr
	Excludes []TableSyncRegStr
}

func (this TableSyncRegStrAll) GetTablesSyncReg() (*TableFilter, error) {
	tbF := &TableFilter{}
	for _, oneR := range this.Includes {
		dbReg, err := regexp.Compile(oneR.DatabaseRegStr)
		if err != nil {
			return nil, errors.Annotatef(err, "error to complie include database regexp: %s", oneR.DatabaseRegStr)
		}
		tbReg, err := regexp.Compile(oneR.TableRegStr)
		if err != nil {
			return nil, errors.Annotatef(err, "error to complie include table regexp: %s", oneR.TableRegStr)
		}
		tbF.Includes = append(tbF.Includes, &TableFilterReg{DatabaseReg: dbReg, TableReg: tbReg})
	}

	for _, oneR := range this.Excludes {
		dbReg, err := regexp.Compile(oneR.DatabaseRegStr)
		if err != nil {
			return nil, errors.Annotatef(err, "error to complie exclude database regexp: %s", oneR.DatabaseRegStr)
		}
		tbReg, err := regexp.Compile(oneR.TableRegStr)
		if err != nil {
			return nil, errors.Annotatef(err, "error to complie exclude table regexp: %s", oneR.TableRegStr)
		}
		tbF.Excludes = append(tbF.Excludes, &TableFilterReg{DatabaseReg: dbReg, TableReg: tbReg})
	}
	if len(tbF.Includes) == 0 && len(tbF.Excludes) == 0 {
		return nil, nil
	}
	return tbF, nil

}

// both of  DatabaseReg and TableReg must be not null
type TableFilterReg struct {
	DatabaseReg *regexp.Regexp
	TableReg    *regexp.Regexp
}

func (this *TableFilterReg) IsTableMatch(dbname, tbname string) bool {
	if this.DatabaseReg.MatchString(dbname) && this.TableReg.MatchString(tbname) {
		return true
	} else {
		return false
	}
}

type TableFilter struct {
	Includes []*TableFilterReg
	Excludes []*TableFilterReg
}

func (this *TableFilter) IsTableTarget(dbname, tbname string) bool {
	var oneReg *TableFilterReg
	//match any exclude, not target
	for _, oneReg = range this.Excludes {
		if oneReg.IsTableMatch(dbname, tbname) {
			return false
		}
	}
	//match any include, target
	for _, oneReg = range this.Includes {
		if oneReg.IsTableMatch(dbname, tbname) {
			return true
		}
	}
	return true

}

type BinFilePosition struct {
	Name string
	Pos  uint32
}

func (this *BinFilePosition) Copy() *BinFilePosition {
	return &BinFilePosition{Name: this.Name, Pos: this.Pos}
}

func (p *BinFilePosition) Compare(o *BinFilePosition) int {

	if p.Name > o.Name {
		return 1
	} else if p.Name < o.Name {
		return -1
	} else {
		if p.Pos > o.Pos {
			return 1
		} else if p.Pos < o.Pos {
			return -1
		} else {
			return 0
		}
	}
}

func (p *BinFilePosition) String() string {
	return fmt.Sprintf("(%s, %d)", p.Name, p.Pos)
}

func (this *BinFilePosition) Valid() bool {
	if this.Name != "" && this.Pos > 0 {
		return true
	} else {
		return false
	}
}

type GtidStr struct {
	Uuid  string
	TrxId int64
}

func (this *GtidStr) Copy() *GtidStr {
	return &GtidStr{
		Uuid:  this.Uuid,
		TrxId: this.TrxId,
	}
}

func (this *GtidStr) String() string {
	return fmt.Sprintf("(%s, %d)", this.Uuid, this.TrxId)
}

func (this *GtidStr) Valid() bool {
	if this.Uuid != "" && this.TrxId > 0 {
		return true
	} else {
		return false
	}
}

type BinlogPosition struct {
	File *BinFilePosition
	Gtid *GtidStr
}

func (this *BinlogPosition) Copy() *BinlogPosition {
	return &BinlogPosition{File: this.File.Copy(), Gtid: this.Gtid.Copy()}
}

func (this *BinlogPosition) String() string {
	return fmt.Sprint("GTID:%s Binlog:%s", this.Gtid.String(), this.File.String())
}

type ColDef struct {
	Name          string
	TypeName      string
	TypeCode      byte
	Notnull       bool
	Unsigned      bool
	AutoIncrement bool
	Elements      []string // Elems is the element list for enum and set type
}

func (this *ColDef) Copy() *ColDef {

	return &ColDef{
		Name:          this.Name,
		TypeName:      this.TypeName,
		TypeCode:      this.TypeCode,
		Notnull:       this.Notnull,
		Unsigned:      this.Unsigned,
		AutoIncrement: this.AutoIncrement,
		Elements:      append([]string{}, this.Elements...),
	}
}

type KeyDef struct {
	Name          string
	ColumnNames   []string
	ColumnIndices []int
}

func NewEmptyKeyDef() *KeyDef {
	return &KeyDef{
		ColumnNames:   []string{},
		ColumnIndices: []int{},
	}
}

func (this *KeyDef) Copy() *KeyDef {
	oneKey := &KeyDef{Name: this.Name}

	nameArr := make([]string, len(this.ColumnNames))
	copy(nameArr, this.ColumnNames)
	oneKey.ColumnNames = nameArr

	indexArr := make([]int, len(this.ColumnIndices))
	copy(indexArr, this.ColumnIndices)
	oneKey.ColumnIndices = indexArr

	return oneKey
}

type SyncConf struct {
	Name string // the table name in synced database, ie ES index name

	IfHasSyncFields bool
	FieldNames      []string //sync only this fields other database like ES
	FieldIdxs       []int    // sync only this fields other database like ES

	IfHasSyncId  bool
	IdFieldNames []string // the unique id in synced database, ie ES doc id
	IdFieldIdxs  []int    // the unique id in synced database, ie ES doc id

	SkipSyncInsert bool
	SkipSyncUpdate bool
	SkipSyncDelete bool
}

func (this *SyncConf) SetIfSync() {
	if len(this.FieldIdxs) > 0 {
		this.IfHasSyncFields = true
	} else {
		this.IfHasSyncFields = false
	}
	if len(this.IdFieldIdxs) > 0 {
		this.IfHasSyncId = true
	} else {
		this.IfHasSyncId = false
	}
}

func NewEmptySyncField() *SyncConf {
	return &SyncConf{
		FieldNames:   []string{},
		FieldIdxs:    []int{},
		IdFieldNames: []string{},
		IdFieldIdxs:  []int{},
	}
}

func (this *SyncConf) Copy() *SyncConf {
	return &SyncConf{
		Name:           this.Name,
		FieldNames:     append([]string{}, this.FieldNames...),
		FieldIdxs:      append([]int{}, this.FieldIdxs...),
		IdFieldNames:   append([]string{}, this.IdFieldNames...),
		IdFieldIdxs:    append([]int{}, this.IdFieldIdxs...),
		SkipSyncInsert: this.SkipSyncInsert,
		SkipSyncDelete: this.SkipSyncDelete,
		SkipSyncUpdate: this.SkipSyncUpdate,
	}
}

type TableDef struct {
	Columns    []*ColDef
	PrimaryKey *KeyDef
	UniqueKeys map[string]*KeyDef
	SyncInfo   *SyncConf
}

func NewEmptyTableDef() *TableDef {
	return &TableDef{
		Columns:    []*ColDef{},
		UniqueKeys: map[string]*KeyDef{},
		PrimaryKey: NewEmptyKeyDef(),
		SyncInfo:   NewEmptySyncField(),
	}
}

func (this *TableDef) GetAllFieldIndices() []int {
	var arr []int
	for i := range this.Columns {
		arr = append(arr, i)
	}
	return arr
}

func (this *TableDef) GetAllFieldNames() []string {
	var arr []string
	for _, oneC := range this.Columns {
		arr = append(arr, oneC.Name)
	}
	return arr
}

// return -1 if not found
func (this *TableDef) GetColIndxByName(colName string) int {
	for idx, oneCol := range this.Columns {
		if oneCol.Name == colName {
			return idx
		}
	}
	return -1
}

func (this *TableDef) Copy() *TableDef {

	tb := &TableDef{
		Columns:    make([]*ColDef, len(this.Columns)),
		PrimaryKey: this.PrimaryKey.Copy(),
		UniqueKeys: map[string]*KeyDef{},
		SyncInfo:   this.SyncInfo.Copy(),
	}

	for idx := range this.Columns {
		tb.Columns[idx] = this.Columns[idx].Copy()
	}

	for key := range this.UniqueKeys {
		tb.UniqueKeys[key] = this.UniqueKeys[key].Copy()
	}

	return tb

}

// in excludeFields, exclude the field
// in includeFields, include the field
// other case, include the field
// return fieldNames and fieldIndices
func (this *TableDef) GetSyncFields(includeFields, excludeFields []string) ([]string, []int) {
	var (
		fieldNames []string
		fieldIdxs  []int
		incLen     int = len(includeFields)
		excLen     int = len(excludeFields)
	)
	for idx, oneF := range this.Columns {
		if excLen > 0 && slice.ContainsString(excludeFields, oneF.Name) {
			continue
		}
		if incLen > 0 {
			if slice.ContainsString(includeFields, oneF.Name) {
				fieldNames = append(fieldNames, oneF.Name)
				fieldIdxs = append(fieldIdxs, idx)
			}
			continue
		}
		fieldNames = append(fieldNames, oneF.Name)
		fieldIdxs = append(fieldIdxs, idx)
	}
	return fieldNames, fieldIdxs
}

// return nil if any field not in the table
func (this *TableDef) GetSyncIdFieldIndex(idFields []string) []int {
	var (
		idArr []int
		idx   int
	)
	for _, f := range idFields {
		idx = this.GetColIndxByName(f)
		if idx < 0 {
			return nil
		}
		idArr = append(idArr, idx)
	}
	return idArr
}

//get EsIdFields, no specific fields set, use primary, unique key field
//if has primary key, return it.
//if has unique keys, sorted by keyname, return the first unique key
//otherwise, return nil
func (this *TableDef) GetDefaultEsIdFields() (string, []string, []int) {
	if this.PrimaryKey != nil && len(this.PrimaryKey.ColumnIndices) > 0 {
		return CprimaryKeyName, append([]string{}, this.PrimaryKey.ColumnNames...), append([]int{}, this.PrimaryKey.ColumnIndices...)
	}
	if len(this.UniqueKeys) > 0 {
		var keArr []string
		for k := range this.UniqueKeys {
			keArr = append(keArr, k)
		}
		sort.Strings(keArr)
		return keArr[0], append([]string{}, this.UniqueKeys[keArr[0]].ColumnNames...), append([]int{}, this.UniqueKeys[keArr[0]].ColumnIndices...)
	} else {
		return "", nil, nil
	}
}

// set field info
// return if match
func (this *TableDef) SetSyncInfo(dbname string, tbname string, filters *dbes.TableToEsIndexConfAll, ifOnlyField bool) bool {
	var (
		ifMatch bool = false
		idxName string
	)

	for _, oneF := range filters.EsIndexSyncCfg {
		idxName = oneF.GetEsIndexName(dbname, tbname)
		if idxName == "" {
			// not match
			continue
		}
		ifMatch = true
		this.SyncInfo.FieldNames, this.SyncInfo.FieldIdxs = this.GetSyncFields(oneF.IncludeFields, oneF.ExcludeFields)
		this.SyncInfo.IdFieldIdxs = this.GetSyncIdFieldIndex(oneF.EsIdFields)
		if len(this.SyncInfo.IdFieldIdxs) > 0 {
			this.SyncInfo.IdFieldNames = append([]string{}, oneF.EsIdFields...)
		} else {
			this.SyncInfo.IdFieldNames = nil
		}
		if !ifOnlyField {
			this.SyncInfo.Name = idxName
			this.SyncInfo.SkipSyncInsert = oneF.Setting.SkipSyncInsert
			this.SyncInfo.SkipSyncDelete = oneF.Setting.SkipSyncDelete
			this.SyncInfo.SkipSyncUpdate = oneF.Setting.SkipSyncUpdate
		}
		break
	}
	if !ifMatch {
		// get default
		this.SyncInfo.FieldIdxs = this.GetAllFieldIndices()
		this.SyncInfo.FieldNames = this.GetAllFieldNames()
		_, this.SyncInfo.IdFieldNames, this.SyncInfo.IdFieldIdxs = this.GetDefaultEsIdFields()
		if !ifOnlyField {
			this.SyncInfo.Name = filters.Setting.GetEsIndexName(dbname, tbname)
			this.SyncInfo.SkipSyncDelete = filters.Setting.SkipSyncDelete
			this.SyncInfo.SkipSyncInsert = filters.Setting.SkipSyncInsert
			this.SyncInfo.SkipSyncUpdate = filters.Setting.SkipSyncUpdate
		}
	}
	this.SyncInfo.SetIfSync()
	return ifMatch
}

type TableId struct {
	SchemaId  uint64
	Database  string // lowercase
	TableName string // lowercase
}

func (this *TableId) Copy() *TableId {
	return &TableId{SchemaId: this.SchemaId, Database: this.Database, TableName: this.TableName}
}

func (this *TableId) FullName() string {
	return fmt.Sprintf("%s.%s", this.Database, this.TableName)
}

func (this *TableId) String() string {
	return fmt.Sprintf("%s.%s(schemaid=%d)", this.Database, this.TableName, this.SchemaId)
}

type TableInfo struct {
	Table *TableId
	Def   *TableDef
}

func (this *TableInfo) Copy() *TableInfo {
	return &TableInfo{Table: this.Table.Copy(), Def: this.Def.Copy()}
}

func NewEmptyTableInfo() *TableInfo {
	return &TableInfo{
		Table: new(TableId),
		Def:   NewEmptyTableDef(),
	}
}

type TableSchemaVersion struct {
	Id              uint64
	GetSrc          uint8
	HostId          uint64
	EventTime       time.Time
	Ddl             string
	Info            *TableInfo
	BinPos          *BinlogPosition
	IfHasDef        bool
	IfHasPrimaryKey bool
	IfHasUniqueKey  bool
}

func (this *TableSchemaVersion) Copy() *TableSchemaVersion {
	tb := new(TableSchemaVersion)
	tb.Id = this.Id
	tb.GetSrc = this.GetSrc
	tb.HostId = this.HostId
	tb.EventTime = this.EventTime
	tb.Ddl = this.Ddl
	tb.Info = this.Info.Copy()
	tb.BinPos = this.BinPos.Copy()
	tb.IfHasDef = this.IfHasDef
	tb.IfHasPrimaryKey = this.IfHasPrimaryKey
	tb.IfHasUniqueKey = this.IfHasUniqueKey
	return tb
}

func (this *TableSchemaVersion) JsonString() (string, error) {
	jbytes, err := json.Marshal(this)
	if err != nil {
		return "", errors.Annotate(err, "error to marshal into json")
	}
	return string(jbytes), nil
}

func (this *TableSchemaVersion) SetIfHasPrimaryUniqueKey() {
	if len(this.Info.Def.PrimaryKey.ColumnNames) > 0 {
		this.IfHasPrimaryKey = true
	}
	if len(this.Info.Def.UniqueKeys) > 0 {
		this.IfHasUniqueKey = true
	}
}

type TableCache struct {
	TableDefs cmap.ConcurrentMap // key: db.tb, value: TableSchemaVersion
}

func (this *TableCache) GetTableScheme(tbFull string, ifCopy bool) (*TableSchemaVersion, error) {
	if !this.TableDefs.Has(tbFull) {
		return nil, errors.NotFoundf("%s not found in TableCache", tbFull)
	}

	val, ok := this.TableDefs.Get(tbFull)
	if !ok {
		return nil, errors.Errorf("TableCache.TableDefs[%s] exists, but fail to get it", tbFull)
	}
	tbSchema, ok := val.(*TableSchemaVersion)
	if !ok {
		return nil, errors.Errorf("fail to convert TableCache.TableDefs[%s] into *TableSchemaVersion", tbFull)
	}
	if ifCopy {
		return tbSchema.Copy(), nil
	} else {
		return tbSchema, nil
	}

}
