package dsql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/juju/errors"

	_ "github.com/go-sql-driver/mysql"

	"github.com/WangJiemin/jamintools/constvar"
	"github.com/WangJiemin/jamintools/mystr"
)

const (
	CsqlGetTableColumnsMulti string = `
	select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE,IS_NULLABLE, EXTRA 
	from information_schema.columns
	where TABLE_SCHEMA in (?) and TABLE_NAME in (?)
	order by TABLE_SCHEMA asc, TABLE_NAME asc, ORDINAL_POSITION asc
	`
	CsqlGetTableColumnsSingle string = `
	select COLUMN_NAME, DATA_TYPE, COLUMN_TYPE,IS_NULLABLE, EXTRA 
	from information_schema.columns
	where TABLE_SCHEMA=? and TABLE_NAME=?
	order by ORDINAL_POSITION asc
	`
	CsqlGetPriUniqueKeysMulti string = `
	select k.table_schema, k.table_name, k.CONSTRAINT_NAME, k.COLUMN_NAME, c.CONSTRAINT_TYPE
	from information_schema.TABLE_CONSTRAINTS as c inner join information_schema.KEY_COLUMN_USAGE as k on
	c.CONSTRAINT_NAME = k.CONSTRAINT_NAME and c.table_schema = k.table_schema and c.table_name=k.table_name
	where c.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE') and c.table_schema in (?) and c.table_name in (?)
	order by k.table_schema asc, k.table_name asc, k.CONSTRAINT_NAME asc, k.ORDINAL_POSITION asc
	`

	CsqlGetPriUniqueKeysSingle string = `
	select k.CONSTRAINT_NAME, k.COLUMN_NAME, c.CONSTRAINT_TYPE
	from information_schema.TABLE_CONSTRAINTS as c inner join information_schema.KEY_COLUMN_USAGE as k on
	c.CONSTRAINT_NAME = k.CONSTRAINT_NAME and c.table_schema = k.table_schema and c.table_name=k.table_name
	where c.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE') and c.table_schema=? and c.table_name=?
	order by k.table_schema asc, k.table_name asc, k.CONSTRAINT_NAME asc, k.ORDINAL_POSITION asc
	`
)

type MysqlAddr struct {
	Host        string
	Port        int
	MyId        uint64
	OldMasterId uint64
}

func (this *MysqlAddr) Equal(h *MysqlAddr) bool {
	if this.Host == h.Host && this.Port == h.Port && this.MyId == h.MyId {
		return true
	}
	return false
}

func (this *MysqlAddr) SameMysqlAddr(host string, port int) bool {
	if this.Host == host && this.Port == port {
		return true
	}
	return false
}

func (this *MysqlAddr) String() string {
	return fmt.Sprintf("host=%s port=%d host_id=%d org_master_id=%d", this.Host, this.Port, this.MyId, this.OldMasterId)
}

func (this *MysqlAddr) IsValid() bool {
	if this.Host != "" && this.Port > 0 && this.MyId > 0 {
		return true
	}
	return false
}

func (this *MysqlAddr) IsAddrValid() bool {
	if this.Host != "" && this.Port > 0 {
		return true
	}
	return false
}

func (this *MysqlAddr) Copy() *MysqlAddr {
	return &MysqlAddr{
		Host:        this.Host,
		Port:        this.Port,
		MyId:        this.MyId,
		OldMasterId: this.OldMasterId,
	}
}

// if no result, return empty  MysqlAddr, Host="" and Port=0
func GetLastMasterHost(db *sql.DB) (*MysqlAddr, error) {
	var (
		queryStr string     = "select host_id, host, port from master_host limit 1"
		addr     *MysqlAddr = &MysqlAddr{}
		host     string
		port     int
		host_id  uint64
	)
	row := db.QueryRow(queryStr)
	err := row.Scan(&host_id, &host, &port)
	if err == sql.ErrNoRows {
		return addr, nil
	} else if err != nil {
		return addr, errors.Annotate(err, "error to query last master addr from mysql: "+queryStr)
	} else {
		addr.Host = host
		addr.Port = port
		addr.MyId = host_id
		return addr, nil
	}
}

// master_host can only have one row
func UpdateMasterHost(db *sql.DB, oldMaster, newMaster *MysqlAddr) error {
	var (
		queryStr string
	)
	if oldMaster.Host == newMaster.Host && oldMaster.Port == newMaster.Port {
		return nil
	}
	if oldMaster.Host != "" && oldMaster.Port != 0 {
		queryStr = "update master_host set host=?, port=?"
	} else {
		queryStr = "insert into master_host (host, port) values (?, ?)"

	}
	_, err := db.Exec(queryStr, newMaster.Host, newMaster.Port)

	if err != nil {
		return errors.Annotate(err, "error to execute query to update master info: "+queryStr)
	} else {
		return nil
	}
}

type HostId struct {
	Self       *MysqlAddr
	OldMasters []*MysqlAddr
}

func (this *HostId) String() string {
	result := this.Self.String()
	for i, oneM := range this.OldMasters {
		result += "\n" + strings.Repeat("-", i+1) + " " + oneM.String()
	}
	return result
}

func (this *HostId) GetMyOldMasters(db *sql.DB) error {
	var (
		host string
		port int

		masterId          uint64
		oldMasterId       uint64
		err               error
		queryStrOldMaster string = "select host, port, org_master_id from mysql_host where host_id=?"
	)

	if this.Self.OldMasterId <= 0 {
		return nil
	} else {
		oldMasterId = this.Self.OldMasterId
	}
	this.OldMasters = []*MysqlAddr{}
	// get old masters
	for {
		oneRow := db.QueryRow(queryStrOldMaster, oldMasterId)
		err = oneRow.Scan(&host, &port, &masterId)
		if err != nil {
			return errors.Annotatef(err, "error to query old master info for host_id=%d: %s", oldMasterId, queryStrOldMaster)
		}
		this.OldMasters = append(this.OldMasters, &MysqlAddr{Host: host, Port: port, MyId: oldMasterId, OldMasterId: masterId})
		if masterId <= 0 {
			break
		}
		oldMasterId = masterId
	}
	return nil

}

// get host ids
// donnot use it, (host, port) in table mysql_host is not unique
func (this *HostId) GetHostIds(db *sql.DB, ifGetOldMaster bool) error {
	var (
		host              string
		port              int
		hostId            uint64
		masterId          uint64
		oldMasterId       uint64
		err               error
		queryStrSelf      string = "select host_id, org_master_id from mysql_host where host=? and port=?"
		queryStrOldMaster string = "select host, port, org_master_id from mysql_host where host_id=?"
	)

	oneRow := db.QueryRow(queryStrSelf, this.Self.Host, this.Self.Port)
	err = oneRow.Scan(&hostId, &oldMasterId)
	if err != nil {
		return errors.Annotatef(err, "error to query host_id for host=%s port=%d: %s", host, port, queryStrSelf)
	}
	this.Self.MyId = hostId
	this.Self.OldMasterId = oldMasterId
	if !ifGetOldMaster {
		return nil
	}

	if oldMasterId <= 0 {
		return nil
	}
	this.OldMasters = []*MysqlAddr{}
	// get old masters
	for {
		oneRow := db.QueryRow(queryStrOldMaster, oldMasterId)
		err = oneRow.Scan(&host, &port, &masterId)
		if err != nil {
			return errors.Annotatef(err, "error to query old master info for host_id=%d: %s", oldMasterId, queryStrOldMaster)
		}
		this.OldMasters = append(this.OldMasters, &MysqlAddr{Host: host, Port: port, MyId: oldMasterId, OldMasterId: masterId})
		if masterId <= 0 {
			break
		}
		oldMasterId = masterId
	}
	return nil

}

// donnot use it
func (this *HostId) ProcessHosts(db *sql.DB, newMaster *MysqlAddr) error {
	var (
		err error
	)
	// get host info
	err = this.GetHostIds(db, true)
	if err != nil {
		return err
	}
	// update master info
	err = this.UpdateAndInsertNewMaster(db, newMaster)
	if err != nil {
		return err
	}

	// update this since we get a new master
	if this.Self.Host != newMaster.Host || this.Self.Port != newMaster.Port {
		this.OldMasters = append([]*MysqlAddr{this.Self}, this.OldMasters...)
		this.Self = newMaster
	}
	return nil

}

//insert new host
func (this *HostId) InsertNewHost(db *sql.DB) error {
	var (
		queryStr string = "insert into mysql_host (host, port, org_master_id) values (?, ?, ?)"
	)
	result, err := db.Exec(queryStr, this.Self.Host, this.Self.Port, this.Self.OldMasterId)
	if err != nil {
		return errors.Annotatef(err, "error to insert new host %s : %s", this.Self.String(), queryStr)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return errors.Annotate(err, "error to get last insert id after insert new host: "+this.Self.String())
	}
	this.Self.MyId = uint64(id)
	return nil
}

// update and insert new master host, get new master hostid and org_master_id
func (this *HostId) UpdateAndInsertNewMaster(db *sql.DB, newMaster *MysqlAddr) error {
	var (
		queryStr string
		err      error
		result   sql.Result
		ifOk     bool = false
		id       int64
		trx      *sql.Tx
	)

	// same master, no need to update
	if this.Self.Host == newMaster.Host && this.Self.Port == newMaster.Port {
		return errors.Errorf("new master has the same addr as old master")
	}
	// no new master, no need to update
	if newMaster.Host == "" || newMaster.Port == 0 {
		return errors.Errorf("new master addr is invalid")
	}

	trx, err = db.Begin()
	if err != nil {
		trx.Rollback()
		return errors.Annotate(err, "error to start a transaction")
	}

	// try free times
	for i := 0; i < 3; i++ {
		ifOk = false
		//first delete record to make sure only one record exists, don't truncate since we need to get unique host_id
		queryStr = "delete from master_host"
		_, err = trx.Exec(queryStr)
		if err != nil {
			continue
			//trx.Rollback()
			//return errors.Annotate(err, "error to delete master record: "+queryStr)
		}

		queryStr = "insert into master_host (host, port) values (?, ?)"

		result, err = trx.Exec(queryStr, newMaster.Host, newMaster.Port)
		if err != nil {
			continue
			//trx.Rollback()
			//return errors.Annotate(err, "error to execute query to insert new master "+newMaster.String()+": "+queryStr)

		}
		id, err = result.LastInsertId()
		if err != nil {
			continue
			//trx.Rollback()
			//return errors.Annotate(err, "error to get last insert id after insert new master: "+newMaster.String())
		}
		newMaster.OldMasterId = this.Self.MyId
		newMaster.MyId = uint64(id)
		queryStr = "insert into mysql_host (host_id, host, port, org_master_id) values (?, ?, ?, ?)"
		_, err = trx.Exec(queryStr, newMaster.MyId, newMaster.Host, newMaster.Port, this.Self.MyId)
		if err != nil {
			continue
			//trx.Rollback()
			//return errors.Annotatef(err, "error to insert new host %s : %s", newMaster.String(), queryStr)
		}
		ifOk = true
		break

	}
	if ifOk {
		trx.Commit()
		return nil
	} else {
		trx.Rollback()
		newMaster.OldMasterId = 0
		newMaster.MyId = 0
		return errors.Annotate(err, "error to update new master info "+newMaster.String()+" : "+queryStr)
	}

}

func (this *HostId) GetAllMasterIds() []uint64 {
	var (
		ids []uint64 = []uint64{this.Self.MyId}
	)
	for _, oneMaster := range this.OldMasters {
		ids = append(ids, oneMaster.MyId)
	}
	return ids

}

// get all table name and schema id
func GetAllTableSchemaIds(db *sql.DB, tbFilter *TableFilter) ([]*TableId, error) {
	var (
		err      error
		queryStr string = "select schema_id, dbname, tbname from schema_src"
		schemaId uint64
		dbname   string
		tbname   string
		tables   []*TableId
	)
	rows, err := db.Query(queryStr)
	if err != nil {
		return nil, errors.Annotate(err, "error to query schema_src: "+queryStr)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&schemaId, &dbname, &tbname)
		if err != nil {
			return nil, errors.Annotate(err, "error to get query result: "+queryStr)
		}
		if tbFilter.IsTableTarget(dbname, tbname) {
			tables = append(tables, &TableId{SchemaId: schemaId, Database: dbname, TableName: tbname})
		}
	}
	return tables, nil
}

// get all table names, in lower case
func GetAllTableNamesFromMaster(db *sql.DB, tbFilter *TableFilter, excludeInternalDatabases []string) ([]*TableId, error) {
	var (
		err      error
		queryStr string = "select table_schema, table_name from information_schema.tables where where table_type='BASE TABLE'"
		results  []*TableId
		tbname   string
		dbname   string
	)
	rows, err := db.Query(queryStr)
	if err != nil {
		return nil, errors.Annotate(err, "error to get tables from master: "+queryStr)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&dbname, &tbname)
		if err != nil {
			return nil, errors.Annotate(err, "error to get query result to get tables from master: "+queryStr)
		}
		dbname = strings.ToLower(dbname)
		tbname = strings.ToLower(tbname)
		if tbFilter.IsTableTarget(dbname, tbname) {
			results = append(results, &TableId{Database: dbname, TableName: tbname})
		}
	}
	return results, nil

}

func (this *TableDef) UnmarshalFromString(tbDefStr string) error {
	tbDefStr = strings.TrimSpace(tbDefStr)
	if tbDefStr == "" {
		return errors.Errorf("no json string to unmarshal into TableDef")
	}
	err := json.Unmarshal([]byte(tbDefStr), this)
	if err != nil {
		return errors.Annotate(err, "error to unmarshal into TableDef:\n"+tbDefStr)
	} else {
		return nil
	}
}

//db url must have parseTime=true
func (this *TableSchemaVersion) GetTableSchema(db *sql.DB, myHost *HostId) error {
	var (
		err             error
		queryStr        string
		oneRow          *sql.Row
		id              uint64
		getSrc          uint8
		hostId          uint64
		versionTime     time.Time
		binlog          string
		binpos          uint32
		uuid            string
		trxid           int64
		ddl             string
		tbDefStr        string
		ifFound         bool   = false
		eventTimeString string = this.EventTime.Format(constvar.DATETIME_FORMAT_FRACTION)
	)

	// try to find in binlog by gtid on same master
	if this.BinPos.Gtid.Valid() {
		queryStr = `select id, get_src, host_id, event_time, binlog, binpos, uuid, trxid, ddl, tbldef 
		from table_schema  where 
		schema_id=? and get_src=1 and host_id=? and uuid=? and trxid < ? 
		order by trxid desc limit 1
		`
		oneRow = db.QueryRow(queryStr, this.Info.Table.SchemaId, myHost.Self.MyId, this.BinPos.Gtid.Uuid, this.BinPos.Gtid.TrxId)
		err = oneRow.Scan(&id, &getSrc, &hostId, &versionTime, &binlog, &binpos, &uuid, &trxid, &ddl, &tbDefStr)
		if err != nil {
			if err != sql.ErrNoRows {
				return errors.Annotatef(err, "error to get table def %s %s : %s", myHost.Self.String(), this.BinPos.String(), queryStr)
			}
		} else {

			ifFound = true
		}
	}
	// try to find in binlog by binlog position on same master
	if !ifFound && this.BinPos.File.Valid() {
		queryStr = `select id, get_src, host_id, event_time, binlog, binpos, uuid, trxid, ddl, tbldef 
		from table_schema where 
		schema_id=? and get_src=1 and host_id=? and (binlog <? or (binlog = ? and binpos < ?)) 
		order by binlog desc, binpos desc limit 1
		`
		oneRow = db.QueryRow(queryStr, this.Info.Table.SchemaId, myHost.Self.MyId, this.BinPos.File.Name, this.BinPos.File.Name, this.BinPos.File.Pos)
		err = oneRow.Scan(&id, &getSrc, &hostId, &versionTime, &binlog, &binpos, &uuid, &trxid, &ddl, &tbDefStr)
		if err != nil {
			if err != sql.ErrNoRows {
				return errors.Annotatef(err, "error to get table def %s %s : %s", myHost.Self.String(), this.BinPos.String(), queryStr)
			}
		} else {
			ifFound = true
		}
	}

	// try to find in binlog by time on all masters
	if !ifFound {
		queryStr = `select id, get_src, host_id, event_time, binlog, binpos, uuid, trxid, ddl, tbldef 
		from table_schema where 
		schema_id=? and get_src=1 and host_id=? and event_time <= ? 
		order by event_time desc, trxid desc, binlog desc,binpos desc limit 1
		`
		//must keep the order of masters. binlog name and uuid may be changed
		for _, oneMasterId := range myHost.GetAllMasterIds() {
			oneRow = db.QueryRow(queryStr, this.Info.Table.SchemaId, oneMasterId, eventTimeString)
			err = oneRow.Scan(&id, &getSrc, &hostId, &versionTime, &binlog, &binpos, &uuid, &trxid, &ddl, &tbDefStr)
			if err != nil {
				if err != sql.ErrNoRows {
					return errors.Annotatef(err, "error to get table def %s %s : %s", myHost.Self.String(), this.BinPos.String(), queryStr)
				}
			} else {
				ifFound = true
				break
			}
		}
	}

	// try to find in information_schema
	if !ifFound {
		queryStr = `select id, get_src, host_id, event_time, timestampdiff(SECOND, event_time, ?) as delta, binlog, binpos, uuid, trxid, ddl, tbldef 
		from table_schema where 
		schema_id=? and get_src=0 and host_id in (?) order by delta desc, host_id desc`
		/*
			queryStr += mystr.SliceToStringUint64(myHost.GetAllMasterIds(), ",")
			queryStr += ") order by delta desc"
		*/
		rows, err := db.Query(queryStr, eventTimeString, this.Info.Table.SchemaId, mystr.SliceToStringUint64(myHost.GetAllMasterIds(), ","))

		if err != nil {
			if err == sql.ErrNoRows {
				ifFound = false
			} else {
				return errors.Annotatef(err, "error to get table def %s %s : %s", myHost.Self.String(), this.BinPos.String(), queryStr)
			}
		} else {
			defer rows.Close()
			//find the latest event_time <=this.EventTime or the abs(delta_time) is smallest
			var delta int64
			for rows.Next() {
				err = rows.Scan(&id, &getSrc, &hostId, &versionTime, &delta, &binlog, &binpos, &uuid, &trxid, &ddl, &tbDefStr)
				if err != nil {
					return errors.Annotatef(err, "error to get query result of table def %s %s : %s", myHost.Self.String(), this.BinPos.String(), queryStr)
				}
				if delta <= 0 {
					break
				}
			}
			ifFound = true
		}

	}
	if ifFound {
		var tbDef *TableDef
		err = tbDef.UnmarshalFromString(tbDefStr)
		if err != nil {
			return err
		}

		this.BinPos.File.Name = binlog
		this.BinPos.File.Pos = binpos
		this.BinPos.Gtid.Uuid = uuid
		this.BinPos.Gtid.TrxId = trxid
		this.Ddl = ddl
		this.EventTime = versionTime
		this.GetSrc = getSrc
		this.HostId = hostId
		this.Info.Def = tbDef

		this.IfHasDef = true

	} else {
		this.IfHasDef = false
	}

	return nil

}

func (this *TableSchemaVersion) GetTableColumnFromMaster(db *sql.DB) error {
	var (
		err        error
		colName    string
		dataType   string
		colType    string
		isNullable string
		extra      string
	)

	rows, err := db.Query(CsqlGetTableColumnsSingle, this.Info.Table.Database, this.Info.Table.TableName)
	if err != nil {

		return errors.Annotate(err, "error to query master to get columns for "+this.Info.Table.String())
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&colName, &dataType, &colType, &isNullable, &extra)
		if err != nil {
			return errors.Annotate(err, "error to get query result of query master to get columns for "+this.Info.Table.String())
		}
		oneCol := &ColDef{Name: colName, TypeName: dataType}
		colType = strings.ToLower(colType)
		if strings.Contains(colType, "unsigned") {
			oneCol.Unsigned = true
		} else {
			oneCol.Unsigned = false
		}
		isNullable = strings.ToLower(isNullable)
		if isNullable == "no" {
			oneCol.Notnull = true
		} else {
			oneCol.Notnull = false
		}
		extra = strings.ToLower(extra)
		if strings.Contains(extra, "auto_increment") {
			oneCol.AutoIncrement = true
		} else {
			oneCol.AutoIncrement = false
		}
		this.Info.Def.Columns = append(this.Info.Def.Columns, oneCol)
	}
	return nil
}

func (this *TableSchemaVersion) GetPriUniqueKeyFromMaster(db *sql.DB) error {
	var (
		err     error
		keyName string
		colName string
		keyType string
		idx     int
		ok      bool
	)

	rows, err := db.Query(CsqlGetPriUniqueKeysSingle, this.Info.Table.Database, this.Info.Table.TableName)
	if err != nil {
		return errors.Annotate(err, "error to get primary/unique keys from master for "+this.Info.Table.String())
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&keyName, &colName, &keyType)
		if err != nil {
			return errors.Annotate(err, "error to get result of query to get primary/unique keys from master for "+this.Info.Table.String())
		}
		idx = this.Info.Def.GetColIndxByName(colName)
		if idx < 0 {
			return errors.Errorf("column %s of %s key %s not found in table def of %s", colName, keyType, keyName, this.Info.Table.String())
		}

		if keyType == "PRIMARY KEY" {
			this.IfHasPrimaryKey = true
			this.Info.Def.PrimaryKey.Name = "primary"
			this.Info.Def.PrimaryKey.ColumnIndices = append(this.Info.Def.PrimaryKey.ColumnIndices, idx)
			this.Info.Def.PrimaryKey.ColumnNames = append(this.Info.Def.PrimaryKey.ColumnNames, colName)
			continue
		} else {
			//unique keys
			this.IfHasUniqueKey = true
			if _, ok = this.Info.Def.UniqueKeys[keyName]; !ok {
				this.Info.Def.UniqueKeys[keyName] = &KeyDef{Name: keyName, ColumnIndices: []int{idx}, ColumnNames: []string{colName}}
			} else {
				this.Info.Def.UniqueKeys[keyName].ColumnIndices = append(this.Info.Def.UniqueKeys[keyName].ColumnIndices, idx)
				this.Info.Def.UniqueKeys[keyName].ColumnNames = append(this.Info.Def.UniqueKeys[keyName].ColumnNames, colName)
			}
		}
	}
	return nil
}

func (this *TableSchemaVersion) InsertIntoDB(db *sql.DB) error {
	var (
		err error

		jbytes   []byte
		queryStr string = `insert into table_schema (get_src, schema_id, host_id, event_time, binlog, binpos, uuid, trxid, ddl, tbldef) 
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
		`
	)
	if !this.IfHasDef {
		return nil
	}
	jbytes, err = json.Marshal(this.Info.Def)
	if err != nil {
		return errors.Annotatef(err, "error to marshal table %s def into json string: %s\n:%s", this.Info.Table.String(), spew.Sdump(this.Info.Def))
	}
	_, err = db.Exec(queryStr, this.GetSrc, this.Info.Table.SchemaId, this.HostId, this.EventTime, this.BinPos.File.Name, this.BinPos.File.Pos,
		this.BinPos.Gtid.Uuid, this.BinPos.Gtid.TrxId, this.Ddl, string(jbytes))
	if err != nil {
		return errors.Annotatef(err, "error to insert into table_schema for %s: %s", this.Info.Table.String(), spew.Sdump(this))
	}
	return nil
}

func (this *TableId) InsertIntoDB(db *sql.DB) error {
	var (
		err      error
		result   sql.Result
		id       int64
		tx       *sql.Tx
		queryStr string = "insert into schema_src (dbname, tbname) values(?,?)"
	)
	tx, err = db.Begin()
	if err != nil {
		return errors.Annotate(err, "error to start a trx")
	}
	result, err = tx.Exec(queryStr, this.Database, this.TableName)
	if err != nil {
		tx.Rollback()
		return errors.Annotate(err, "error to insert into schema_src for "+this.FullName())
	}
	id, err = result.LastInsertId()
	if err != nil {
		tx.Rollback()
		return errors.Annotate(err, "error to get last_insert_id for "+this.FullName())
	}
	tx.Commit()
	this.SchemaId = uint64(id)
	return nil
}

func (this *TableId) CopyTableSchemaFromOldTable(db *sql.DB, oldTb *TableId) error {
	var (
		err      error
		queryStr string = `insert into table_schema (get_src, schema_id, host_id, event_time, binlog, binpos, uuid, trxid, ddl, tbldef, create_time, update_time)
		select get_src, ?, host_id, event_time, binlog, binpos, uuid, trxid, ddl, tbldef, create_time, update_time from table_schema where schema_id=?
		`
	)
	tx, err := db.Begin()
	if err != nil {
		return errors.Annotate(err, "error to start a trx")
	}
	_, err = tx.Exec(queryStr, this.SchemaId, oldTb.SchemaId)
	if err != nil {
		tx.Rollback()
		return errors.Annotatef(err, "error to copy table_schema for %s from %s", this.FullName(), oldTb.FullName())
	}
	tx.Commit()
	return nil
}
