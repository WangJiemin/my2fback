package dbes

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
)

const (
	CregDbpartName  string = "dbpart"
	CregTbPartName  string = "tbpart"
	CesIdSep        string = "::"
	CesIndexNameSep string = "."
)

type TableToEsIndexRegexp struct {
	Database *regexp.Regexp // group match 'dbpart' will be first part of es index name
	Table    *regexp.Regexp // group match 'tbpart' will be last part of es index name
}

func (this *TableToEsIndexRegexp) IsTableMatch(dbname, tbname string) bool {
	if this.Database.MatchString(dbname) && this.Table.MatchString(tbname) {
		return true
	} else {
		return false
	}

}

// return: indexName, ifBothDatabaseAndTableMatch
// both db and tb  match regexp and submatch of group dbpart and tbpart not empty, return them
// other case, return empty string "".
func (this *TableToEsIndexRegexp) GetEsIndexName(dbname, tbname, sep string) (string, bool) {
	var (
		dbPart string = ""
		tbPart string = ""
	)

	if this.Database == nil || this.Table == nil {
		return "", false
	}
	arr := this.Database.FindStringSubmatch(dbname)
	arrLen := len(arr)
	if arrLen > 0 {

		if arrLen > 1 {
			for i, kn := range this.Database.SubexpNames() {
				if i > 0 && i < arrLen && kn == CregDbpartName {
					dbPart = arr[i]
					break
				}
			}
		}
	} else {
		return "", false
	}

	arr = this.Table.FindStringSubmatch(tbname)
	arrLen = len(arr)
	if arrLen > 0 {

		if arrLen > 1 {
			for i, kn := range this.Table.SubexpNames() {
				if i > 0 && i < arrLen && kn == CregTbPartName {
					tbPart = arr[i]
					break
				}
			}
		}
	} else {
		return "", false
	}

	if dbPart != "" && tbPart != "" {
		return dbPart + sep + tbPart, true
	}
	return "", true

}

type TableToEsSetting struct {
	EsIndexPrefix  string `toml:"es_index_prefix"`
	EsIndexSuffix  string `toml:"es_index_suffix"`
	EsIdSep        string `toml:"es_index_id_seperator"`
	EsIndexNameSep string `toml:"es_index_name_seperator"`
	SkipSyncInsert bool   `toml:"skip_sync_insert"`
	SkipSyncDelete bool   `toml:"skip_sync_delete"`
	SkipSyncUpdate bool   `toml:"skip_sync_update"`
}

func (this *TableToEsSetting) GetEsIndexName(dbname, tbname string) string {
	var (
		idxName string = ""
	)
	if this.EsIndexPrefix != "" {
		idxName = this.EsIndexPrefix + this.EsIndexNameSep
	}
	idxName += dbname + this.EsIndexNameSep + tbname
	if this.EsIndexSuffix != "" {
		idxName += this.EsIndexNameSep + this.EsIndexSuffix
	}
	return idxName

}

type TableToEsIndexConf struct {
	DatabaseRegexp string `toml:"database_regexp"`
	TableRegexp    string `toml:"table_regexp"`
	Filter         *TableToEsIndexRegexp
	EsIndex        string            `toml:"es_index_name"`
	ExcludeFields  []string          `toml:"include_table_fields"`
	IncludeFields  []string          `toml:"exclude_table_fields"`
	EsIdFields     []string          `toml:"es_index_id_fields"`
	Setting        *TableToEsSetting `toml:"index_common_cfg"`
}

func (this *TableToEsIndexConf) SetRegExp() error {
	var (
		tbReg *TableToEsIndexRegexp = &TableToEsIndexRegexp{Database: nil, Table: nil}
		err   error
	)
	this.DatabaseRegexp = strings.TrimSpace(this.DatabaseRegexp)
	this.TableRegexp = strings.TrimSpace(this.TableRegexp)

	if this.DatabaseRegexp == "" || this.TableRegexp == "" {
		return errors.Errorf("None of TableToEsIndexConf.DatabaseRegexp and TableToEsIndexConf.TableRegexp can be empty")
	}

	tbReg.Database, err = regexp.Compile(this.DatabaseRegexp)
	if err != nil {
		return errors.Annotate(err, "invalid database regular expression")
	}

	tbReg.Table, err = regexp.Compile(this.TableRegexp)
	if err != nil {
		return errors.Annotate(err, "invalid table regular expression")
	}

	this.Filter = tbReg
	return nil
}

// return "" if not match
func (this *TableToEsIndexConf) GetEsIndexName(dbname, tbname string) string {
	idx, _ := this.Filter.GetEsIndexName(dbname, tbname, this.Setting.EsIndexNameSep)
	if idx == "" {
		// not match, use database.table as es index name
		return ""
	}
	if this.EsIndex != "" {
		// prefer this as es index name
		idx = this.EsIndex
	}
	if this.Setting.EsIndexPrefix != "" {
		idx = this.Setting.EsIndexPrefix + this.Setting.EsIndexNameSep + idx
	}
	if this.Setting.EsIndexSuffix != "" {
		idx += this.Setting.EsIndexNameSep + this.Setting.EsIndexSuffix
	}
	return idx
}

type TableToEsIndexConfAll struct {
	Setting        *TableToEsSetting     `toml:"es_index_common"`
	EsIndexSyncCfg []*TableToEsIndexConf `toml:"es_index_table"`
}
