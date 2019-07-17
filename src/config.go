package src

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/WangJiemin/jamintools/constvar"
	"github.com/WangJiemin/jamintools/ehand"
	"github.com/WangJiemin/jamintools/logging"
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/toolkits/file"
	"github.com/toolkits/slice"
)

const (
	C_Version      = "my2fback V2.0 By WangJiemin.\n\tE_mail: 278667010@qq.com"
	C_validOptMsg  = "valid options are: "
	C_joinSepComma = ","
	//C_ddlRegexp    = `^\s*(alter|create|rename|truncate|drop)`
	C_ignoreParsedErrSql = "^create definer.+trigger"

	C_unknownColPrefix   = "dropped_column_"
	C_unknownColType     = "unknown_type"
	C_unknownColTypeCode = mysql.MYSQL_TYPE_NULL

	C_tblDefFile = "tabSchame.json"

	C_trxBegin    = 0
	C_trxCommit   = 1
	C_trxRollback = 2
	C_trxProcess  = -1

	C_reProcess  = 0
	C_reContinue = 1
	C_reBreak    = 2
	C_reFileEnd  = 3
)

type ConfCmd struct {
	Mode      string
	WorkType  string
	MysqlType string

	Host     string
	Port     uint
	User     string
	Passwd   string
	Socket   string
	ServerId uint

	//Databases    []string
	//Tables       []string
	DatabaseRegs []*regexp.Regexp
	ifHasDbReg   bool
	TableRegs    []*regexp.Regexp
	ifHasTbReg   bool
	FilterSql    []string
	FilterSqlLen int

	StartFile         string
	StartPos          uint
	StartFilePos      mysql.Position
	IfSetStartFilePos bool

	StopFile         string
	StopPos          uint
	StopFilePos      mysql.Position
	IfSetStopFilePos bool

	StartDatetime      uint32
	StopDatetime       uint32
	BinlogTimeLocation string

	IfSetStartDateTime bool
	IfSetStopDateTime  bool

	ToLastLog      bool
	PrintInterval  int
	BigTrxRowLimit int
	LongTrxSeconds int

	IfSetStopParsPoint bool

	OutputDir string

	//MinColumns     bool
	FullColumns    bool
	InsertRows     int
	KeepTrx        bool
	SqlTblPrefixDb bool
	FilePerTable   bool

	PrintExtraInfo bool
	IfWriteOrgSql  bool

	Threads uint

	ReadTblDefJsonFile string
	OnlyColFromFile    bool
	DumpTblDefToFile   string

	BinlogDir string

	GivenBinlogFile string

	UseUniqueKeyFirst         bool
	IgnorePrimaryKeyForInsert bool

	//DdlRegexp string
	ParseStatementSql bool

	IgnoreParsedErrForSql string // if parsed error, for sql match this regexp, only print error info, but not exits
	IgnoreParsedErrRegexp *regexp.Regexp
}

var (
	GLogger             *logging.MyLog = &logging.MyLog{}
	GConfCmd            *ConfCmd       = &ConfCmd{}
	GBinlogTimeLocation *time.Location
	GSqlParser          *parser.Parser = parser.New()

	GUseDatabase string = ""

	GOptsValidMode      []string = []string{"repl", "file"}
	GOptsValidWorkType  []string = []string{"tbldef", "stats", "2sql", "rollback"}
	GOptsValidMysqlType []string = []string{"mysql", "mariadb"}
	GOptsValidFilterSql []string = []string{"insert", "update", "delete"}

	GOptsValueRange map[string][]int = map[string][]int{
		"PrintInterval":  []int{1, 600, 30},
		"BigTrxRowLimit": []int{10, 30000, 500},
		"LongTrxSeconds": []int{1, 3600, 300},
		"InsertRows":     []int{1, 500, 30},
		"Threads":        []int{1, 16, 2},
	}

	GStatsColumns []string = []string{
		"StartTime", "StopTime", "Binlog", "PosRange",
		"Database", "Table",
		"BigTrxs", "BiggestTrx", "LongTrxs", "LongestTrx",
		"Inserts", "Updates", "Deletes", "Trxs", "Statements",
		"Renames", "RenamePoses", "Ddls", "DdlPoses",
	}

	GDdlPrintHeader []string = []string{"datetime", "binlog", "startposition", "stopposition", "sql"}
	//GThreadsFinished          = &Threads_Finish_Status{finishedThreadsCnt: 0, threadsCnt: 0}
)

func (this *ConfCmd) ParseCmdOptions() {
	var (
		version   bool
		dbs       string
		tbs       string
		sqlTypes  string
		startTime string
		stopTime  string
		err       error
	)

	flag.Usage = func() {
		this.PrintUsageMsg()
	}

	flag.BoolVar(&version, "v", false, "print version")
	flag.StringVar(&this.Mode, "m", "file", StrSliceToString(GOptsValidMode, C_joinSepComma, C_validOptMsg)+". repl: as a slave to get binlogs from master. file: get binlogs from local filesystem. default file")
	flag.StringVar(&this.WorkType, "w", "stats", StrSliceToString(GOptsValidWorkType, C_joinSepComma, C_validOptMsg)+". tbldef: only get table definition structure; 2sql: convert binlog to sqls, rollback: generate rollback sqls, stats: analyze transactions. default: stats")
	flag.StringVar(&this.MysqlType, "M", "mysql", StrSliceToString(GOptsValidMysqlType, C_joinSepComma, C_validOptMsg)+". server of binlog, mysql or mariadb, default mysql")

	flag.StringVar(&this.Host, "H", "127.0.0.1", "master host, DONOT need to specify when -w=stats. if mode is file, it can be slave or other mysql contains same schema and table structure, not only master. default 127.0.0.1")
	flag.UintVar(&this.Port, "P", 3306, "master port, default 3306. DONOT need to specify when -w=stats")
	flag.StringVar(&this.User, "u", "", "mysql user. DONOT need to specify when -w=stats")
	flag.StringVar(&this.Passwd, "p", "", "mysql user password. DONOT need to specify when -w=stats")
	flag.StringVar(&this.Socket, "S", "", "mysql socket file")
	flag.UintVar(&this.ServerId, "mid", 1113306, "works with -m=repl, this program replicates from master as slave to read binlogs. Must set this server id unique from other slaves, default 1113306")

	flag.StringVar(&dbs, "dbs", "", "only parse database which match any of these regular expressions. The regular expression should be in lower case because database name is translated into lower case and then matched against it. \n\tMulti regular expressions is seperated by comma, default parse all databases. Useless when -w=stats")
	flag.StringVar(&tbs, "tbs", "", "only parse table which match any of these regular expressions.The regular expression should be in lower case because database name is translated into lower case and then matched against it. \n\t Multi regular expressions is seperated by comma, default parse all tables. Useless when -w=stats")
	flag.StringVar(&sqlTypes, "sql", "", StrSliceToString(GOptsValidFilterSql, C_joinSepComma, C_validOptMsg)+". only parse these types of sql, comma seperated, valid types are: insert, update, delete; default is all(insert,update,delete)")

	flag.StringVar(&this.StartFile, "sbin", "", "binlog file to start reading")
	flag.UintVar(&this.StartPos, "spos", 0, "start reading the binlog at position")
	flag.StringVar(&this.StopFile, "ebin", "", "binlog file to stop reading")
	flag.UintVar(&this.StopPos, "epos", 0, "Stop reading the binlog at position")

	flag.StringVar(&this.BinlogTimeLocation, "tl", "Local", "time location to parse timestamp/datetime column in binlog, such as Asia/Shanghai. default Local")
	flag.StringVar(&startTime, "sdt", "", "Start reading the binlog at first event having a datetime equal or posterior to the argument, it should be like this: \"2004-12-25 11:25:56\"")
	flag.StringVar(&stopTime, "edt", "", "Stop reading the binlog at first event having a datetime equal or posterior to the argument, it should be like this: \"2004-12-25 11:25:56\"")

	flag.BoolVar(&this.ToLastLog, "C", false, "works with -w='stats', keep analyzing transations to last binlog for -m=file, and keep analyzing for -m=repl")
	flag.IntVar(&this.PrintInterval, "i", this.GetDefaultValueOfRange("PrintInterval"), "works with -w='stats', print stats info each PrintInterval. "+this.GetDefaultAndRangeValueMsg("PrintInterval"))
	flag.IntVar(&this.BigTrxRowLimit, "b", this.GetDefaultValueOfRange("BigTrxRowLimit"), "transaction with affected rows greater or equal to this value is considerated as big transaction. "+this.GetDefaultAndRangeValueMsg("BigTrxRowLimit"))
	flag.IntVar(&this.LongTrxSeconds, "l", this.GetDefaultValueOfRange("LongTrxSeconds"), "transaction with duration greater or equal to this value is considerated as long transaction. "+this.GetDefaultAndRangeValueMsg("LongTrxSeconds"))

	flag.BoolVar(&this.FullColumns, "a", false, "Works with -w=2sql|rollback. for update sql, include unchanged columns. for update and delete, use all columns to build where condition.\t\ndefault false, this is, use changed columns to build set part, use primary/unique key to build where condition")

	flag.IntVar(&this.InsertRows, "r", this.GetDefaultValueOfRange("InsertRows"), "Works with -w=2sql|rollback. rows for each insert sql. "+this.GetDefaultAndRangeValueMsg("InsertRows"))
	flag.BoolVar(&this.KeepTrx, "k", false, "Works with -w=2sql|rollback. wrap result statements with 'begin...commit|rollback'")
	flag.BoolVar(&this.SqlTblPrefixDb, "d", true, "Works with -w=2sql|rollback. Prefix table name with database name in sql, ex: insert into db1.tb1 (x1, x1) values (y1, y1). Default true")

	flag.StringVar(&this.OutputDir, "o", "", "result output dir, default current work dir. Attension, result files could be large, set it to a dir with large free space")
	flag.BoolVar(&this.IfWriteOrgSql, "ors", false, "for mysql>=5.6.2 and binlog_rows_query_log_events=on, if set, output original sql. default false")

	flag.BoolVar(&this.PrintExtraInfo, "e", false, "Works with -w=2sql|rollback. Print database/table/datetime/binlogposition...info on the line before sql, default false")

	flag.BoolVar(&this.FilePerTable, "f", false, "Works with -w=2sql|rollback. one file for one table if true, else one file for all tables. default false. Attention, always one file for one binlog")

	flag.UintVar(&this.Threads, "t", uint(this.GetDefaultValueOfRange("Threads")), "Works with -w=2sql|rollback. threads to run, default 4")

	flag.StringVar(&this.ReadTblDefJsonFile, "rj", "", "Works with -w=2sql|rollback, read table structure from this file and merge from mysql")
	flag.BoolVar(&this.OnlyColFromFile, "oj", false, "Only use table structure from -rj, do not get or merge table struct from mysql")
	flag.StringVar(&this.DumpTblDefToFile, "dj", C_tblDefFile, "dump table structure to this file. default "+C_tblDefFile)

	flag.BoolVar(&this.UseUniqueKeyFirst, "U", false, "prefer to use unique key instead of primary key to build where condition for delete/update sql")
	flag.BoolVar(&this.IgnorePrimaryKeyForInsert, "I", false, "for insert statement when -wtype=2sql, ignore primary key")
	//flag.StringVar(&this.DdlRegexp, "de", C_ddlRegexp, "sql(lower case) matching this regular expression will be outputed into ddl_info.log")
	flag.BoolVar(&this.ParseStatementSql, "stsql", false, "when -w=2sql, also parse plain sql and write into result file even if binlog_format is not row. default false")
	flag.StringVar(&this.IgnoreParsedErrForSql, "ies", C_ignoreParsedErrSql, "for sql which is error to parsed and matched by this regular expression, just print error info, skip it and continue parsing, otherwise stop parsing and exit.\n\tThe regular expression should be in lower case, because sql is translated into lower case and then matched against it.")

	flag.Parse()
	//flag.PrintDefaults()

	if version {
		fmt.Printf("%s\n", C_Version)
		os.Exit(0)
	}

	if this.Mode != "repl" && this.Mode != "file" {
		GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("unsupported mode=%s, valid modes: file, repl\n", this.Mode),
			logging.ERROR, ehand.ERR_INVALID_OPTION)
	}

	if this.Mode == "file" && this.WorkType != "tbldef" {
		// the last arg should be binlog file
		if flag.NArg() != 1 {
			GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("missing binlog file. binlog file as last arg must be specify when -m=file"),
				logging.ERROR, ehand.ERR_MISSING_OPTION)

		}
		this.GivenBinlogFile = flag.Args()[0]
		if !file.IsFile(this.GivenBinlogFile) {
			GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("%s doesnot exists nor a file\n", this.GivenBinlogFile),
				logging.ERROR, ehand.ERR_FILE_NOT_EXISTS)
		} else {
			this.BinlogDir = filepath.Dir(this.GivenBinlogFile)
		}
	}

	if this.ReadTblDefJsonFile != "" {
		GLogger.WriteToLogByFieldsNormalOnlyMsg("start to get table structure from file "+this.ReadTblDefJsonFile, logging.INFO)
		if !file.IsFile(this.ReadTblDefJsonFile) {
			GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("%s doesnot exists nor a file\n", this.ReadTblDefJsonFile),
				logging.ERROR, ehand.ERR_FILE_NOT_EXISTS)
		}
		jdat, err := file.ToBytes(this.ReadTblDefJsonFile)
		if err != nil {
			GLogger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to read file "+this.ReadTblDefJsonFile,
				logging.ERROR, ehand.ERR_FILE_READ)
		}

		err = json.Unmarshal(jdat, &(G_TablesColumnsInfo.tableInfos))
		//(&G_TablesColumnsInfo).DumpTblInfoJsonToFile("tmp.json")
		if err != nil {
			GLogger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to unmarshal file "+this.ReadTblDefJsonFile,
				logging.ERROR, ehand.ERR_JSON_UNMARSHAL)
		} else {
			GLogger.WriteToLogByFieldsNormalOnlyMsg("successfully get table structure from file "+this.ReadTblDefJsonFile, logging.INFO)
		}
	}

	// check --output-dir
	if this.OutputDir != "" {
		ifExist, errMsg := CheckIsDir(this.OutputDir)
		if !ifExist {
			GLogger.WriteToLogByFieldsExitMsgNoErr(errMsg, logging.ERROR, ehand.ERR_DIR_NOT_EXISTS)
		}
	} else {
		this.OutputDir, _ = os.Getwd()
	}

	if this.DumpTblDefToFile != "" {
		if !file.IsExist(filepath.Dir(this.DumpTblDefToFile)) {
			GLogger.WriteToLogByFieldsExitMsgNoErr("dir of "+this.DumpTblDefToFile+" not exists",
				logging.ERROR, ehand.ERR_INVALID_OPTION)
		}
		if !filepath.IsAbs(this.DumpTblDefToFile) {
			this.DumpTblDefToFile = filepath.Join(this.OutputDir, this.DumpTblDefToFile)
		}
	}

	this.ifHasDbReg = false
	if dbs != "" {
		dbArr := CommaSeparatedListToArray(dbs)
		for _, dbRegStr := range dbArr {
			dbreg, err := regexp.Compile(dbRegStr)
			if err != nil {
				GLogger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("%s is not a valid regular expression", dbRegStr), logging.ERROR, ehand.ERR_ERROR)
			}
			this.DatabaseRegs = append(this.DatabaseRegs, dbreg)
		}
		if len(this.DatabaseRegs) > 0 {
			this.ifHasDbReg = true
		}
	}

	this.ifHasTbReg = false
	if tbs != "" {
		tbArr := CommaSeparatedListToArray(tbs)
		for _, tbRegStr := range tbArr {
			tbReg, err := regexp.Compile(tbRegStr)
			if err != nil {
				GLogger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("%s is not a valid regular expression", tbRegStr), logging.ERROR, ehand.ERR_ERROR)
			}
			this.TableRegs = append(this.TableRegs, tbReg)
		}
		if len(this.TableRegs) > 0 {
			this.ifHasTbReg = true
		}
	}

	if sqlTypes != "" {

		this.FilterSql = CommaSeparatedListToArray(sqlTypes)
		for _, oneSqlT := range this.FilterSql {
			CheckElementOfSliceStr(GOptsValidFilterSql, oneSqlT, "invalid sqltypes", true)
		}
		this.FilterSqlLen = len(this.FilterSql)
	} else {
		this.FilterSqlLen = 0
	}

	GBinlogTimeLocation, err = time.LoadLocation(this.BinlogTimeLocation)
	if err != nil {
		GLogger.WriteToLogByFieldsErrorExtramsgExit(err, "invalid time location "+this.BinlogTimeLocation, logging.ERROR, ehand.ERR_ERROR)
	}
	if startTime != "" {
		t, err := time.ParseInLocation(constvar.DATETIME_FORMAT, startTime, GBinlogTimeLocation)
		GLogger.WriteToLogByFieldsErrorExtramsgExit(err, "invalid start datetime -sdt "+startTime,
			logging.ERROR, ehand.ERR_ERROR)
		this.StartDatetime = uint32(t.Unix())
		this.IfSetStartDateTime = true
	} else {
		this.IfSetStartDateTime = false
	}

	if stopTime != "" {
		t, err := time.ParseInLocation(constvar.DATETIME_FORMAT, stopTime, GBinlogTimeLocation)
		GLogger.WriteToLogByFieldsErrorExtramsgExit(err, "invalid stop datetime -edt "+startTime,
			logging.ERROR, ehand.ERR_ERROR)
		this.StopDatetime = uint32(t.Unix())
		this.IfSetStopDateTime = true
		this.IfSetStopParsPoint = true
	} else {
		this.IfSetStopDateTime = false
	}

	if startTime != "" && stopTime != "" {
		if this.StartDatetime >= this.StopDatetime {
			GLogger.WriteToLogByFieldsExitMsgNoErr("-sdt must be ealier than -edt", logging.ERROR, ehand.ERR_OPTION_MISMATCH)
		}
	}

	/*
		if this.DdlRegexp == "" {
			this.DdlRegexp = C_ddlRegexp
		}
		gDdlRegexp = regexp.MustCompile(this.DdlRegexp)
	*/
	if this.IgnoreParsedErrForSql != "" {
		this.IgnoreParsedErrRegexp, err = regexp.Compile(this.IgnoreParsedErrForSql)
		if err != nil {
			GLogger.WriteToLogByFieldsErrorExtramsgExit(err, "invalid regular expression: "+this.IgnoreParsedErrForSql, logging.ERROR, ehand.ERR_REG_COMPILE)
		}
	}

	this.CheckCmdOptions()

}

func (this *ConfCmd) CheckCmdOptions() {
	//check -m
	CheckElementOfSliceStr(GOptsValidMode, this.Mode, "invalid arg for -m", true)

	//check -w
	CheckElementOfSliceStr(GOptsValidWorkType, this.WorkType, "invalid arg for -w", true)

	//check --mtype
	CheckElementOfSliceStr(GOptsValidMysqlType, this.MysqlType, "invalid arg for -M", true)

	if this.Mode != "file" && this.WorkType != "stats" {
		//check --user
		this.CheckRequiredOption(this.User, "-u must be set", true)
		//check --password
		this.CheckRequiredOption(this.Passwd, "-p must be set", true)

	}

	if this.StartFile != "" {
		this.StartFile = filepath.Base(this.StartFile)
	}
	if this.StopFile != "" {
		this.StopFile = filepath.Base(this.StopFile)
	}

	//check --start-binlog --start-pos --stop-binlog --stop-pos
	if this.StartFile != "" && this.StartPos != 0 && this.StopFile != "" && this.StopPos != 0 {
		cmpRes := CompareBinlogPos(this.StartFile, this.StartPos, this.StopFile, this.StopPos)
		if cmpRes != -1 {
			GLogger.WriteToLogByFieldsExitMsgNoErr("start postion(-sbin -spos) must less than stop position(-ebin -epos)",
				logging.ERROR, ehand.ERR_OPTION_MISMATCH)
		}
	}

	if this.StartFile != "" && this.StartPos != 0 {
		this.IfSetStartFilePos = true
		this.StartFilePos = mysql.Position{Name: this.StartFile, Pos: uint32(this.StartPos)}

	} else {
		if this.StartFile != "" || this.StartPos != 0 {
			GLogger.WriteToLogByFieldsExitMsgNoErr("-sbin and -spos must be set together",
				logging.ERROR, ehand.ERR_MISSING_OPTION)
		}
		this.IfSetStartFilePos = false

	}

	if this.StopFile != "" && this.StopPos != 0 {

		this.IfSetStopFilePos = true
		this.StopFilePos = mysql.Position{Name: this.StopFile, Pos: uint32(this.StopPos)}
		this.IfSetStopParsPoint = true

	} else {
		if this.StopFile != "" || this.StopPos != 0 {
			GLogger.WriteToLogByFieldsExitMsgNoErr("-ebin and -epos must be set together",
				logging.ERROR, ehand.ERR_MISSING_OPTION)
		}

		this.IfSetStopFilePos = false
		this.IfSetStopParsPoint = false

	}

	if this.Mode == "repl" && this.WorkType != "tbldef" {
		if this.StartFile == "" || this.StartPos == 0 {
			GLogger.WriteToLogByFieldsExitMsgNoErr("when -m=repl, -sbin and -spos must be specified",
				logging.ERROR, ehand.ERR_MISSING_OPTION)
		}
	}

	// check --interval
	if this.PrintInterval != this.GetDefaultValueOfRange("PrintInterval") {
		this.CheckValueInRange("PrintInterval", this.PrintInterval, "value of -i out of range", true)
	}

	// check --big-trx-rows
	if this.BigTrxRowLimit != this.GetDefaultValueOfRange("BigTrxRowLimit") {
		this.CheckValueInRange("BigTrxRowLimit", this.BigTrxRowLimit, "value of -b out of range", true)
	}

	// check --long-trx-seconds
	if this.LongTrxSeconds != this.GetDefaultValueOfRange("LongTrxSeconds") {
		this.CheckValueInRange("LongTrxSeconds", this.LongTrxSeconds, "value of -l out of range", true)
	}

	// check --insert-rows
	if this.InsertRows != this.GetDefaultValueOfRange("InsertRows") {
		this.CheckValueInRange("InsertRows", this.InsertRows, "value of -r out of range", true)
	}

	// check --threads
	if this.Threads != uint(this.GetDefaultValueOfRange("Threads")) {
		this.CheckValueInRange("Threads", int(this.Threads), "value of -t out of range", true)
	}

	// check --to-last-log
	if this.ToLastLog {
		if this.Mode != "repl" || this.WorkType != "stats" {
			GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintln("-C only works with -m=repl and -w=stats"),
				logging.ERROR, ehand.ERR_OPTION_MISMATCH)
		}
		this.IfSetStopParsPoint = true
	}

}

func (this *ConfCmd) CheckValueInRange(opt string, val int, prefix string, ifExt bool) bool {
	valOk := true
	if val < this.GetMinValueOfRange(opt) {
		valOk = false
	} else if val > this.GetMaxValueOfRange(opt) {
		valOk = false
	}

	if !valOk {

		if ifExt {
			GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("%s: %d is specfied, but %s\n",
				prefix, val, this.GetDefaultAndRangeValueMsg(opt)), logging.ERROR, ehand.ERR_OPTION_OUTRANGE)
		} else {
			GLogger.WriteToLogByFieldsNormalOnlyMsgExitCode(fmt.Sprintf("%s: %d is specfied, but %s\n",
				prefix, val, this.GetDefaultAndRangeValueMsg(opt)), logging.ERROR, ehand.ERR_OPTION_OUTRANGE)
		}
	}
	return valOk
}

func (this *ConfCmd) CheckRequiredOption(v interface{}, prefix string, ifExt bool) bool {
	// options must set, default value is not suitable
	notOk := false
	switch realVal := v.(type) {
	case string:
		if realVal == "" {
			notOk = true
		}
	case int:
		if realVal == 0 {
			notOk = true
		}
	}
	if notOk {
		GLogger.WriteToLogByFieldsExitMsgNoErr(prefix, logging.ERROR, ehand.ERR_INVALID_OPTION)
	}
	return true
}

func (this *ConfCmd) PrintUsageMsg() {
	//flag.Usage()
	var logo = `
*****************************************************************************************************
*	system_command: {$command}																		*
*	system_goos: {$goos}																			*
*	system_arch: {$arch}																			*
*	hostname: {$hostname}																			*
*	hostaddress: {$host_ip}																			*
*	blog: {$url}																					*
*		read binlog from master, work as a fake slave: ./my2fback -m repl opts...					*
*		read binlog from local filesystem: ./my2fback -m file opts... mysql-bin.000010				*
*****************************************************************************************************
	`
	arch := fmt.Sprint(runtime.GOARCH)
	good := fmt.Sprint(runtime.GOOS)
	hostname, host_ip := GetSystemHomeNameAndAdderss()
	fmt.Printf("%s\n", C_Version)
	logo = strings.Replace(logo, "{$command}", os.Args[0], -1)
	logo = strings.Replace(logo, "{$arch}", arch, -1)
	logo = strings.Replace(logo, "{$goos}", good, -1)
	logo = strings.Replace(logo, "{$hostname}", hostname, -1)
	logo = strings.Replace(logo, "{$host_ip}", host_ip, -1)
	logo = strings.Replace(logo, "{$url}", "https://jiemin.wang", -1)
	fmt.Println(logo)
	flag.PrintDefaults()

}

func (this *ConfCmd) GetMinValueOfRange(opt string) int {
	return GOptsValueRange[opt][0]
}

func (this *ConfCmd) GetMaxValueOfRange(opt string) int {
	return GOptsValueRange[opt][1]
}

func (this *ConfCmd) GetDefaultValueOfRange(opt string) int {
	//fmt.Printf("default value of %s: %d\n", opt, gOptsValueRange[opt][2])
	return GOptsValueRange[opt][2]
}

func (this *ConfCmd) GetDefaultAndRangeValueMsg(opt string) string {
	return fmt.Sprintf("Valid values range from %d to %d, default %d",
		this.GetMinValueOfRange(opt),
		this.GetMaxValueOfRange(opt),
		this.GetDefaultValueOfRange(opt),
	)
}

func (this *ConfCmd) IsTargetTable(db, tb string) bool {
	dbLower := strings.ToLower(db)
	tbLower := strings.ToLower(tb)
	if this.ifHasDbReg {
		ifMatch := false
		for _, oneReg := range this.DatabaseRegs {
			if oneReg.MatchString(dbLower) {
				ifMatch = true
				break
			}
		}
		if !ifMatch {
			return false
		}
	}

	if this.ifHasTbReg {
		ifMatch := false
		for _, oneReg := range this.TableRegs {
			if oneReg.MatchString(tbLower) {
				ifMatch = true
				break
			}
		}
		if !ifMatch {
			return false
		}
	}
	return true

}

func (this *ConfCmd) IsTargetDml(dml string) bool {
	if this.FilterSqlLen < 1 {
		return true
	}
	if slice.ContainsString(this.FilterSql, dml) {
		return true
	} else {
		return false
	}
}
