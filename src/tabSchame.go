package src

import (
	"fmt"

	"github.com/WangJiemin/jamintools/ehand"
	"github.com/WangJiemin/jamintools/logging"
)

func GetTblDefFromDbAndMergeAndDump(cfg *ConfCmd) {

	ifNeedGetTblDefFromDb := false
	if cfg.WorkType == "tbldef" {
		ifNeedGetTblDefFromDb = true
	}
	if cfg.WorkType != "stats" && !cfg.OnlyColFromFile {
		ifNeedGetTblDefFromDb = true
	}

	if ifNeedGetTblDefFromDb {
		if (cfg.Socket == "") && (cfg.Host == "" || cfg.Port == 0) {
			GLogger.WriteToLogByFieldsExitMsgNoErr("when (-w!=stats and not sepecify -oj) or -w=tbldef, must specify mysql addr and login user/password to get table definition",
				logging.ERROR, ehand.ERR_MISSING_OPTION)

		} else if cfg.User == "" || cfg.Passwd == "" {
			GLogger.WriteToLogByFieldsExitMsgNoErr("when (-w!=stats and not sepecify -oj) or -w=tbldef, must specify mysql addr and login user/password to get table definition",
				logging.ERROR, ehand.ERR_MISSING_OPTION)
		}

		GLogger.WriteToLogByFieldsNormalOnlyMsg("start to get table structure from mysql", logging.INFO)
		GetAndMergeColumnStructFromJsonFileAndDb(cfg, &G_TablesColumnsInfo)
		//fmt.Println("finish getting table struct from db:", time.Now())
		//write table column def json
		if len(G_TablesColumnsInfo.tableInfos) == 0 {
			GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("get no table difinition info from mysql. -dbs and -tbs should be in lower case, or pls check user %s has privileges to read tables in infomation_schema!!!\nError Exits!!", cfg.User),
				logging.ERROR, ehand.ERR_MYSQL_QUERY)
		} else {
			GLogger.WriteToLogByFieldsNormalOnlyMsg("successfully get table structure from mysql", logging.INFO)
		}

	}

	if cfg.WorkType != "stats" && len(G_TablesColumnsInfo.tableInfos) == 0 {
		GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("-w!=stats, but get no table definition info from mysql or local json file!!!\nError Exits!!"),
			logging.ERROR, ehand.ERR_MYSQL_QUERY)
	}

	if cfg.DumpTblDefToFile != "" && ifNeedGetTblDefFromDb && len(G_TablesColumnsInfo.tableInfos) > 0 {
		(&G_TablesColumnsInfo).DumpTblInfoJsonToFile(cfg.DumpTblDefToFile)
		GLogger.WriteToLogByFieldsNormalOnlyMsg("table definition has been dumped to "+cfg.DumpTblDefToFile, logging.INFO)
	}

	if cfg.WorkType == "tbldef" {
		GLogger.WriteToLogByFieldsNormalOnlyMsgExit(fmt.Sprintf("-w=tbldef, and table definition has been dumped to %s\nExits! Bye!\n",
			cfg.DumpTblDefToFile), logging.WARNING)
	}
}

