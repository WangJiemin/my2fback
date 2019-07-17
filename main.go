package main

import (
	my "my2fback/src"
	"sync"

	"github.com/siddontang/go-mysql/replication"
)

func main() {
	my.GLogger.CreateNewRawLogger()
	my.GConfCmd.IfSetStopParsPoint = false
	my.GConfCmd.ParseCmdOptions()

	my.GetTblDefFromDbAndMergeAndDump(my.GConfCmd)

	if my.GConfCmd.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
	}

	eventChan := make(chan my.MyBinEvent, my.GConfCmd.Threads*2)
	statChan := make(chan my.BinEventStats, my.GConfCmd.Threads*2)
	orgSqlChan := make(chan my.OrgSqlPrint, my.GConfCmd.Threads*2)
	sqlChan := make(chan my.ForwardRollbackSqlOfPrint, my.GConfCmd.Threads*2)
	var wg, wgGenSql sync.WaitGroup

	// stats file
	statFH, ddlFH, biglongFH := my.OpenStatsResultFiles(my.GConfCmd)
	defer statFH.Close()
	defer ddlFH.Close()
	defer biglongFH.Close()
	wg.Add(1)
	go my.ProcessBinEventStats(statFH, ddlFH, biglongFH, my.GConfCmd, statChan, &wg)
	if my.GConfCmd.IfWriteOrgSql {
		wg.Add(1)
		go my.PrintOrgSqlToFile(my.GConfCmd.OutputDir, orgSqlChan, &wg)
	}

	if my.GConfCmd.WorkType != "stats" {
		// write forward or rollback sql to file
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, sqlChan, &wg)

		// generate forward or rollback sql from binlog
		//gThreadsFinished.threadsCnt = my.GConfCmd.Threads
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, eventChan, sqlChan, &wgGenSql)
		}

	}

	if my.GConfCmd.Mode == "repl" {
		my.ParserAllBinEventsFromRepl(my.GConfCmd, eventChan, statChan, orgSqlChan)
	} else if my.GConfCmd.Mode == "file" {
		myParser := my.BinFileParser{}
		myParser.Parser = replication.NewBinlogParser()
		myParser.Parser.SetTimestampStringLocation(my.GBinlogTimeLocation)
		myParser.Parser.SetParseTime(false)  // donot parse mysql datetime/time column into go time structure, take it as string
		myParser.Parser.SetUseDecimal(false) // sqlbuilder not support decimal type
		myParser.MyParseAllBinlogFiles(my.GConfCmd, eventChan, statChan, orgSqlChan)
	}

	//fmt.Println(gThreadsFinished.threadsCnt, gThreadsFinished.threadsCnt)
	wgGenSql.Wait()
	close(sqlChan)

	wg.Wait()

}
