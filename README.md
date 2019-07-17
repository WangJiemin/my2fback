# 简介
	my2fback 实现了基于row格式binlog的回滚闪回功能，让误删除或者误更新数据，可以不停机不使用备份而快速回滚误操作。
	         也可以解释binlog（支持非row格式binlog）生成易读的SQL。
	
	my2fback 连接数据库帐号的权限: 
	    MySQL5.6/MariaDB10.1/MariaDB10.2版本
	    mysql> GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, PROCESS ON *.* TO 'user'@'localhost' IDENTIFIED BY 'xxxxxx';
	    
	    MySQL5.7版本
	    mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'xxxxxx';
	    mysql> GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, PROCESS ON *.* TO 'user'@'localhost';
        	    
	    
    my2fback 通过解释mysql/mariadb binlog/relaylog实现以下三大功能:
        1）flashback/闪回/回滚， DML回滚到任意时间或者位置。
        	生成的文件名为rollback.xxx.sql或者db.tb.rollback.xxx.sql
            生成的SQL形式如下
            ```sql
            begin
            DELETE FROM `test`.`t1` WHERE `id`=1
            # datetime=2019-06-15_16:23:58 database=test table=t1 binlog=mysql-bin.000012 startpos=417 stoppos=575
            commit
            ```
        2）前滚，把binlog/relaylog的DML解释成易读的SQL语句。
        	*支持非row格式的binlog， 默认不解释非row格式的DML， 需要指定参数-stsql
        	生成的文件名为forward.xxx.sql或者db.tb.forward.xxx.sql
            生成的SQL形式如下
            ```sql
            begin
            # datetime=2019-06-15_16:23:58 database=test table=t1 binlog=mysql-bin.000012 startpos=417 stoppos=575
            INSERT INTO `test`.`t1` (`id`,`name`,`sr`,`icon`,`points`,`sa`,`sex`) VALUES (1,'张三1','华南理工大学&SCUT',X'89504e47',1.1,1.1,1)
            commit
            ```
        3) 输出row格式下的原始SQL（5.7）
        	结果文件名为original_sql.binlogxxx.sql
        	
        4) 统计分析， 输出各个表的DML统计， 输出大事务与长事务， 输出所有的DDL 
           * DML统计结果文件：binlog_status.txt
           * 大事务与长事务结果文件：binlog_biglong_trx.txt
           * DDL结果文件：ddl_info.txt
# 版本 
    * 以上功能均可指定任意的单库多库， 单表多表， 任意时间点， 任意binlog位置。
    * 支持mysql5.5及以上，也支持MariaDB的binlog， 支持传统复制的binlog， 也支持GTID的binlog。
    * 支持直接指定文件路径的binlog， 也支持主从复制， my2fback 作为从库从主库拉binlog来过解释。
# 限制
    * 使用回滚/闪回功能时，binlog格式必须为row,且binlog_row_image=full， 其它功能支持非row格式binlog
    * 只能回滚DML， 不能回滚DDL
    * 支持V4格式的binlog， V3格式的没测试过，测试与使用结果显示，mysql5.1，mysql5.5, mysql5.6与mysql5.7的binlog均支持
    * 支持指定-tl时区来解释binlog中time/datetime字段的内容。开始时间-sdt与结束时间-edt也会使用此指定的时区， 
       但注意此开始与结束时间针对的是binlog event header中保存的unix timestamp。结果中的额外的datetime时间信息都是binlog event header中的unix timestamp
    * decimal字段使用float64来表示， 但不损失精度
    * 所有字符类型字段内容按golang的utf8(相当于mysql的utf8mb4)来表示
# 特点
    1) 支持V4版本的binlog， 支持传统与GTID的binlog， 支持mysql5.1与mairiadb5.5及以上版本的binlog， 也同样支持relaylog(结果中注释的信息binlog=xxx startpos=xxx stoppos=xx是对应的主库的binlog信息)
        --mtype=mariadb
    2）支持以时间及位置条件过滤， 并且支持单个以及多个连续binlog的解释。
    	区间范围为左闭右开， [-sxx， -exxx)
        解释binlog的开始位置：
            -sbin mysql-bin.000101
            -spos 4
        解释binlog的结束位置：
            -ebin mysql-bin.000105
            -epos 4
        解释binlog的开始时间    
            -sdt "2018-04-21 00:00:00"
        解释binlog的结束时间  
            -edt "2018-04-22 11:00:00"
    3）支持以库及表条件过滤, 以逗号分隔
    	支持正则表达式，如-dbs "db\d+,db_sh\d+"。正则表达式中请使用小写字母，因为数据库名与表名会先转成小写再与正则表达式进行匹配
        -dbs db1,db2
        -tbs tb1,tb2
    4）支持以DML类型(update,delete,insert)条件过滤
        -sql delete,update
    5) 支持分析本地binlog，也支持复制协议， my2fback作为一个从库从主库拉binlog来本地解释
        -m file //解释本地binlog
        -m repl //my2fback作为slave连接到主库拉binlog来解释
    6）输出的结果支持一个binlog一个文件， 也可以一个表一个文件
        -f 
        例如对于binlog mysql-bin.000101, 如果一个表一个文件， 则生成的文件形式为db.tb.rollback.101.sql(回滚)，db.tb.forward.101.sql(前滚)，
        否则是rollback.101.sql(回滚),forward.101.sql(前滚)
    7）输出的结果是大家常见的易读形式的SQL，支持表名前是否加数据库名
        -d
        ```sql
        begin
        # datetime=2019-06-15_00:14:34 database=test table=t1 binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `test`.`t1` SET `sa`=1001 WHERE `id`=5;
        # datetime=2019-06-15_00:14:45 database=test table=t1 binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `test`.`t1` SET `name`=null WHERE `id`=5;
        commit
        ```
        否则为
         ```sql
        begin
        # datetime=2019-06-15_00:14:34 database=test table=t1 binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `t1` SET `sa`=1001 WHERE `id`=5;
        # datetime=2019-06-15_00:14:45 database=test table=t1 binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `t1` SET `name`=null WHERE `id`=5;
        commit
        ```
        
    8）输出结果支持是否保留事务
        -k
        ```sql
        begin
        # datetime=2019-06-15_00:14:34 database=test table=t1 binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `test`.`t1` SET `sa`=1001 WHERE `id`=5;
        # datetime=2019-06-15_00:14:45 database=test table=t1 binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `test`.`t1` SET `name`=null WHERE `id`=5;
        commit
        ```
        不保留则是这样：
        ```sql
        # datetime=2019-06-15_00:14:34 database=test table=t1 binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `test`.`t1` SET `sa`=1001 WHERE `id`=5;
        # datetime=2019-06-15_00:14:45 database=test table=t1 binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `test`.`t1` SET `name`=null WHERE `id`=5;
        ```
        如果复制因为特别大的事务而中断， 则可以以不保留事务的形式生成前滚的SQL, 在从库上执行， 然后跳过这个事务， 再启动复制， 免去重建从库的
        麻烦， 特别是很大的库
    9）支持输出是否包含时间与binlog位置信息
        -e
        包含额外的信息则为
        ```sql
        # datetime=2019-06-15_00:14:34 database=test table=t1 binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `test`.`t1` SET `sa`=1001 WHERE `id`=5;
        # datetime=2019-06-15_00:14:45 database=test table=t1 binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `test`.`t1` SET `name`=null WHERE `id`=5;
        ```
        否则为
        ```sql
        UPDATE `test`.`t1` SET `sa`=1001 WHERE `id`=5;
        UPDATE `test`.`t1` SET `name`=null WHERE `id`=5;
        ```
    10）支持生成的SQL只包含最少必须的字段, 前提下是表含有主键或者唯一索引
        默认为
        ```sql
        UPDATE `test`.`t1` SET `sa`=1001 WHERE `id`=5;
        DELETE FROM `test` WHERE `id`=5;
        ```
        -a 则为
        ```sql
        UPDATE `test`.`t1` SET `id`=5, `age`=21, `sex`='M',`sa`=1001, `name`='test' WHERE `id`=5 and `age`=21 and `sex`='M' and `sa`=900 and `name`='test';
        DELETE FROM `test` WHERE `id`=5 and `age`=21 and `sex`='M' and `sa`=900 and `name`='test';
        ```
    11） 支持优先使用唯一索引而不是主键来构建where条件
        -U
        有时不希望使用主健来构建wheret条件， 如发生双写时， 自增主健冲突了， 这时使用非主健的唯一索引来避免生成的SQL主健冲突
    12） 支持生成的insert语句不包含主健
        -I
        发生双写时， 自增主健冲突了， 这时使用这个参数来让生成的insert语句不包括主健来避免生成的SQL主健冲突
    13）支持大insert拆分成小insert语句。
        -r 100
        对于一个insert 1000行的插入， 会生成10个insert语句，每个语句插入100行
    
    14）支持非row格式binlog的解释
    	当-w 2sql时加上参数-stsql，则会解释非row格式的DML语句。使用的是https://github.com/pingcap/parser的SQL解释器来解释DDL与非row格式的DML。
		由于不是支持所有要SQL， 如create trigger就不支持， 遇到SQL无法解释时会报错退出， 如需要跳过该SQL并继续解释， 请使用参数-ies。-ies 后接正则表达式，
		解释错误或者无法解释的SQL如果匹配-ies指定的正则表达式， 则my2fback不会退出而是跳过该SQL继续解释后面的binlog， 否则错误退出。
		-ies后接的正则表达式请使用小写字母, 因为my2fback会先把SQL转成小写再与之匹配。
    ```
# 安装与使用
    1)安装
        如果需要编译， 请使用GO>=1.11.x版本来编译。
        1. 开启GO111MODULE参数
         * 编译linux 平台
             1) 执行编译命令
                `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o releases/my2fback -ldflags "-s -w" main.go`
         * 编译windows 平台
             1) 执行编译命令
                `CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o releases/my2fback -ldflags "-s -w" main.go`
                                
        
        2. 没有开启GO111MODULE参数
         * 编译linux 平台
             `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o releases/my2fback -ldflags "-s -w" main.go`
         * 编译windows 平台
             `CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o releases/my2fback -ldflags "-s -w" main.go`
               
    2)下载
        有编译好的linux与window二进制版本
        [Linux](https://github.com/WangJiemin/my2fback/blob/master/releases/my2fback)
        [windows](https://github.com/WangJiemin/my2fback/blob/master/releases/my2fback.exe)
        
    3）使用
        *生成前滚SQL与DML报表:
            /usr/local/bin/my2fback -m repl -w 2sql -M mysql -t 4 -mid 3331 -H 127.0.0.1 -P 3306 -u xxx -p xxx -dbs db1,db2 -tbs tb1,tb2 -sbin mysql-bin.000556 -spos 107 -ebin mysql-bin.000559 -epos 4 -e -f -r 20 -k -b 100 -l 10 -o /home/apps/tmp
        *生成回滚SQL与DML报表:
            /usr/local/bin/my2fback -m file -w rollback -M mysql -t 4 -H 127.0.0.1 -P 3306 -u xxx -p xxx -dbs db1,db2 -tbs tb1,tb2 -tbs tb1,tb2 -sdt "2017-09-28 13:00:00" -edt "2017-09-28 16:00:00" -e -f -r 20 -k -b 100 -l 10  -o /home/apps/tmp  mysql-bin.000556
        *只生成DML报表:
            /usr/local/bin/my2fback -m file -w stats -M mysql -i 20 -b 100 -l 10 -o /home/apps/tmp mysql-bin.000556

# TODO
- [x] file方式解析binlog
- [x] file方式回滚binlog
- [x] repl方式解析binlog
- [x] repl方式回滚binlog
- [x] DML统计信息
- [x] 大事务与长事务统计信息
- [x] DDL统计信息
- [x] 测试




