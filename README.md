# 简介
#### my2fback 实现了基于row格式binlog的回滚闪回功能，让误删除或者误更新数据，可以不停机不使用备份而快速回滚误操作。也可以解释binlog（支持非row格式binlog）生成易读的SQL。
 
#### my2fback 连接数据库帐号的权限: 
* MySQL5.6/MariaDB10.1/MariaDB10.2版本
```bash
mysql> GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, PROCESS ON *.* TO 'user'@'localhost' IDENTIFIED BY 'xxxxxx';
```
* MySQL5.7版本
```bash
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'xxxxxx';
mysql> GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, PROCESS ON *.* TO 'user'@'localhost';
```

# 版本 
* 以上功能均可指定任意的单库多库， 单表多表， 任意时间点， 任意binlog位置。
* 支持mysql5.5及以上，也支持MariaDB的binlog， 支持传统复制的binlog， 也支持GTID的binlog。
* 支持直接指定文件路径的binlog， 也支持主从复制， my2fback 作为从库从主库拉binlog来过解释。

# 限制
* 使用回滚/闪回功能时，binlog格式必须为row,且binlog_row_image=full， 其它功能支持非row格式binlog
* 只能回滚DML， 不能回滚DDL
* 支持V4格式的binlog， V3格式的没测试过，测试与使用结果显示，mysql5.1，mysql5.5, mysql5.6与mysql5.7的binlog均支持
* 支持指定-tl时区来解释binlog中time/datetime字段的内容。开始时间-sdt与结束时间-edt也会使用此指定的时区， 
   + 但注意此开始与结束时间针对的是binlog event header中保存的unix timestamp。结果中的额外的datetime时间信息都是binlog event header中的unix timestamp
* decimal字段使用float64来表示， 但不损失精度
* 所有字符类型字段内容按golang的utf8(相当于mysql的utf8mb4)来表示
    
# 安装与使用 
* 下载
   + 有编译好的linux与window二进制版本
   * [Linux](https://github.com/WangJiemin/my2fback/blob/master/releases/my2fback)
   * [windows](https://github.com/WangJiemin/my2fback/blob/master/releases/my2fback.exe)
* 使用
   * [Document]()

# TODO
- [x] file方式解析binlog
- [x] file方式回滚binlog
- [x] repl方式解析binlog
- [x] repl方式回滚binlog
- [x] DML统计信息
- [x] 大事务与长事务统计信息
- [x] DDL统计信息
- [x] 测试




