# scadb

scadb是一个轻量级的高性能MySQL中间件产品，可以支持MySQL分库分表，以及读写分离等功能，同时还致力于解决跨区域的数据库的高可用性的问题。与此同时，scadb还提供了功能强大的导入和导出功能，实现了强大的数据管理功能。

主要功能包括：
1、读写分离
2、分库分表
3、数据导入和导出
4、机房感知功能
5、跨机房的高可用性




# 一、安装准备

1、安装好mysql
2、下载JDK1.8的安装文件
3、下载zookeeper
4、下载scadb安装文件，下载地址如下：
http://pan.baidu.com/s/1dEHPKAX


# 二、安装步骤
1、启动MySQL，在mysql上创建scadb用户，如下所示：
mysql -uroot 
mysql > grant all on *.* to scadb identified by 'scadb' ;

2、安装好jdk1.8， 并设置好JAVA_HOME
export JAVA_HOME=<JDK_PATH>

3、启动zookeeper

4、下载的scadb有三个文件，分别是：（假设下载的文件在/tmp目录）
scadb_2.11-1.0.0-bin.tar.gz
scadb-admin_2.11-1.0.0-bin.tar.gz
scadb-tools_2.11-1.0.0-bin.tar.gz
把他们解压缩到/opt/scadb下面:


> # mkdir /opt/scadb
# cd /opt/scadb
# tar xzf /tmp/scadb_2.11-1.0.0-bin.tar.gz
# tar xzf /tmp/scadb-admin_2.11-1.0.0-bin.tar.gz
# tar xzf /tmp/scadb-tools_2.11-1.0.0-bin.tar.gz

# ls -1
scadb_2.11-1.0.0
scadb-admin_2.11-1.0.0
scadb-tools_2.11-1.0.0


这时候我们可以看到在/opt/scadb下面有三个子目录。

5、初始化scadb的运行环境

我们使用scadb-tools中的InitEnv工具来进行初始化，如下所示：
# /opt/scadb/scadb-tools_2.11-1.0.0/bin/scadb org.herry2038.scadb.tools.InitEnv --zk localhost:2181

InitEnv工具有很多参数，每个参数都有缺省的值，对于一个测试环境我们基本上不需要制定任何参数。

6、启动scadb服务进程

nohup /opt/scadb/scadb-admin_2.11-1.0.0/bin/scadb admin &
nohup /opt/scadb/scadb_2.11-1.0.0/bin/scadb server &


# 三、使用scadb的docker镜像

1. 下载镜像
> # docker pull herry2038/scadb

2. 运行镜像
> # docker run -d --name scadb herry2038/scadb

3. 进入镜像
> # sudo docker exec -t -i scadb /bin/bash

在镜像中开始使用scadb，见第4节：使用scadb

四、使用scadb
现在开始我们就可以使用scadb了。

1、 连接到scadb

scadb的缺省端口号是9527
> # mysql -utest -ptest -h127.0.0.1 -P9527


2、 创建表
在命令行输入如下sql语句：

>  /*!scadb:partitionkey=id rule=rule10*/CREATE TABLE a (
`id` BIGINT(20) NOT NULL DEFAULT '0',
`name` VARCHAR(50) NULL DEFAULT NULL,
`t` DATETIME NULL DEFAULT NULL,
`b` BLOB NULL,
PRIMARY KEY (`id`)
)
COLLATE='utf8_bin' ENGINE=InnoDB ;


这时候系统会创建一个表a，它有10个分区表，其中id字段是分区字段。

如果不加上前面的注释，系统会创建一个非分区表。

3、 查看系统中的表

> show tables ;
show create table a ;

4、 插入数据
> insert into a ( id,name ) values ( 1,'123') ;
insert into a ( id,name ) values ( 2,'123') ;
insert into a ( id,name ) values ( 3,'123') ;
insert into a ( id,name ) values ( 4,'123') ;
insert into a ( id,name ) values ( 5,'123') ;
insert into a ( id,name ) values ( 6,'123') ;
insert into a ( id,name ) values ( 7,'123') ;
insert into a ( id,name ) values ( 8,'123') ;
insert into a ( id,name ) values ( 9,'123') ;
insert into a ( id,name ) values ( 10,'123') ;

5、 查询数据
> select id,name from a where id=1 ;

6、 更新数据
> update a set name='234' where id=1 ;

7、 删除数据
> delete from a where id=1 ;
