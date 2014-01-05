HBase-Research
==============

[HBase](http://hbase.apache.org/)数据库源代码学习研究(包括代码注释、文档、用于代码分析的测试用例)


## 使用的HBase版本

保持与官方的[0.94](http://svn.apache.org/repos/asf/hbase/branches/0.94/)版本同步


## 构建与运行环境

需要JDK7以及[Apache Maven](http://maven.apache.org/)


## 生成Eclipse工程

mvn eclipse:eclipse <br><br>

在eclipse中导入 <br>
File->Import->General->Existing Projects into Workspace


## 启动HBase

在eclipse中先打开/hbase/src/test/java/my/test/start/HMasterStarter.java，<br>
然后按Ctrl+F11运行它，<br>
当看到提示Waiting for region servers count to settle时，<br>
再打开同目录中的HRegionServerStarter，同样按Ctrl+F11运行它，<br>
此时会有两个Console，<br>
在HMasterStarter这个Console最后出现Master has completed initialization这样的信息时就表示它启动成功了，<br>
而HRegionServerStarter这个Console最后出现Done with post open deploy task这样的信息时说明它启动成功了。<br>
