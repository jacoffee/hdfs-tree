## HDFS-Tree

hdfs-tree is a utility tool to display hdfs directory in tree structure, meanwhile you can sort directory by size/file count/mtime. 
This is useful when you want to check the disk usage of hdfs, just like what we do in bash with **du -h --max-depth=1 / | sort -hr | head** to check the linux file system disk usage. 

## Build

1. git clone https://github.com/jacoffee/hdfs-tree.git

2. cd ${Project_HOME} && mvn clean -DskipTests package

3. cd ${Project_HOME}/target && java -jar hdfs-tree_2.10-1.0.jar xxx

## Documentation

Possible parameters you can pass with HDFSTree are as follows:

```bash
Usage: hdfs-tree [OPTION]
Options:

  -d, --depth  <arg>   max traverse depth for directory, default 5
  -l, --limit  <arg>   top N to display by sort, default 5
  -r, --reverse        reverse the sort or not
  -s, --sort  <arg>    sort options, must be size | mtime | count(file count)
  -t, --type  <arg>    file type to compare, d | f
      --help           Show help message
```

However, if you want to traverse the root directory, you should use **hdfs://localhost/** instead of **hdfs://localhost**。 As for the reason, please check
**org.apache.hadoop.hdfs.DistributedFileSystem.getPathName** and everything will be clear.

In hadoop cluster that supports namenode ha, you should pass the hadoop configuration directory in the class path, i.e

```bash
java -cp ${HADOOP_CONF_DIR}:hdfs-tree_2.10-1.0.jar com.jacoffee.HDFSTree $@
```

Also, you can configure two environment variables: ${HDFS_CONF_DIR}、${HDFS_TREE_HOME} and use the script **hdfs-tree** in ${HDFS_TREE_HOME}/bin directly.

### 1. display directory sort by size desc

```bash
java -jar hdfs-tree_2.10-1.0.jar -r -l 3 -d 2 -s size hdfs://localhost/
```

```bash
hdfs://localhost/

├── [       2GB] user
	├── [       2GB] allen
	├── [     526MB] hive
	├── [       1KB] platform
├── [     709MB] tmp
	├── [     709MB] hadoop-yarn
	├── [        0B] hive
├── [      99MB] spark
	├── [      99MB] eventLog
```

### 2. display directory and sort by filecount desc

```bash
java -jar hdfs-tree_2.10-1.0.jar -r -l 3 -d 2 -s count hdfs://localhost/
```

```bash
hdfs://localhost/
├── [       692] spark
	├── [       692] eventLog
├── [       392] tmp
	├── [       392] hadoop-yarn
	├── [         0] hive
├── [       301] user
	├── [       201] hive
	├── [        99] allen
	├── [         1] platform
```


### 2. display directory and sort by mtime desc

```bash
java -jar hdfs-tree_2.10-1.0.jar -r -l 3 -d 2 -s mtime hdfs://localhost/
```

```bash
hdfs://localhost/
├── [2018-02-05 09:44:07] user
	├── [2018-04-02 22:50:19] allen
	├── [2018-02-05 09:44:07] platform
	├── [2017-08-06 11:18:50] hive
```

