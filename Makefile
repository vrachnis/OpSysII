JC = /usr/bin/javac
JAR = /usr/bin/jar
MKDIR = /bin/mkdir -p
SCP = /usr/bin/scp

# Project title
TITLE = TfIdf
REMOTE_HOST = romo.ceid.upatras.gr
HADOOP_HOME = ${HOME}/hadoop
HBASE_HOME = ${HOME}/hbase
CASSANDRA_HOME = ${HOME}/cassandra

# The classes that need compiling
CLASSES = HDSearch.java \
	  Sort.java \
	  HBSave.java \
	  CaSave.java

CLIENT = CliSearch.java

# This only works for 1 package.
# TODO: fix for 2 or more class files
#PACKAGE := $(shell head -1 $(CLASSES) | cut -d" " -f2 | sed 's/\./\//g; s/;//')
PACKAGE = gr/upatras/ceid/romo
BYTECODE := $(patsubst %.java, $(TITLE)_classes/$(PACKAGE)/%.class, $(CLASSES))

CLASSPATH = $(HBASE_HOME)/hbase-0.20.6.jar:$(HADOOP_HOME)/hadoop-0.20.2-core.jar:$(HADOOP_HOME)/lib/commons-logging-1.0.4.jar:$(HBASE_HOME)/lib/zookeeper-3.2.2.jar:$(HBASE_HOME)/lib/log4j-1.2.15.jar:$(CASSANDRA_HOME)/lib/apache-cassandra-0.8.0.jar:$(CASSANDRA_HOME)/lib/apache-cassandra-thrift-0.8.0.jar:$(CASSANDRA_HOME)/lib/libthrift-0.6.jar:$(CASSANDRA_HOME)/lib/slf4j-api-1.6.1.jar:.

.PHONY: send clean jar

all: $(TITLE)_classes $(TITLE)_classes/$(BYTECODE)

jar: $(TITLE).jar

$(TITLE)_classes/$(PACKAGE)/%.class: $(PACKAGE)/%.java
	$(JC) -Xlint:unchecked -classpath $(HADOOP_HOME)/hadoop-0.20.2-core.jar:$(HADOOP_HOME)/lib/commons-cli-1.2.jar:$(HBASE_HOME)/hbase-0.20.6.jar:$(CASSANDRA_HOME)/lib/apache-cassandra-0.8.0.jar:$(CASSANDRA_HOME)/lib/apache-cassandra-thrift-0.8.0.jar:. -d $(TITLE)_classes $<

$(TITLE)_classes:
	$(MKDIR) $(TITLE)_classes

%.jar: $(TITLE)_classes $(TITLE)_classes/$(BYTECODE)
	$(JAR) -cvf $(TITLE).jar -C $(TITLE)_classes/ .

send: $(TITLE).jar
	$(SCP) $(TITLE).jar $(REMOTE_HOST):

CliSearch.class: CliSearch.java
	$(JC) -Xlint:unchecked -classpath $(HBASE_HOME)/hbase-0.20.6.jar:$(HADOOP_HOME)/hadoop-0.20.2-core.jar:$(HADOOP_HOME)/lib/commons-cli-1.2.jar:$(CASSANDRA_HOME)/lib/apache-cassandra-0.8.0.jar:$(CASSANDRA_HOME)/lib/apache-cassandra-thrift-0.8.0.jar:$(CASSANDRA_HOME)/lib/libthrift-0.6.jar:. $(CLIENT)

example: CliSearch.class
	@echo
	@echo "###################################################"
	@echo "# Running a query for \"zip 00\" on cassandra"
	@echo "###################################################"
	@CLASSPATH=$(CLASSPATH) java CliSearch cassandra romo.ceid.upatras.gr zip 00 2>/dev/null
	@echo
	@echo "###################################################"
	@echo "# Running a query for \"zip 00\" on hbase"
	@echo "###################################################"
	@CLASSPATH=$(CLASSPATH) java CliSearch hbase romo.ceid.upatras.gr zip 00 2>/dev/null
	@echo

clean:
	$(RM) -r $(TITLE)_classes
	$(RM) $(TITLE).jar
	$(RM) *.class