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

client: $(CLIENT)
	$(JC) -Xlint:unchecked -classpath $(HOME)/hbase/hbase-0.20.6.jar:$(HOME)/hadoop/hadoop-0.20.2-core.jar:$(HOME)/hadoop/lib/commons-cli-1.2.jar:. $(CLIENT)

clean:
	$(RM) -r $(TITLE)_classes
	$(RM) $(TITLE).jar
	$(RM) *.class