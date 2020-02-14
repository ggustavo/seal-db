# seal-db
Prototype of a Database Management System (DBMS)

##  Download
Clone the project or [download](https://github.com/ggustavo/seal-db/archive/master.zip) directly (unzip the file)
```shell
git clone https://github.com/ggustavo/seal-db.git
```

## Build and Execution
An easy way is build and run as eclipse project

1- First [download](https://www.eclipse.org/downloads/) eclipse IDE

2- Open the eclipse and follow the instructions: File-> Import-> General-> Existing Project into Workspace. Choose the seal-db directory and finish

3- Look for a main class in the [tests](/src/tests) package to run. For example [TestGraphicInterface.java](/src/tests/TestGraphicInterface.java)

## Exploring seal-db GUI

Choose some settings, for example the buffer size (in pages), buffer policy, among others.

![alt text](https://github.com/ggustavo/seal-db/raw/master/GUI-examples/settings.png "Settings")

Open new connections in different schemas and execute queries.

![alt text](https://github.com/ggustavo/seal-db/raw/master/GUI-examples/connection.png "Connection")

The buffer manager window are updated in real time according to queries submitted. Dirty (modified) pages and non-dirty (not modified) pages are depicted in colors orange and yellow respectively, whereas hot pages and cold pages takes colors red and blue respectively, in LRU list. In addition, hit count and miss count as well as hit ratio are always computed.

![alt text](https://github.com/ggustavo/seal-db/raw/master/GUI-examples/buffer.png "Buffer Manager")

The transaction manager window shows operations submitted by each transaction, execution history (schedule) and the serialization graph.

![alt text](https://github.com/ggustavo/seal-db/raw/master/GUI-examples/transaction.png "Transaction Manager")

The recovery manager window, which shows log file. The log records capture all database operations. A log miner facility is also available to enable database learner to visualize the sort of data a log record contains, including after and before images of updated pages (left bottom side).

![alt text](https://github.com/ggustavo/seal-db/raw/master/GUI-examples/recovery.png "Reconvery Manager")

There are three ways to express a query: 

* Graphically by dragging query operator icons from icon palette and dropping them to plan view area (central region of the query window) 

* By writing a relational calculus expression 

* By writing a SQL statement

![alt text](https://github.com/ggustavo/seal-db/raw/master/GUI-examples/query.png "Query Processor")


