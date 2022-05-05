Running instructions:

The formally submitted code for DataStream API can be found in the folder `project4` in the master branch.
Currently all the programs can only be run locally through IntelliJ. When you run the programs, make sure to check the `Run/Debug Configurations`. Navigate to Modify options and select `Add dependencies with "provided" scope to classpath`. All the results will be printed in the IntelliJ terminal.

For the query configurations, all the parameters that the queries use are stored as static variables inside of the Java code files, you can directly find them (e.g., `Query1.java`) and they all have explanatory comments.

The default data generator is `DataGenerator.java`, besides generating data as usual, it can also load/output generated tuples from/to CSV files. Additionally, `DataGenFix.java` contains some hard-coded data.

To build the .jar file, go to the pom.xml configuration and find the following line (make sure to have the Maven clean package command):\
`<mainClass>clusterdata.Query4</mainClass>`\
change the name of the query to the one you want to run.

To run the job on the Flink dashboard, use the following terminal commands:\
`$ ./bin/start-cluster.sh`\
`$ ./bin/flink run <filename>.jar`\
`$ ./bin/stop-cluster.sh`\
You can find the query results in the log files generated.\
There are built .jar files for each query which can be directly found in the folder `project4`, these jobs use the default query settings.

For the DataStream API experiments, I tested all the queries. Each data source will produce 50± tuples. The test is conducted locally. During the data generation, I used the sleep option (150 milliseconds) to slow down the data stream for better visualization. For all the sources, I only generated data for 1 patient (it is possible to process data for multiple patients). For all the out-of-order data, I set the out-of-order-ness to be 10, where for every 10 tuples, the timestamp order is shuffled. I did not test the case where the increase in timestamp fluctuates. 

Here are the query response time results:

Query 1: 2 streams, 100 tuples in total\
Uniform distribution:\
Out-of-order: 14.286234 seconds\
In-order: 12.779635 seconds\
Exponential distribution:\
Out-of-order: 14.636771 seconds\
In-order: 12.855877 seconds

Query 2: 2 streams, 100 tuples in total\
Uniform distribution:\
Out-of-order: 12.864382 seconds\
In-order: 12.9195 seconds

Query 3: 2 streams, 100 tuples in total\
Uniform distribution:\
Out-of-order: 12.895523 seconds\
In-order: 12.8302555 seconds

Query 4: 3 streams, 150± tuples in total\
Uniform distribution:\
Out-of-order: 14.456437 seconds\
In-order: 12.995051 seconds

From these tests, we can see that for query 1 and 4, out-of-order data takes more time to be processed than in-order data, but in query 2 and 3, out-of-order and in-order data resulted in similar query performances.

Possible improvements/issues:
- There are some limitations in terms of extendibility and reusability of the code, for example, the co-process functions may not necessarily work for all types of tuples, they only work for tuples that are involved in the query computations in this particular problem set. These can be improved so that the program can become more object-oriented and flexible.
- For Flink DataStream API, the implementation of Kafka stream may not work well. Therefore, the default Flink data stream sink should be prioritized when testing. Use caution when switching to Kafka.
- For query 1, it is now possible to process out-of-order data by scheduling timer, and the sessions can be identified for one stream successfully, but for the other stream which participates in the connection, since the way its sessions are identified entirely depends on the 1st stream, I did not figure out how to use timer to handle out-of-order data for it, therefore I used a priority queue as a workaround, which can harm the processing efficiency.
- In query 2, I found that I do not need to use timer or priority queue, because as far as I can define the bounded out-of-order-ness, the tumbling window can put the tuples in the correct windows regardless of their order of arrival (although the tuples may still be out-of-order within a window, but that is not an issue for query 2).
- Also, in some of the queries, when processing data, we should consider filtering out the unneeded columns/attributes since delivering unnecessary data can incur more overhead.
- In query 4, when 3 data streams are connected, oftentimes the messages of the rise and fall of glucose will be shown before the actual data of glucose being shown. Also, when the total amount of tuples is bounded, my current implementation of query 4 may not process all the insulin data generated (sometimes the opposite happens, where more insulin tuples than expected are output), some insulin data at the end gets lost due to my flawed waitlist mechanic.
- There is a problem which exists at least in query 1. When the number of tuples generated is finite, the quantity must be multiples of 10, otherwise the remainder tuples will be lost. The reason is still unclear, presumably it has to do with the priority queue buffer. Therefore, as a possible improvement, all the buffer queues and waitlists must be automatically purged when there is a singal telling that the data streams are ended.
- Note that the hard-coded data in `DataGenFix.java` was generated by an older version of data generator before it got debugged, and the experiment results from the demo presentation used the data here. In the current version of query 1, a new problem can be identified where it is no more possible to replicate the demo results possibly due to the reasons stated in the previous point.
- One suggestion to compare the performances of Flink DataStream API and StateFun is to compute the query throughput from the performance data we have, where you divide the total number of tuples processed by the runtime. Note that an interfering factor is that when data was being generated, it underwent some sleep time which slowed it down.
