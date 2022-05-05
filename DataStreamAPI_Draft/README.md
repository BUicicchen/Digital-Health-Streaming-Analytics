NOTE: the code files here are our previous drafts which are kept as references, DO NOT run them.



Stateful Function running instructions:

Stateful Function implementations are in branch `docker` (query 1 and 4 by Cici) and `docker2` (query 2 and 3 by Yumiao).

DataStream API running instructions:

To execute the currently finished DataStream API programs, the following files in the `DataStreamAPI` folder of the `master` branch cover all the executable tasks:

- templateJoin.java\
Test for joining two data streams through tumbling event time window.

- templateQ1.java\
Test for identifying sessions for a data steam. When the value of the attribute of interest is above a certain threshold, the session starts and the following tuples will continue to be put in this session until a non-qualified tuple (the attribute of interest of that tuple is below the threshold) is encountered and the session ends. The next session will be triggered if another qualified tuple is encountered.

- templateQ2Q4.java\
Test for two functionalities:\
(1) Maintain 3 consecutive windows, and keep tracking on their averages values with respect to a certain attribute.\
(2) Raise a warning message if the attribute of interest increases by more than a threshold percentage within a certain period of time.

Currently all the programs can only be run locally through IntelliJ. When you run the programs, make sure to check the Run/Debug Configurations. Navigate to `Modify options` and select `Add dependencies with "provided" scope to classpath`. All the results will be printed in the IntelliJ terminal.

Note:
- When running the programs, make sure to put all the ".java" files in the same folder.
- The custom datatypes for different devices are not used yet.
