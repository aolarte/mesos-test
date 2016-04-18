# Apache Mesos test

This quick example will examine a long number and find the n consecutive numbers with the largest sum. The example leverages Apache Mesos, which is an open source cluster management system. With Mesos, the task is split among several slave nodes to speed up the process.

The example we implemented is [Problem #8](https://projecteuler.net/problem=8) from Project Euler. In practice there's faster ways of solving this problem (Starting up a new task in Apache Mesos is fairly time consuming), but it's a good example to implement in Apache Mesos.

## The short of it

These commands will:
* Build the framework jar. Jar is copied to a location accessible by the VM
* Start up a stock CentOS VM and install Apache Mesos and dependencies
* This packages will bring up one master instance and one slave instance
* Start up the framework.
* The output will printed shortly before the framework finishes executing


    mvn package
    cd vm
    vagrant up
    vagrant ssh -c "/vagrant/run-framework"
    

The final output will look like this:

    Apr 18, 2016 4:27:55 PM com.andresolarte.mesos.framework.logic.LargestProductCoordinator outputResult
    INFO: ================= DONE. Highest result total received: XXXXXXX
    I0418 16:27:55.797878 19639 sched.cpp:1903] Asked to stop the driver
    I0418 16:27:55.798001 19639 sched.cpp:1143] Stopping framework '7267c27c-20eb-4530-890c-750b8d962cb6-0011'
    I0418 16:27:55.798286 19625 sched.cpp:1903] Asked to stop the driver


## The long of it

The solution is composed of a custom Mesos Framework written in Java, and a VM hosting a Mesos master and slave to run the framework.
The "framework" package provides 3 main Java classes:

* Framework: The framework configures the different parts of the system, such as the Scheduler and the Executor
* Scheduler: The Scheduler will distribute the tasks as slaves become available
* Executor: The class that will perform the requested task. The Executor will run in each of the slaves that is committed to a task

In this case the Executor will receive a part of the problem as data when starting the task.  Once it has solved its piece of the problem it will return its answer to the Scheduler, inside of a TaskStatus message. The Scheduler will examine each of the results to find the one with the highest total.  Compared to a Map/Reduce solution, the Executor is doing the Map role, and the Scheduler is doing the Reduce role.

Apache Mesos and the required dependencies will be automatically installed and started.  The Mesos console will be available at [http://127.0.0.1:5050/](http://127.0.0.1:5050/) (the port is forwarded to the Vagrant VM automatically unless there's a conflict)

The VM will only spin up one slave. Having only one slave makes following the process easier. One or two other slaves can be spun up manually:

    vagrant ssh -c "/vagrant/run-slave2"
    vagrant ssh -c "/vagrant/run-slave3"

Each command will spawn a new slave on the foreground.

The VM seems to work fine with up to three slaves at once with the default config. Getting more slaves into a single VM would probably require tweaking some config. Even with 3 slaves sometimes there will be resources exhaustation. It should be noted that Mesos slaves are meant to run one per server.


### Cleanup

To remove your Vagrant virtual machine, remember to:

    vagrant destroy

### TODO

* Slave and Executor failure is not handled
* Remote slaves won't work without some reconfiguration of Mesos. Mesos is using 127.0.0.1 for its transport. The Java code will scale to multiple machines, assuming that framework is installed in the same location (/vagrant).

This example is based on the Java example provided with the Apache Mesos distribution.
