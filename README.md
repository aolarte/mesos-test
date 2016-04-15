# Apache Mesos test

This quick example will examine a long number and find the n consecutive numbers with the largest sum. The example leverages Apache Mesos, which is an open source cluster management system. With mesos, the task is split among several slave nodes to speed up the process.

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

## The long of it

The solution is composed of a custom Mesos Framework written in Java, and a VM hosting a Mesos master and slave to run the framework.
The "framework" package provides 3 main Java classes:

* Framework: The framework configures the different parts of the system, such as the Scheduler and the Executor
* Scheduler: The Scheduler will distribute the tasks as slaves become available
* Executor: The class that will perform the requested task. The Executer will run in each of the slaves that is committed to a task

In this case the Excutor will receive a part of the problem as data when starting the task.  Once it has solved its piece of the problem it will return its answer to the Scheduler, inside of a TaskStatus message. The Scheduler will examine each of the results to find the one with the highest total.  Compared to a Map/Reduce solution, the Executor is doing the Map role, and the Scheduler is doing the Reduce role.

Apache Mesos and the required dependencies will be automatically installed and started.  The Mesos console will be available at [http://127.0.0.1:5050/](http://127.0.0.1:5050/)

The VM will only spin up one slave. Other slaves can be spun up manually:

    vagrant ssh
    sudo -s
    mesos-slave --master=127.0.0.1:5050 --port=5052 --work_dir=/tmp/mesos2

(make sure to use a different port number and work directory for each slave)


### TODO

* Slave and Executor failure is not handled
* Remote slaves won't work without some reconfiguration of Mesos. Mesos is using 127.0.0.1 for its transport.

This example is based on the Java example provided with the Apache Mesos distribution.