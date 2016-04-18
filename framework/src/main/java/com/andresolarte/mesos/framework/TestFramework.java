package com.andresolarte.mesos.framework;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;

import java.io.File;
import java.util.logging.Logger;

/**
 * The Framework provides the entry point into the system. It will connect to the Mesos master,
 * and set up the all of the pieces:
 * <ul>
 *     <li>Scheduler: manages tasks, and assigns them based on the offers that come in from avaialbe resources</li>
 *     <li>Executor: the process that will actually execute the task</li>
 *     <li>Driver: establishes the connection with the Mesos master, allowing the Scheduler to hook in to the Mesos cluster.</li>
 * </ul>
 * The framework class doesn't implement any Mesos specific interfaces, and is run like a normal java application with a main method (in production it would be run as a service).
 */
public class TestFramework {
    private final static Logger LOGGER = Logger.getLogger(TestFramework.class.getName());

    private static void usage() {
        String name = TestFramework.class.getName();
        System.err.println("Usage: " + name + " master");
    }

    public static void main(String[] args) throws Exception {
        if (args.length !=1) {
            usage();
            System.exit(1);
        }

        String uri = new File("/vagrant/test-executor").getCanonicalPath();

        ExecutorInfo executor = ExecutorInfo.newBuilder()
                .setExecutorId(ExecutorID.newBuilder().setValue("default"))
                .setCommand(CommandInfo.newBuilder().setValue(uri))
                .setName("Test Executor (Java)")
                .setSource("java_test")
                .build();

        FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
                .setUser("") // Have Mesos fill in the current user.
                .setName("Test Framework (Java)")
                .setCheckpoint(true);

        boolean implicitAcknowledgements = true;

        if (System.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS") != null) {
            LOGGER.info("Enabling explicit acknowledgements for status updates");
            implicitAcknowledgements = false;
        }

        Scheduler scheduler =  new TestScheduler(implicitAcknowledgements, executor);

        MesosSchedulerDriver driver = null;

        frameworkBuilder.setPrincipal("test-framework-java");

        driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], implicitAcknowledgements);

        int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

        // Ensure that the driver process terminates.
        driver.stop();

        System.exit(status);
    }

}
