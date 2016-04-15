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

public class TestFramework {
    private final static Logger LOGGER = Logger.getLogger(TestFramework.class.getName());

    private static void usage() {
        String name = TestFramework.class.getName();
        System.err.println("Usage: " + name + " master <tasks>");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
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

        Scheduler scheduler = args.length == 1
                ? new TestScheduler(implicitAcknowledgements, executor)
                : new TestScheduler(implicitAcknowledgements, executor, Integer.parseInt(args[1]));

        MesosSchedulerDriver driver = null;

        frameworkBuilder.setPrincipal("test-framework-java");

        driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], implicitAcknowledgements);

        int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

        // Ensure that the driver process terminates.
        driver.stop();

        // For this test to pass reliably on some platforms, this sleep is
        // required to ensure that the SchedulerDriver teardown is complete
        // before the JVM starts running native object destructors after
        // System.exit() is called. 500ms proved successful in test runs,
        // but on a heavily loaded machine it might not.
        // TODO(greg): Ideally, we would inspect the status of the driver
        // and its associated tasks via the Java API and wait until their
        // teardown is complete to exit.
        Thread.sleep(500);

        System.exit(status);
    }

}
