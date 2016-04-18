package com.andresolarte.mesos.framework;

import com.andresolarte.mesos.framework.logic.LargestProductFinder;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import java.util.logging.Logger;

/**
 * The Executor is tha class that actually does the job that the framework initiates.
 * There will be one Executor per slave node.
 */
public class TestExecutor implements Executor {
    private final static Logger LOGGER = Logger.getLogger(TestExecutor.class.getName());

    public static void main(String[] args) throws Exception {
        MesosExecutorDriver driver = new MesosExecutorDriver(new TestExecutor());
        System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
    }

    @Override
    public void registered(ExecutorDriver driver,
                           ExecutorInfo executorInfo,
                           FrameworkInfo frameworkInfo,
                           SlaveInfo slaveInfo) {
        LOGGER.info("Registered executor on " + slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, SlaveInfo executorInfo) {
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
        new Thread(
                new LargestProductFinder(task, driver)).start();
    }

    @Override
    public void killTask(ExecutorDriver driver, TaskID taskId) {
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
    }

}
