package com.andresolarte.mesos.framework;

import com.andresolarte.mesos.framework.dto.Request;
import com.andresolarte.mesos.framework.dto.Result;
import com.andresolarte.mesos.framework.logic.LargestProductCoordinator;
import com.andresolarte.mesos.framework.util.ByteStringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * The Scheduler will receive offers from available slaves in the Mesos cluster, and will send out tasks.
 * The Scheduler receives different status messages from the Executors running on the slaves
 */
public class TestScheduler implements Scheduler {
    private final static Logger LOGGER = Logger.getLogger(TestScheduler.class.getName());
    private static double CPUS_PER_TASK = 1;
    private static double MEM_PER_TASK = 32;
    //Configuration variables
    private final boolean implicitAcknowledgements;
    private final Protos.ExecutorInfo executor;
    //Task control variables
    private int launchedTasks = 0;
    private int finishedTasks = 0;

    private LargestProductCoordinator largestProductCoordinator;


    public TestScheduler(boolean implicitAcknowledgements,
                         Protos.ExecutorInfo executor) {
        this.implicitAcknowledgements = implicitAcknowledgements;
        this.executor = executor;
        largestProductCoordinator = new LargestProductCoordinator();
        largestProductCoordinator.setupTasks();
    }


    @Override
    public void registered(SchedulerDriver driver,
                           Protos.FrameworkID frameworkId,
                           Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework registered with ID = " + frameworkId.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
    }

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Protos.Offer> offers) {


        for (Protos.Offer offer : offers) {
            reviewOfferAndLaunchTask(driver, offer);
        }
    }

    private void reviewOfferAndLaunchTask(SchedulerDriver driver, Protos.Offer offer) {


        LOGGER.info(
                "Received offer from slave: " + offer.getSlaveId());


        if (launchedTasks < largestProductCoordinator.getTotalTasks()) {
            Request request = largestProductCoordinator.createRequest(launchedTasks);
            Protos.Offer.Operation.Launch.Builder launch = Protos.Offer.Operation.Launch.newBuilder();

            Protos.TaskID taskId = Protos.TaskID.newBuilder()
                    .setValue(Integer.toString(launchedTasks++)).build();

            LOGGER.info("Launching task " + taskId.getValue() +
                    " using offer " + offer.getId().getValue());

            Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                    .setName("task " + taskId.getValue())
                    .setTaskId(taskId)
                    .setSlaveId(offer.getSlaveId())
                    .addResources(Protos.Resource.newBuilder()
                            .setName("cpus")
                            .setType(Protos.Value.Type.SCALAR)
                            .setScalar(Protos.Value.Scalar.newBuilder().setValue(CPUS_PER_TASK)))
                    .addResources(Protos.Resource.newBuilder()
                            .setName("mem")
                            .setType(Protos.Value.Type.SCALAR)
                            .setScalar(Protos.Value.Scalar.newBuilder().setValue(MEM_PER_TASK)))
                    .setExecutor(Protos.ExecutorInfo.newBuilder(executor))
                    .setData(ByteStringUtils.toByteString(request))
                    .build();

            launch.addTaskInfos(Protos.TaskInfo.newBuilder(task));
            List<Protos.OfferID> offerIds = new ArrayList<>();
            offerIds.add(offer.getId());

            List<Protos.Offer.Operation> operations = new ArrayList<>();

            Protos.Offer.Operation operation = Protos.Offer.Operation.newBuilder()
                    .setType(Protos.Offer.Operation.Type.LAUNCH)
                    .setLaunch(launch)
                    .build();

            operations.add(operation);

            Protos.Filters filters = Protos.Filters.newBuilder().setRefuseSeconds(1).build();

            driver.acceptOffers(offerIds, operations, filters);

        }


    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.info("Status update: task " + status.getTaskId().getValue() +
                " is in state " + status.getState().getValueDescriptor().getName());
        if (status.getState() == Protos.TaskState.TASK_FINISHED) {
            finishedTasks++;
            LOGGER.info("Finished tasks: " + finishedTasks);
            Result result = ByteStringUtils.fromByteString(status.getData());
            largestProductCoordinator.ingestResult(result);
        }


        if (status.getState() == Protos.TaskState.TASK_LOST ||
                status.getState() == Protos.TaskState.TASK_KILLED ||
                status.getState() == Protos.TaskState.TASK_FAILED ||
                status.getState() == Protos.TaskState.TASK_ERROR) {
            LOGGER.severe("Aborting because task " + status.getTaskId().getValue() +
                    " is in unexpected state " +
                    status.getState().getValueDescriptor().getName() +
                    " with reason '" +
                    status.getReason().getValueDescriptor().getName() + "'" +
                    " from source '" +
                    status.getSource().getValueDescriptor().getName() + "'" +
                    " with message '" + status.getMessage() + "'");
            driver.abort();
        }

        if (!implicitAcknowledgements) {
            driver.acknowledgeStatusUpdate(status);
        }

        if (finishedTasks == largestProductCoordinator.getTotalTasks()) {
            largestProductCoordinator.outputResult();
            driver.stop();
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 Protos.ExecutorID executorId,
                                 Protos.SlaveID slaveId,
                                 byte[] data) {
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
    }

    @Override
    public void executorLost(SchedulerDriver driver,
                             Protos.ExecutorID executorId,
                             Protos.SlaveID slaveId,
                             int status) {
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.severe("Error: " + message);
    }


}
