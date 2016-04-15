package com.andresolarte.mesos.framework;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

class TestScheduler implements Scheduler {
    private final static Logger LOGGER = Logger.getLogger(TestScheduler.class.getName());
    private static double CPUS_PER_TASK = 1;
    private static double MEM_PER_TASK = 128;
    //Configuration variables
    private final boolean implicitAcknowledgements;
    private final Protos.ExecutorInfo executor;
    //Task control variables
    private int totalTasks;
    private int launchedTasks = 0;
    private int finishedTasks = 0;

    //Business logic variables
    private int[] digits;
    private Result highestResult = null;
    private int numberOfDigits = 13;
    private int digitsPerTask = 20; //13*1.5 rounded up to ensure enough overlap to cover all cases


    public TestScheduler(boolean implicitAcknowledgements,
                         Protos.ExecutorInfo executor) {
        this(implicitAcknowledgements, executor, -1);
    }

    public TestScheduler(boolean implicitAcknowledgements,
                         Protos.ExecutorInfo executor,
                         int totalTasks) {
        this.implicitAcknowledgements = implicitAcknowledgements;
        this.executor = executor;
        setupTasks();
    }

    public static void main(String[] args) {
        new TestScheduler(false, null).setupTasks();
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


        if (launchedTasks < totalTasks) {
            Request request = createRequest(launchedTasks);
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
            ingestResult(result);
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

        if (finishedTasks == totalTasks) {
            outputResult();
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

    //Business logic methods
    private void setupTasks() {
        String input = "73167176531330624919225119674426574742355349194934" +
                "96983520312774506326239578318016984801869478851843" +
                "85861560789112949495459501737958331952853208805511" +
                "12540698747158523863050715693290963295227443043557" +
                "66896648950445244523161731856403098711121722383113" +
                "62229893423380308135336276614282806444486645238749" +
                "30358907296290491560440772390713810515859307960866" +
                "70172427121883998797908792274921901699720888093776" +
                "65727333001053367881220235421809751254540594752243" +
                "52584907711670556013604839586446706324415722155397" +
                "53697817977846174064955149290862569321978468622482" +
                "83972241375657056057490261407972968652414535100474" +
                "82166370484403199890008895243450658541227588666881" +
                "16427171479924442928230863465674813919123162824586" +
                "17866458359124566529476545682848912883142607690042" +
                "24219022671055626321111109370544217506941658960408" +
                "07198403850962455444362981230987879927244284909188" +
                "84580156166097919133875499200524063689912560717606" +
                "05886116467109405077541002256983155200055935729725" +
                "71636269561882670428252483600823257530420752963450";
        digits = input.chars()
                .map(Character::getNumericValue).toArray();

        float numberOfDigitsFloat = numberOfDigits;
        float totalTasksFloat = digits.length / numberOfDigitsFloat;
        this.totalTasks = (int) Math.ceil(totalTasksFloat);
        LOGGER.info("Total number of tasks for the input: " + totalTasks + " " + totalTasksFloat);
    }

    private void ingestResult(Result result) {
        LOGGER.info("Received result with total: " + result.getTotal());
        //We received a result, let's see if it's higher than the one we currently hold.
        if (highestResult == null || result.getTotal() > highestResult.getTotal()) {
            highestResult = result;
        }
    }

    private void outputResult() {
        LOGGER.info("================= DONE. Highest result total received: " + highestResult.getTotal());
    }

    private Request createRequest(int requestIndex) {
        int startIndex = requestIndex * numberOfDigits;
        int endIndex = startIndex + digitsPerTask;
        if (endIndex > (digits.length - 1)) {
            endIndex = digits.length - 1;
        }
        Request request = new Request(Arrays.copyOfRange(digits, startIndex, endIndex), numberOfDigits);
        return request;
    }
}
