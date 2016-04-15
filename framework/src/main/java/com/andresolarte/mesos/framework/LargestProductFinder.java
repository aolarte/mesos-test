package com.andresolarte.mesos.framework;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.IntStream;

class LargestProductFinder implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(TestExecutor.class.getName());

    private final Protos.TaskInfo task;
    private final ExecutorDriver driver;

    public LargestProductFinder(Protos.TaskInfo task, ExecutorDriver driver) {
        this.task = task;
        this.driver = driver;
    }

    public void run() {

        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING).build();

        driver.sendStatusUpdate(status);
        try {
            Request request = ByteStringUtils.fromByteString(task.getData());

            LOGGER.info("Running task " + task.getTaskId().getValue());

            Result result = findHighestSequence(request);


            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setData(ByteStringUtils.toByteString(result))
                    .setState(Protos.TaskState.TASK_FINISHED).build();
        } catch (Exception e) {
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setMessage(e.getMessage() + "\n" + getStringFromException(e))
                    .setState(Protos.TaskState.TASK_FAILED).build();
        }

        driver.sendStatusUpdate(status);

    }

    private String getStringFromException(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    private Result findHighestSequence(Request request) {
        int maxInitialIndex = request.getDigitSequence().length - request.getNumberOfDigits();
        Optional<Result> result = IntStream.range(0, maxInitialIndex)
                .mapToObj(i -> Arrays.copyOfRange(request.getDigitSequence(), i, i + request.getNumberOfDigits()))
                .map(x -> Arrays.stream(x).collect(Result::new, Result::addDigit, Result::combine))
                .sorted(Comparator.comparing(Result::getTotal).reversed())
                .findFirst();
        return result.orElse(new Result());
    }


}
