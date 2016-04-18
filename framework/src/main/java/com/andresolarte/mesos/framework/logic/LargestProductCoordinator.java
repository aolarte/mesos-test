package com.andresolarte.mesos.framework.logic;

import com.andresolarte.mesos.framework.dto.Request;
import com.andresolarte.mesos.framework.dto.Result;

import java.util.Arrays;
import java.util.logging.Logger;

/**
 * This object will slice up the problem into multiple tasks.
 * It will also inject the results as they start to come in from the slaves, looking for the result with the highest total.
 * This will run as part of the Scheduler
 */
public class LargestProductCoordinator {

    private final static Logger LOGGER = Logger.getLogger(LargestProductCoordinator.class.getName());

    private int[] digits;
    private Result highestResult = null;
    private int numberOfDigits = 13;
    private int digitsPerTask = 20; //13*1.5 rounded up to ensure enough overlap to cover all cases with maximum parallelism
    private int totalTasks;

    public void setupTasks() {
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

    public void ingestResult(Result result) {
        LOGGER.info("Received result with total: " + result.getTotal());
        //We received a result, let's see if it's higher than the one we currently hold.
        if (highestResult == null || result.getTotal() > highestResult.getTotal()) {
            highestResult = result;
        }
    }

    public void outputResult() {
        LOGGER.info("================= DONE. Highest result total received: " + highestResult.getTotal());
    }

    public Request createRequest(int requestIndex) {
        int startIndex = requestIndex * numberOfDigits;
        int endIndex = startIndex + digitsPerTask;
        if (endIndex > (digits.length - 1)) {
            endIndex = digits.length - 1;
        }
        Request request = new Request(Arrays.copyOfRange(digits, startIndex, endIndex), numberOfDigits);
        return request;
    }

    public int getTotalTasks() {
        return totalTasks;
    }
}
