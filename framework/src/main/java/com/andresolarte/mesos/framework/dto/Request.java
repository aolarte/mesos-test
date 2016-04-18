package com.andresolarte.mesos.framework.dto;

import java.io.Serializable;

/**
 * Simple DTO to send a request to a slave.
 */
public class Request implements Serializable {
    private int numberOfDigits;
    private int[] digitSequence;

    public Request(int[] digitSequence, int numberOfDigits) {
        this.numberOfDigits = numberOfDigits;
        this.digitSequence = digitSequence;
    }

    public int[] getDigitSequence() {
        return digitSequence;
    }

    public int getNumberOfDigits() {
        return numberOfDigits;
    }

}
