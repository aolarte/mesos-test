package com.andresolarte.mesos.framework;

import java.io.Serializable;

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
