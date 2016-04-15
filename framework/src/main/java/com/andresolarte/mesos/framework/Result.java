package com.andresolarte.mesos.framework;

import java.io.Serializable;

public class Result implements Serializable {
    private long total = 1;
    private String digitSequence;


    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public String getDigitSequence() {
        return digitSequence;
    }

    public void setDigitSequence(String digitSequence) {
        this.digitSequence = digitSequence;
    }

    public void addDigit(int newDigit) {
        total = total * newDigit;
        digitSequence = digitSequence + newDigit;
    }

    public void combine(Result result) {
        total = total * result.getTotal();
        digitSequence = digitSequence + result.digitSequence;
    }


}
