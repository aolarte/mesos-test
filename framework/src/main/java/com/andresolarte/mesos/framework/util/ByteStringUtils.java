package com.andresolarte.mesos.framework.util;

import com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ByteStringUtils {

    private final static Logger LOGGER = Logger.getLogger(ByteStringUtils.class.getName());


    public static byte[] toBytes(Serializable res) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(res);
            out.close();
            return bos.toByteArray();

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    public static <T> T fromBytes(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try {
            ObjectInputStream in = new ObjectInputStream(bis);
            Object o = in.readObject();
            return (T) o;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;

    }

    public static ByteString toByteString(Serializable result) {
        return ByteString.copyFrom(toBytes(result));
    }

    public static <T> T fromByteString(ByteString byteString) {
        return fromBytes(byteString.toByteArray());
    }
}
