package org.jlab.clas.std.services.util;

import java.math.BigInteger;
import java.util.Set;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;

public class BigFactorial implements Engine {

    private static final String NAME = "BigFactorial";
    private static final int DEFAULT_N = 6000;

    private volatile int factorial = DEFAULT_N;

    @Override
    public EngineData configure(EngineData input) {
        String mimeType = input.getMimeType();
        if (mimeType.equalsIgnoreCase(EngineDataType.STRING.mimeType())) {
            String value = (String) input.getData();
            int tmp = isNumber(value);
            if (tmp > 0) {
                factorial = tmp;
                System.out.println(NAME + " service: configuring factorial = " + factorial + "!");
            } else {
                System.err.println(NAME + " service: wrong configuration value = " + value);
            }
        } else if (mimeType.equalsIgnoreCase(EngineDataType.SFIXED32.mimeType())) {
            factorial = (Integer) input.getData();
            System.out.println(NAME + " service: configuring factorial = " + factorial + "!");
        } else {
            System.err.println(NAME + " service: wrong configuration mime-type = " + mimeType);
        }
        return null;
    }

    private int isNumber(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    @Override
    public EngineData execute(EngineData input) {
        BigInteger fact = BigInteger.valueOf(1);
        for (int i = 1; i <= factorial; i++) {
            fact = fact.multiply(BigInteger.valueOf(i));
        }
        return input;
    }

    @Override
    public EngineData executeGroup(Set<EngineData> inputs) {
        return null;
    }

    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.STRING,
                                        EngineDataType.SFIXED32,
                                        Clas12Types.EVIO);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(Clas12Types.EVIO);
    }

    @Override
    public Set<String> getStates() {
        return null;
    }

    @Override
    public String getDescription() {
        return "Calculates a big factorial n! to simulate a service with intensive CPU usage.";
    }

    @Override
    public String getVersion() {
        return "1.0";
    }

    @Override
    public String getAuthor() {
        return "Sebastián Mancilla  <smancill@jlab.org>";
    }

    @Override
    public void reset() {
        factorial = DEFAULT_N;
    }

    @Override
    public void destroy() {
        // nothing
    }
}
