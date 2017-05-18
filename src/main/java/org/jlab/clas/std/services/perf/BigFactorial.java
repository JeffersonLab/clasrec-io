package org.jlab.clas.std.services.perf;

import java.math.BigInteger;
import java.util.Set;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.services.util.Clas12Types;
import org.json.JSONException;
import org.json.JSONObject;

public class BigFactorial implements Engine {

    private static final String NAME = "BigFactorial";
    private static final int DEFAULT_N = 6000;

    private volatile int factorial = DEFAULT_N;

    @Override
    public EngineData configure(EngineData input) {
        String mimeType = input.getMimeType();
        if (mimeType.equals(EngineDataType.JSON.mimeType())) {
            JSONObject data = new JSONObject((String) input.getData());
            String key = "factorial";
            if (data.has(key)) {
                try {
                    factorial = data.getInt(key);
                    System.out.println(NAME + " service: factorial set to = " + factorial + "!");
                } catch (JSONException e) {
                    Object value = data.get(key);
                    System.err.println(NAME + " service: wrong configuration value = " + value);
                }
            } else {
                System.err.println(NAME + " service: missing factorial configuration");
            }
        } else {
            System.err.println(NAME + " service: wrong configuration mime-type = " + mimeType);
        }
        return null;
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
        return ClaraUtil.buildDataTypes(Clas12Types.HIPO,
                                        Clas12Types.EVIO,
                                        EngineDataType.JSON);
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
