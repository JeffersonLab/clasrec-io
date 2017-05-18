package org.jlab.clas.std.services.perf;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.std.services.ServiceUtils;
import org.jlab.clas.std.services.util.Clas12Types;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Benchmark1 implements Engine {

    private static final String NAME = "Benchmark1";

    private static final String DEFAULT_MODE = "array";
    private static final int DEFAULT_SIZE = 200_000;
    private static final int DEFAULT_ITER = 5;
    private static final int DEFAULT_OPS = 500;

    private volatile String mode = DEFAULT_MODE;
    private volatile int size = DEFAULT_SIZE;
    private volatile int iter = DEFAULT_ITER;
    private volatile int ops = DEFAULT_OPS;


    @Override
    public EngineData configure(EngineData input) {
        String mimeType = input.getMimeType();
        if (mimeType.equals(EngineDataType.JSON.mimeType())) {
            JSONObject data = new JSONObject((String) input.getData());
            try {
                if (data.has("mode")) {
                    String mode = data.getString("mode");
                    System.out.println(NAME + "service config: mode = " + mode);
                    this.mode = mode;
                }
                if (data.has("size")) {
                    int size = data.getInt("size");
                    System.out.println(NAME + "service config: size = " + size);
                    this.size = size;
                }
                if (data.has("iter")) {
                    int iter = data.getInt("iter");
                    System.out.println(NAME + "service config: iter = " + iter);
                    this.iter = iter;
                }
                if (data.has("ops")) {
                    int ops = data.getInt("ops");
                    System.out.println(NAME + "service config: ops = " + ops);
                    this.ops = ops;
                }
            } catch (JSONException e) {
                System.err.println(NAME + " service config: " + e.getMessage());
            }
        } else {
            System.err.println(NAME + " service config: wrong mime-type = " + mimeType);
        }
        return null;
    }


    @Override
    public EngineData execute(EngineData input) {
        String mode = this.mode;

        if (mode.equals("array")) {
            runArray();
        } else if (mode.equals("list")) {
            runList();
        } else {
            EngineData output = new EngineData();
            ServiceUtils.setError(output, "Invalid mode: " + mode);
            return output;
        }
        return input;
    }


    private void runArray() {
        int size = this.size;
        int iter = this.iter;
        int ops = this.ops;

        long[] list1 = new long[size];
        long[] list2 = new long[size];

        for (int j = 0; j < size; j++) {
            list1[j] = j;
            list2[j] = j;
        }

        int block = size / iter;
        for (int i = 0; i < iter; i++) {
            int min = i * block;
            int max = min + block;

            for (int j = 0; j < size; j++) {
                long oldValue = list1[j];
                long newValue = oldValue + list2[j];
                if (j >= min && j < max) {
                    list1[j] = newValue;
                }
            }
            long sum = 0;
            for (int j = 0; j < size; j++) {
                sum += list1[j];
            }
            long avg = sum / size;
            for (int j = 0; j < size; j++) {
                long oldValue = list2[j];
                long newValue = oldValue + avg;
                long update = newValue;
                for (int k = 1; k <= ops; k++) {
                    update += newValue * (k % 10);
                }
                if (j >= min && j < max) {
                    list2[j] = update;
                }
            }
        }
    }


    private void runList() {
        int size = this.size / 2;
        int iter = this.iter * 2;
        int ops = this.ops;

        List<Long> list1 = new ArrayList<>(size);
        List<Long> list2 = new ArrayList<>(size);

        for (int j = 0; j < size; j++) {
            list1.add(Long.valueOf(j));
            list2.add(Long.valueOf(j));
        }

        int block = size / iter;
        for (int i = 0; i < iter; i++) {
            int min = i * block;
            int max = min + block;

            for (int j = 0; j < size; j++) {
                long oldValue = list1.get(j);
                long newValue = oldValue + list2.get(j);
                if (j >= min && j < max) {
                    list1.set(j, newValue);
                }
            }
            long sum = 0;
            for (int j = 0; j < size; j++) {
                sum += list1.get(j);
            }
            long avg = sum / size;
            for (int j = 0; j < size; j++) {
                long oldValue = list2.get(j);
                long newValue = oldValue + avg;
                long update = newValue;
                for (int k = 1; k <= ops; k++) {
                    update += newValue * (k % 10);
                }
                if (j >= min && j < max) {
                    list2.set(j, update);
                }
            }
        }
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
        return ClaraUtil.buildDataTypes(Clas12Types.HIPO,
                                        Clas12Types.EVIO);
    }


    @Override
    public Set<String> getStates() {
        return null;
    }


    @Override
    public String getDescription() {
        return "Benchmark service for memory usage.";
    }


    @Override
    public String getVersion() {
        return "0.1";
    }


    @Override
    public String getAuthor() {
        return "smancill";
    }


    @Override
    public void reset() {
        mode = DEFAULT_MODE;
        size = DEFAULT_SIZE;
        iter = DEFAULT_ITER;
        ops = DEFAULT_OPS;
    }

    @Override
    public void destroy() {
        // nothing
    }


    public static void main(String[] args) {
        Engine engine = new Benchmark1();

        JSONObject params = new JSONObject();
        params.put("mode", "array");
        params.put("size", 200_000);
        params.put("iter", 3);
        params.put("ops", 250);

        EngineData config = new EngineData();
        config.setData(EngineDataType.JSON, params.toString());
        engine.configure(config);

        EngineData data = new EngineData();
        data.setData("test_data");
        long start = System.currentTimeMillis();
        engine.execute(data);
        long end = System.currentTimeMillis();
        System.out.printf("Total time: %d ms%n", (end - start));
    }
}
