package org.jlab.clas.std.services.base;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineSpecification;
import org.jlab.clas.std.services.util.ServiceUtils;
import org.json.JSONObject;

public abstract class AbstractEventReader<Reader> implements Engine {

    private static final String CONF_ACTION = "action";
    private static final String CONF_FILENAME = "file";

    private static final String CONF_ACTION_OPEN = "open";
    private static final String CONF_ACTION_CLOSE = "close";

    private static final String REQUEST_NEXT = "next";
    private static final String REQUEST_NEXT_REC = "next-rec";
    private static final String REQUEST_ORDER = "order";
    private static final String REQUEST_COUNT = "count";

    private static final String NO_FILE = "No open file";
    private static final String END_OF_FILE = "End of file";

    private static final int EOF_NOT_FROM_WRITER = 0;
    private static final int EOF_WAITING_REC = -1;

    // Experimental specification file
    private final EngineSpecification info = new EngineSpecification(this.getClass());
    private final String name = info.name();


    private String fileName = ClaraConstants.UNDEFINED;
    private String openError = NO_FILE;

    protected Reader reader;
    private final Object readerLock = new Object();

    private int currentEvent = 1;
    private int eventCount;

    private Set<Integer> processingEvents = new HashSet<Integer>();
    private int eofRequestCount;


    @Override
    public EngineData configure(EngineData input) {
        final long startTime = System.currentTimeMillis();
        if (input.getMimeType().equalsIgnoreCase(EngineDataType.JSON.mimeType())) {
            String source = (String) input.getData();
            JSONObject data = new JSONObject(source);
            if (data.has(CONF_ACTION) && data.has(CONF_FILENAME)) {
                String action = data.getString(CONF_ACTION);
                if (action.equals(CONF_ACTION_OPEN)) {
                    openFile(data);
                } else if (action.equals(CONF_ACTION_CLOSE)) {
                    closeFile(data);
                } else {
                    String errMsg = "%s config: Wrong value of '%s' parameter = '%s'%n";
                    System.err.printf(errMsg, name, CONF_ACTION, action);
                }
            } else {
                String errMsg = "%s config: Missing '%s' or '%s' parameters: %s%n";
                System.err.printf(errMsg, name, CONF_ACTION, CONF_FILENAME, source);
            }
        } else {
            String errMsg = "%s config: Wrong config type '%s'%n";
            System.err.printf(errMsg, name, input.getMimeType());
        }
        long configureTime = System.currentTimeMillis() - startTime;
        System.out.printf("%s config time: %d [ms]%n", name, configureTime);
        return null;
    }


    private void openFile(JSONObject configData) {
        synchronized (readerLock) {
            if (reader != null) {
                closeFile();
            }
            fileName = configData.getString(CONF_FILENAME);
            System.out.printf("%s service: Request to open file %s%n", name, fileName);
            try {
                reader = createReader(Paths.get(fileName), configData);
                eventCount = readEventCount();
                currentEvent = 0;
                processingEvents.clear();
                eofRequestCount = 0;
                System.out.printf("%s service: Opened file %s%n", name, fileName);
            } catch (EventReaderException e) {
                openError = String.format("Error opening the file %s%n%s",
                        fileName, ClaraUtil.reportException(e));
                System.err.printf("%s service: %s%n", name, openError);
                fileName = null;
            }
        }
    }


    private void closeFile(JSONObject configData) {
        synchronized (readerLock) {
            fileName = configData.getString(CONF_FILENAME);
            System.out.printf("%s service: Request to close file %s%n", name, fileName);
            if (reader != null) {
                closeFile();
            } else {
                System.err.printf("%s service: File %s not open%n", name, fileName);
            }
            openError = NO_FILE;
            fileName = null;
        }
    }


    private void closeFile() {
        closeReader();
        reader = null;
        System.out.printf("%s service: Closed file %s%n", name, fileName);
    }


    protected abstract Reader createReader(Path file, JSONObject opts) throws EventReaderException;

    protected abstract void closeReader();


    @Override
    public EngineData execute(EngineData input) {
        EngineData output = new EngineData();

        String dt = input.getMimeType();
        if (dt.equalsIgnoreCase(EngineDataType.STRING.mimeType())) {
            String request = (String) input.getData();
            if (request.equals(REQUEST_NEXT) || request.equals(REQUEST_NEXT_REC)) {
                getNextEvent(input, output);
            } else if (request.equals(REQUEST_ORDER)) {
                System.out.printf("%s execute request: %s%n", name, REQUEST_ORDER);
                getFileByteOrder(output);
            } else if (request.equals(REQUEST_COUNT)) {
                System.out.printf("%s execute request: %s%n", name, REQUEST_COUNT);
                getEventCount(output);
            } else {
                ServiceUtils.setError(output, String.format("Wrong input data = '%s'", request));
            }
        } else {
            String errorMsg = String.format("Wrong input type '%s'", dt);
            ServiceUtils.setError(output, errorMsg);
        }

        return output;
    }


    private boolean isReconstructionRequest(EngineData input) {
        String requestType = (String) input.getData();
        return requestType.equalsIgnoreCase(REQUEST_NEXT_REC);
    }


    private void getNextEvent(EngineData input, EngineData output) {
        synchronized (readerLock) {
            boolean fromRec = isReconstructionRequest(input);
            if (fromRec) {
                processingEvents.remove(input.getCommunicationId());
            }
            if (reader == null) {
                ServiceUtils.setError(output, openError, 1);
            } else if (currentEvent < eventCount) {
                returnNextEvent(output);
            } else {
                ServiceUtils.setError(output, END_OF_FILE, 1);
                if (fromRec) {
                    if (processingEvents.isEmpty()) {
                        eofRequestCount++;
                        ServiceUtils.setError(output, END_OF_FILE, eofRequestCount + 1);
                        output.setData(EngineDataType.SFIXED32.mimeType(), eofRequestCount);
                    } else {
                        output.setData(EngineDataType.SFIXED32.mimeType(), EOF_WAITING_REC);
                    }
                } else {
                    output.setData(EngineDataType.SFIXED32.mimeType(), EOF_NOT_FROM_WRITER);
                }
            }
        }
    }


    private void returnNextEvent(EngineData output) {
        try {
            Object event = readEvent(currentEvent);
            output.setData(getDataType().toString(), event);
            output.setDescription("data");
            output.setCommunicationId(currentEvent);
            processingEvents.add(currentEvent);
            currentEvent++;
        } catch (EventReaderException e) {
            String msg = String.format("Error requesting event %d from file %s%n%n%s",
                    currentEvent, fileName, ClaraUtil.reportException(e));
            ServiceUtils.setError(output, msg, 1);
        }
    }


    private void getFileByteOrder(EngineData output) {
        synchronized (readerLock) {
            if (reader == null) {
                ServiceUtils.setError(output, openError, 1);
            } else {
                try {
                    output.setData(EngineDataType.STRING.mimeType(), readByteOrder().toString());
                    output.setDescription("byte order");
                } catch (EventReaderException e) {
                    String msg = String.format("Error requesting byte-order from file %s%n%n%s",
                            fileName, ClaraUtil.reportException(e));
                    ServiceUtils.setError(output, msg, 1);
                }
            }
        }
    }


    private void getEventCount(EngineData output) {
        synchronized (readerLock) {
            if (reader == null) {
                ServiceUtils.setError(output, openError, 1);
            } else {
                output.setData(EngineDataType.SFIXED32.mimeType(), eventCount);
                output.setDescription("event count");
            }
        }
    }


    @Override
    public EngineData executeGroup(Set<EngineData> inputs) {
        return null;
    }


    protected abstract int readEventCount() throws EventReaderException;

    protected abstract ByteOrder readByteOrder() throws EventReaderException;

    protected abstract Object readEvent(int eventNumber) throws EventReaderException;

    protected abstract EngineDataType getDataType();


    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(
                EngineDataType.JSON,
                EngineDataType.STRING);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(
                getDataType(),
                EngineDataType.STRING,
                EngineDataType.SFIXED32);
    }


    @Override
    public Set<String> getStates() {
        return null;
    }

    @Override
    public String getDescription() {
        return info.description();
    }

    @Override
    public String getVersion() {
        return info.version();
    }

    @Override
    public String getAuthor() {
        return String.format("%s  <%s>", info.author(), info.email());
    }

    @Override
    public void reset() {
        synchronized (readerLock) {
            if (reader != null) {
                closeFile();
            }
        }
    }


    @Override
    public void destroy() {
        synchronized (readerLock) {
            if (reader != null) {
                closeFile();
            }
        }
    }
}
