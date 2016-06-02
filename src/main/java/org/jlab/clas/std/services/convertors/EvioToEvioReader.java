package org.jlab.clas.std.services.convertors;

import java.io.File;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineSpecification;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.clas.std.services.util.ServiceUtils;
import org.jlab.coda.jevio.EvioCompactReader;
import org.jlab.coda.jevio.EvioException;
import org.json.JSONObject;

/**
 * Converter service that converts EvIO persistent data to EvIO transient data
 * (i.e. Reads EvIO events from an input file)
 */
public class EvioToEvioReader implements Engine {

    private static final String CONF_ACTION = "action";
    private static final String CONF_FILENAME = "file";

    private static final String CONF_ACTION_OPEN = "open";
    private static final String CONF_ACTION_CLOSE = "close";
    private static final String CONF_ACTION_CACHE = "cache";

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

    private EvioCompactReader evioReader;
    private final Object readerLock = new Object();

    private boolean useCached = false;
    private ByteBuffer cachedEvent;

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
                String inputFile = data.getString(CONF_FILENAME);
                if (action.equals(CONF_ACTION_OPEN)) {
                    openFile(inputFile);
                } else if (action.equals(CONF_ACTION_CLOSE)) {
                    closeFile(inputFile);
                } else if (action.equals(CONF_ACTION_CACHE)) {
                    readCachedEvent(inputFile);
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


    private void openFile(String inputFile) {
        synchronized (readerLock) {
            if (evioReader != null) {
                closeFile();
            }
            fileName = inputFile;
            System.out.printf("%s service: Request to open file %s%n", name, fileName);
            try {
                evioReader = new EvioCompactReader(new File(fileName));
                eventCount = evioReader.getEventCount();
                currentEvent = 1;
                processingEvents.clear();
                eofRequestCount = 0;
                System.out.printf("%s service: Opened file %s%n", name, fileName);
            } catch (IOException | EvioException e) {
                openError = String.format("Error opening the file %s%n%s",
                        fileName, ClaraUtil.reportException(e));
                System.err.printf("%s service: %s%n", name, openError);
                fileName = null;
            }
            if (useCached) {
                useCached = false;
                cachedEvent = null;
            }
        }
    }


    private void closeFile(String inputFile) {
        synchronized (readerLock) {
            fileName = inputFile;
            System.out.printf("%s service: Request to close file %s%n", name, fileName);
            if (evioReader != null) {
                closeFile();
            } else {
                System.err.printf("%s service: File %s not open%n", name, fileName);
            }
            openError = NO_FILE;
            fileName = null;
            useCached = false;
            cachedEvent = null;
        }
    }


    private void closeFile() {
        evioReader.close();
        evioReader = null;
        System.out.printf("%s service: Closed file %s%n", name, fileName);
    }


    private void readCachedEvent(String inputFile) {
        openFile(inputFile);
        synchronized (readerLock) {
            useCached = true;
            if (evioReader != null) {
                try {
                    System.out.printf("%s service: Caching a single event to avoid IO%n", name);
                    cachedEvent = evioReader.getEventBuffer(currentEvent, true);
                    System.out.printf("%s service: Event size: %d%n", name, cachedEvent.limit());
                    closeFile();
                } catch (EvioException e) {
                    String msg = String.format("Error requesting event from file %s%n%n%s",
                            fileName, ClaraUtil.reportException(e));
                    System.err.printf("%s service: %s%n", name, msg);
                }
            }
        }
    }


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
            if (evioReader == null) {
                ServiceUtils.setError(output, openError, 1);
            } else if (currentEvent > eventCount) {
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
            } else {
                if (useCached) {
                    returnCachedEvent(output);
                } else {
                    returnNextEvent(output);
                }
            }
        }
    }


    private void returnNextEvent(EngineData output) {
        try {
            ByteBuffer event = evioReader.getEventBuffer(currentEvent, true);
            output.setData(Clas12Types.EVIO.mimeType(), event);
            output.setDescription("data");
            output.setCommunicationId(currentEvent);
            processingEvents.add(currentEvent);
            currentEvent++;
        } catch (EvioException e) {
            String msg = String.format("Error requesting event from file %s%n%n%s", fileName,
                    ClaraUtil.reportException(e));
            ServiceUtils.setError(output, msg, 1);
        }
    }


    private void returnCachedEvent(EngineData output) {
        if (cachedEvent == null) {
            String msg = String.format("Error getting cached event of file %s%n", fileName);
            ServiceUtils.setError(output, msg, 1);
        } else {
            output.setData(Clas12Types.EVIO.mimeType(), cachedEvent);
            output.setDescription("skip");
            output.setCommunicationId(currentEvent);
            processingEvents.add(currentEvent);
            currentEvent++;
        }
    }


    private void getFileByteOrder(EngineData output) {
        synchronized (readerLock) {
            if (evioReader == null) {
                ServiceUtils.setError(output, openError, 1);
            } else {
                output.setData(EngineDataType.STRING.mimeType(),
                               evioReader.getFileByteOrder().toString());
                output.setDescription("byte order");
            }
        }
    }


    private void getEventCount(EngineData output) {
        synchronized (readerLock) {
            if (evioReader == null) {
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

    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(
                EngineDataType.JSON,
                EngineDataType.STRING);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(
                Clas12Types.EVIO,
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
            if (evioReader != null) {
                closeFile();
            }
        }
    }


    @Override
    public void destroy() {
        synchronized (readerLock) {
            if (evioReader != null) {
                closeFile();
            }
        }
    }
}
