package org.jlab.clas.std.services.convertors;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineSpecification;
import org.jlab.clara.util.CConstants;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.clas.std.services.util.ServiceUtils;
import org.jlab.clas12.tools.property.JPropertyList;
import org.jlab.coda.jevio.EvioCompactEventWriter;
import org.jlab.coda.jevio.EvioException;

/**
 * Converter service that converts EvIO transient data to EvIO persistent data
 * (i.e. writes EvIO events to an output file).
 * Supports evio-4.1 version.
 */
public class EvioToEvioWriter implements Engine {

    private static final String CONF_ACTION = "action";
    private static final String CONF_FILENAME = "file";
    private static final String CONF_ORDER = "order";
    private static final String CONF_OVERWRITE = "overwrite";

    private static final String CONF_ACTION_OPEN = "open";
    private static final String CONF_ACTION_CLOSE = "close";
    private static final String CONF_ACTION_SKIP = "skip";

    private static final String OUTPUT_NEXT = "next-rec";
    private static final String EVENT_SKIP = "skip";

    private static final String NO_FILE = "No open file";

    // Experimental specification file
    private final EngineSpecification info = new EngineSpecification(this.getClass());
    private final String name = info.name();


    private String fileName = CConstants.UNDEFINED;
    private ByteOrder fileByteOrder = ByteOrder.nativeOrder();

    private boolean skipEvents = false;

    private String openError = NO_FILE;
    private int eventCounter;

    private EvioCompactEventWriter writer;
    private final Object writerLock = new Object();


    @Override
    public EngineData configure(EngineData input) {

        if (input.getMimeType().equalsIgnoreCase(Clas12Types.PROPERTY_LIST.mimeType())) {
            JPropertyList pl = (JPropertyList) input.getData();
            if (pl.containsProperty(CONF_ACTION)) {
                String action = pl.getPropertyValue(CONF_ACTION);
                if (action.equals(CONF_ACTION_OPEN)) {
                    if (pl.containsProperty(CONF_FILENAME)) {
                        openFile(pl);
                    } else {
                        String errMsg = "%s config: Missing '%s' property. PL: %s%n";
                        System.err.printf(errMsg, name, CONF_FILENAME, pl);
                    }
                } else if (action.equals(CONF_ACTION_CLOSE)) {
                    if (pl.containsProperty(CONF_FILENAME)) {
                        closeFile(pl);
                    } else {
                        String errMsg = "%s config: Missing '%s' property. PL: %s%n";
                        System.err.printf(errMsg, name, CONF_FILENAME, pl);
                    }
                } else if (action.equals(CONF_ACTION_SKIP)) {
                    skipAll();
                } else {
                    String errMsg = "%s config: Wrong value of '%s' property = '%s'%n";
                    System.err.printf(errMsg, name, CONF_ACTION, action);
                }
            } else {
                String errMsg = "%s config: Missing '%s' property. PL: %s%n";
                System.err.printf(errMsg, name, CONF_ACTION, pl);
            }
        } else {
            String errMsg = "%s config: Wrong mimetype '%s'%n";
            System.err.printf(errMsg, name, input.getMimeType());
        }

        return null;
    }


    private void openFile(JPropertyList pl) {
        synchronized (writerLock) {
            if (writer != null) {
                writeAndClose();
            }

            fileName = pl.getPropertyValue(CONF_FILENAME);
            if (pl.containsProperty(CONF_ORDER)) {
                String byteOrder = pl.getPropertyValue(CONF_ORDER);
                if (byteOrder.equals(ByteOrder.BIG_ENDIAN.toString())) {
                    fileByteOrder = ByteOrder.BIG_ENDIAN;
                } else {
                    fileByteOrder = ByteOrder.LITTLE_ENDIAN;
                }
            }

            boolean overWriteOK = false;
            if (pl.containsProperty(CONF_OVERWRITE) &&
                    pl.getPropertyValue(CONF_OVERWRITE).equals("true")) {
                overWriteOK = true;
            }

            System.out.printf("%s service: Request to open file %s%n", name, fileName);
            try {
                File file = new File(fileName);
                if (!overWriteOK) {
                    writer = new EvioCompactEventWriter(file.getName(),
                                                        file.getParent(),
                                                        0,
                                                        0,
                                                        20000000,
                                                        fileByteOrder,
                                                        null);
                } else {
                    writer = new EvioCompactEventWriter(file.getName(),
                                                        file.getParent(),
                                                        0,
                                                        0,
                                                        1000000,
                                                        10000,
                                                        20000000,
                                                        fileByteOrder,
                                                        null,
                                                        true);
                }
                eventCounter = 0;
                System.out.printf("%s service: Opened file %s%n", name, fileName);
            } catch (EvioException e) {
                openError = String.format("Error opening the file %s%n%s",
                                          fileName, ClaraUtil.reportException(e));
                System.err.printf("%s service: %s%n", name, openError);
                fileName = null;
                eventCounter = 0;
            }

            skipEvents = false;
        }
    }


    private void closeFile(JPropertyList pl) {
        synchronized (writerLock) {
            fileName = pl.getPropertyValue(CONF_FILENAME);
            System.out.printf("%s service: Request to close file %s%n", name, fileName);
            if (writer != null) {
                writeAndClose();
            } else {
                System.err.printf("%s service: File %s not open%n", name, fileName);
            }
            openError = NO_FILE;
            fileName = null;
            eventCounter = 0;
        }
    }


    private void writeAndClose() {
        if (eventCounter > 0) {
            writer.close();
        }
        System.out.printf("%s service: Closed file %s%n", name, fileName);
        writer = null;
    }


    private void skipAll() {
        System.out.printf("%s service: Request to skip events%n", name);
        synchronized (writerLock) {
            if (writer == null) {
                skipEvents = true;
                System.out.printf("%s service: Skipping all events%n", name);
            } else {
                System.err.printf("%s service: A file %s is already open%n", name, fileName);
            }
        }
    }


    @Override
    public EngineData execute(EngineData input) {
        EngineData output = new EngineData();

        String dt = input.getMimeType();
        if (!dt.equalsIgnoreCase(Clas12Types.EVIO.mimeType())) {
            ServiceUtils.setError(output, String.format("Wrong input type '%s'", dt));
            return output;
        }

        if (skipEvents || input.getDescription().equals(EVENT_SKIP)) {
            output.setData(EngineDataType.STRING.mimeType(), OUTPUT_NEXT);
            output.setDescription("event skipped");
            return output;
        }

        synchronized (writerLock) {
            if (writer == null) {
                ServiceUtils.setError(output, openError);
            } else {
                try {
                    ByteBuffer event = (ByteBuffer) input.getData();
                    writer.writeEvent(event);
                    eventCounter++;
                    output.setData(EngineDataType.STRING.mimeType(), OUTPUT_NEXT);
                    output.setDescription("event saved");

                } catch (IOException | EvioException e) {
                    String msg = String.format("Error saving event to file %s%n%n%s",
                            fileName, ClaraUtil.reportException(e));
                    ServiceUtils.setError(output, msg);
                }
            }
        }

        return output;
    }

    @Override
    public EngineData executeGroup(Set<EngineData> inputs) {
        return null;
    }

    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(Clas12Types.EVIO, Clas12Types.PROPERTY_LIST);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.STRING);
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
        return String.format("%s <%s>", info.author(), info.email());
    }

    @Override
    public void reset() {
        // nothing
    }

    @Override
    public void destroy() {
        if (writer != null) {
            writeAndClose();
        }
    }
}
