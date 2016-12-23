package org.jlab.clas.std.services.base;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineSpecification;
import org.jlab.clas.std.services.util.FileUtils;
import org.jlab.clas.std.services.util.ServiceUtils;
import org.json.JSONObject;

public abstract class AbstractEventWriter<Writer> implements Engine {

    private static final String CONF_ACTION = "action";
    private static final String CONF_FILENAME = "file";

    private static final String CONF_ACTION_OPEN = "open";
    private static final String CONF_ACTION_CLOSE = "close";
    private static final String CONF_ACTION_SKIP = "skip";

    private static final String OUTPUT_NEXT = "next-rec";
    private static final String EVENT_SKIP = "skip";

    private static final String NO_FILE = "No open file";

    // Experimental specification file
    private final EngineSpecification info = new EngineSpecification(this.getClass());
    private final String name = info.name();

    private String fileName = ClaraConstants.UNDEFINED;
    private boolean skipEvents = false;

    private String openError = NO_FILE;
    private int eventCounter;

    protected Writer writer;
    private final Object writerLock = new Object();


    @Override
    public EngineData configure(EngineData input) {
        final long startTime = System.currentTimeMillis();
        if (input.getMimeType().equalsIgnoreCase(EngineDataType.JSON.mimeType())) {
            String source = (String) input.getData();
            JSONObject configData = new JSONObject(source);
            if (configData.has(CONF_ACTION)) {
                String action = configData.getString(CONF_ACTION);
                if (action.equals(CONF_ACTION_OPEN)) {
                    if (configData.has(CONF_FILENAME)) {
                        openFile(configData);
                    } else {
                        String errMsg = "%s config: Missing '%s' parameter: %s%n";
                        System.err.printf(errMsg, name, CONF_FILENAME, source);
                    }
                } else if (action.equals(CONF_ACTION_CLOSE)) {
                    if (configData.has(CONF_FILENAME)) {
                        closeFile(configData);
                    } else {
                        String errMsg = "%s config: Missing '%s' parameter: %s%n";
                        System.err.printf(errMsg, name, CONF_FILENAME, source);
                    }
                } else if (action.equals(CONF_ACTION_SKIP)) {
                    skipAll();
                } else {
                    String errMsg = "%s config: Wrong value of '%s' parameter = '%s'%n";
                    System.err.printf(errMsg, name, CONF_ACTION, action);
                }
            } else {
                String errMsg = "%s config: Missing '%s' parameter: %s%n";
                System.err.printf(errMsg, name, CONF_ACTION, source);
            }
        } else {
            String errMsg = "%s config: Wrong mimetype '%s'%n";
            System.err.printf(errMsg, name, input.getMimeType());
        }
        long configureTime = System.currentTimeMillis() - startTime;
        System.out.printf("%s config time: %d [ms]%n", name, configureTime);
        return null;
    }


    private void openFile(JSONObject configData) {
        synchronized (writerLock) {
            if (writer != null) {
                writeAndClose();
            }
            fileName = configData.getString(CONF_FILENAME);
            System.out.printf("%s service: Request to open file %s%n", name, fileName);
            try {
                File file = new File(fileName);
                File outputDir = file.getParentFile();
                if (outputDir != null) {
                    FileUtils.createDirectories(outputDir.toPath());
                }
                writer = createWriter(Paths.get(fileName), configData);
                eventCounter = 0;
                System.out.printf("%s service: Opened file %s%n", name, fileName);
            } catch (IOException | EventWriterException e) {
                openError = String.format("Error opening the file %s%n%s",
                                          fileName, ClaraUtil.reportException(e));
                System.err.printf("%s service: %s%n", name, openError);
                fileName = null;
                eventCounter = 0;
            }

            skipEvents = false;
        }
    }


    private void closeFile(JSONObject data) {
        synchronized (writerLock) {
            fileName = data.getString(CONF_FILENAME);
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
            closeWriter();
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




    protected abstract Writer createWriter(Path file, JSONObject opts) throws EventWriterException;

    protected abstract void closeWriter();


    @Override
    public EngineData execute(EngineData input) {
        EngineData output = new EngineData();

        String dt = input.getMimeType();
        if (!dt.equalsIgnoreCase(getDataType().mimeType())) {
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
                    writeEvent(input.getData());
                    eventCounter++;
                    output.setData(EngineDataType.STRING.mimeType(), OUTPUT_NEXT);
                    output.setDescription("event saved");

                } catch (EventWriterException e) {
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


    protected abstract void writeEvent(Object event) throws EventWriterException;

    protected abstract EngineDataType getDataType();


    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(getDataType(), EngineDataType.JSON);
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
        return String.format("%s  <%s>", info.author(), info.email());
    }

    @Override
    public void reset() {
        synchronized (writerLock) {
            if (writer != null) {
                writeAndClose();
            }
        }
    }

    @Override
    public void destroy() {
        synchronized (writerLock) {
            if (writer != null) {
                writeAndClose();
            }
        }
    }
}
