package org.jlab.clas.std.services.convertors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.std.services.AbstractEventWriterService;
import org.jlab.clara.std.services.EventWriterException;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.coda.jevio.EventWriter;
import org.jlab.coda.jevio.EvioException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Converter service that converts EvIO transient data to EvIO persistent data
 * (i.e. writes EvIO events to an output file).
 * Supports evio-4.1 version.
 */
public class EvioToEvioWriter extends AbstractEventWriterService<EventWriter> {

    private static final String CONF_ORDER = "order";
    private static final String CONF_OVERWRITE = "overwrite";

    @Override
    protected EventWriter createWriter(Path file, JSONObject opts) throws EventWriterException {
        try {
            String baseName = file.toFile().getName();
            String directory = file.toFile().getParent();
            String runType = null;

            int runNumber = 0;
            int split = 0;

            int blockSizeMax = 1_000_000;
            int blockCountMax = 10_000;
            int bufferSize = 20_000_000;

            ByteOrder byteOrder = getByteOrder(opts);
            String xmlDictionary = null;

            boolean overwriteOK = getOverwriteOption(opts);
            boolean append = false;

            return new EventWriter(baseName, directory, runType,
                                   runNumber, split,
                                   blockSizeMax, blockCountMax, bufferSize,
                                   byteOrder, xmlDictionary, null,
                                   overwriteOK, append);
        } catch (JSONException | EvioException e) {
            throw new EventWriterException(e);
        }
    }

    private ByteOrder getByteOrder(JSONObject opts) {
        if (opts.has(CONF_ORDER)) {
            String byteOrder = opts.getString(CONF_ORDER);
            if (byteOrder.equals(ByteOrder.BIG_ENDIAN.toString())) {
                return ByteOrder.BIG_ENDIAN;
            }
            return ByteOrder.LITTLE_ENDIAN;
        }
        return ByteOrder.BIG_ENDIAN;
    }

    private boolean getOverwriteOption(JSONObject opts) {
        return opts.has(CONF_OVERWRITE) ? opts.getBoolean(CONF_OVERWRITE) : false;
    }

    @Override
    protected void closeWriter() {
        writer.close();
    }

    @Override
    protected void writeEvent(Object event) throws EventWriterException {
        try {
            writer.writeEvent((ByteBuffer) event);
        } catch (EvioException | IOException e) {
            throw new EventWriterException(e);
        }
    }

    @Override
    protected EngineDataType getDataType() {
        return Clas12Types.EVIO;
    }
}
