package org.jlab.clas.std.services.convertors;

import java.nio.ByteOrder;
import java.nio.file.Path;

import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.std.services.AbstractEventReaderService;
import org.jlab.clara.std.services.EventReaderException;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.hipo.io.HipoReader;
import org.json.JSONObject;

/**
 * Service that converts HIPO persistent data to HIPO transient data
 * (i.e. reads HIPO events from an input file)
 */
public class HipoToHipoReader extends AbstractEventReaderService<HipoReader> {

    @Override
    protected HipoReader createReader(Path file, JSONObject opts)
            throws EventReaderException {
        try {
            HipoReader reader = new HipoReader();
            reader.open(file.toString());
            return reader;
        } catch (Exception e) {
            throw new EventReaderException(e);
        }
    }

    @Override
    protected void closeReader() {
        reader.close();
    }

    @Override
    public int readEventCount() throws EventReaderException {
        return reader.getEventCount();
    }

    @Override
    public ByteOrder readByteOrder() throws EventReaderException {
        return ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public Object readEvent(int eventNumber) throws EventReaderException {
        try {
            return reader.readHipoEvent(eventNumber);
        } catch (Exception e) {
            throw new EventReaderException(e);
        }
    }

    @Override
    protected EngineDataType getDataType() {
        return Clas12Types.HIPO;
    }
}
