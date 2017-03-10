package org.jlab.clas.std.services.convertors;

import java.nio.file.Path;

import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.services.base.AbstractEventWriter;
import org.jlab.clas.std.services.base.EventWriterException;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.hipo.data.HipoEvent;
import org.jlab.hipo.io.HipoWriter;
import org.jlab.hipo.utils.FileUtils;
import org.json.JSONObject;

/**
 * Service that converts HIPO transient data to HIPO persistent data
 * (i.e. writes HIPO events to an output file).
 */
public class HipoToHipoWriter extends AbstractEventWriter<HipoWriter> {

    private static final String CONF_COMPRESSION = "compression";
    private static final String CONF_SCHEMA = "schema_dir";

    @Override
    protected HipoWriter createWriter(Path file, JSONObject opts) throws EventWriterException {
        try {
            HipoWriter writer = new HipoWriter();
            writer.setCompressionType(getCompression(opts));
            writer.getSchemaFactory().initFromDirectory(getSchemaDirectory(opts));
            writer.open(file.toString());
            return writer;
        } catch (Exception e) {
            throw new EventWriterException(e);
        }
    }

    private int getCompression(JSONObject opts) {
        return opts.has(CONF_COMPRESSION) ? opts.getInt(CONF_COMPRESSION) : 0;
    }

    private String getSchemaDirectory(JSONObject opts) {
        return opts.has(CONF_SCHEMA)
                ? opts.getString(CONF_SCHEMA)
                : FileUtils.getEnvironmentPath("CLAS12DIR", "etc/bankdefs/hipo");
    }


    @Override
    protected void closeWriter() {
        writer.close();
    }

    @Override
    protected void writeEvent(Object event) throws EventWriterException {
        try {
            writer.writeEvent((HipoEvent) event);
        } catch (Exception e) {
            throw new EventWriterException(e);
        }
    }

    @Override
    protected EngineDataType getDataType() {
        return Clas12Types.HIPO;
    }
}
