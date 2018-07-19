package org.jlab.clas.std.services.convertors;

import java.nio.file.Path;

import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.std.services.AbstractEventWriterService;
import org.jlab.clara.std.services.EventWriterException;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.jnp.hipo.data.HipoEvent;
import org.jlab.jnp.hipo.io.HipoWriter;
import org.jlab.jnp.utils.file.FileUtils;
import org.json.JSONObject;

/**
 * Service that converts HIPO transient data to HIPO persistent data
 * (i.e. writes HIPO events to an output file).
 */
public class HipoToHipoWriter extends AbstractEventWriterService<HipoWriter> {

    private static final String CONF_COMPRESSION = "compression";
    private static final String CONF_SCHEMA_DIR = "schema_dir";
    private static final String CONF_SCHEMA_FILTER = "schema_filter";

    @Override
    protected HipoWriter createWriter(Path file, JSONObject opts) throws EventWriterException {
        try {
            HipoWriter writer = new HipoWriter();
            configure(writer, opts);
            writer.open(file.toString());
            return writer;
        } catch (Exception e) {
            throw new EventWriterException(e);
        }
    }

    private void configure(HipoWriter writer, JSONObject opts) {
        if (opts.has(CONF_COMPRESSION)) {
            int compression = opts.getInt(CONF_COMPRESSION);
            System.out.printf("%s service: compression level = %d%n", getName(), compression);
            writer.setCompressionType(compression);
        }

        String schemaDir = FileUtils.getEnvironmentPath("CLAS12DIR", "etc/bankdefs/hipo");
        if (opts.has(CONF_SCHEMA_DIR)) {
            schemaDir = opts.getString(CONF_SCHEMA_DIR);
            System.out.printf("%s service: schema directory = %s%n", getName(), schemaDir);
        }
        writer.getSchemaFactory().initFromDirectory(schemaDir);

        if (opts.has(CONF_SCHEMA_DIR)) {
            boolean useFilter = opts.optBoolean(CONF_SCHEMA_FILTER, true);
            System.out.printf("%s service: schema filter = %b%n", getName(), useFilter);
            writer.setSchemaFilter(useFilter);
        }
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
