package org.jlab.clas.std.services.convertors;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.std.services.AbstractEventReaderService;
import org.jlab.clara.std.services.EventReaderException;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.coda.et.EtAttachment;
import org.jlab.coda.et.EtConstants;
import org.jlab.coda.et.EtEvent;
import org.jlab.coda.et.EtStation;
import org.jlab.coda.et.EtStationConfig;
import org.jlab.coda.et.EtSystem;
import org.jlab.coda.et.EtSystemOpenConfig;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.jevio.EvioCompactReader;
import org.jlab.detector.decode.CLASDecoder;
import org.jlab.io.evio.EvioDataEvent;
import org.jlab.io.evio.EvioFactory;
import org.jlab.io.hipo.HipoDataEvent;
import org.jlab.jnp.hipo.data.HipoEvent;
import org.json.JSONObject;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Service that converts ET ring EVIO events to HIPO transient events.
 */
public class EtRingToHipoReader extends AbstractEventReaderService<EtRingToHipoReader.EtReader> {

    private static final String CONF_SYSTEM = "system";
    private static final String CONF_HOST = "host";
    private static final String CONF_PORT = "port";

    @Override
    protected EtRingToHipoReader.EtReader createReader(Path file, JSONObject opts)
            throws EventReaderException {
        try {
            return new EtReader(getEtSystem(opts), getEtHost(opts), getEtPort(opts));
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
        return 1000;
    }

    @Override
    public ByteOrder readByteOrder() throws EventReaderException {
        return ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public Object readEvent(int eventNumber) throws EventReaderException {
        try {
            HipoEvent event = reader.getEvent();
            if (event == null) {
                throw new EventReaderException("Could not obtain an event from ET ring");
            }
            return event;
        } catch (Exception e) {
            throw new EventReaderException(e);
        }
    }

    @Override
    protected EngineDataType getDataType() {
        return Clas12Types.HIPO;
    }


    public static class EtReader implements Closeable {

        private static final String DEFAULT_SYS_NAME = "clara_system";
        private static final String DEFAULT_STAT_NAME = "clara_station";

        private static final Integer MAX_NEVENTS = 10;
        private static final Integer TIMEOUT = 20000;

        private final EtSystem sys;
        private final EtAttachment att;

        private final CLASDecoder decoder = new CLASDecoder();

        private final Queue<HipoEvent> evQueue = new LinkedList<>();

        EtReader(String system, String host, int port) throws Exception {
            EtSystemOpenConfig config = new EtSystemOpenConfig(system, host, port);

            sys = new EtSystem(config, EtConstants.debugInfo);
            sys.open();

            EtStationConfig statConfig = new EtStationConfig();

            EtStation stat = sys.createStation(statConfig, DEFAULT_STAT_NAME);
            att = sys.attach(stat);
        }

        HipoEvent getEvent() throws Exception {
            if (evQueue.isEmpty()) {
                getEtEvents();
            }
            return evQueue.poll();
        }

        private void getEtEvents() throws Exception {
            EtEvent[] etEvents = sys.getEvents(att, Mode.TIMED, null, TIMEOUT, MAX_NEVENTS);
            try {
                for (EtEvent etEvent : etEvents) {
                    ByteBuffer etBuffer = etEvent.getDataBuffer();
                    ByteBuffer evioBuffer = ByteBuffer.allocate(etEvent.getLength());
                    evioBuffer.put(etEvent.getData());
                    evioBuffer.order(etBuffer.order());

                    EvioCompactReader evioReader = new EvioCompactReader(evioBuffer);
                    ByteBuffer evBuffer = evioReader.getEventBuffer(1);
                    EvioDataEvent evio = new EvioDataEvent(evBuffer, EvioFactory.getDictionary());
                    HipoDataEvent hipo = (HipoDataEvent) decoder.getDataEvent(evio);
                    evQueue.add(hipo.getHipoEvent());
                }
            } finally {
                sys.dumpEvents(att, etEvents);
            }
        }

        @Override
        public void close() {
            sys.close();
        }
    }


    private static String getEtSystem(JSONObject opts) {
        return opts.has(CONF_SYSTEM) ? opts.getString(CONF_SYSTEM) : EtReader.DEFAULT_SYS_NAME;
    }


    private static String getEtHost(JSONObject opts) {
        return opts.has(CONF_HOST) ? opts.getString(CONF_HOST) : ClaraUtil.localhost();
    }


    private static int getEtPort(JSONObject opts) {
        return opts.has(CONF_PORT) ? opts.getInt(CONF_PORT) : EtConstants.serverPort;
    }
}
