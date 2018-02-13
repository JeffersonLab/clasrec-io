package org.jlab.clas.std.services.convertors;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.std.services.AbstractEventReaderService;
import org.jlab.clara.std.services.EventReaderException;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.coda.et.EtAttachment;
import org.jlab.coda.et.EtConstants;
import org.jlab.coda.et.EtSystem;
import org.jlab.coda.et.EtSystemOpenConfig;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.exception.EtTimeoutException;
import org.jlab.coda.et.EtStationConfig;
import org.jlab.coda.et.EtStation;
import org.jlab.coda.et.EtEvent;
import org.jlab.coda.jevio.EvioCompactReader;
import org.jlab.detector.decode.CLASDecoder;
import org.jlab.io.base.DataBank;
import org.jlab.io.base.DataEvent;
import org.jlab.io.evio.EvioDataDictionary;
import org.jlab.io.evio.EvioDataEvent;
import org.jlab.io.hipo.HipoDataEvent;
import org.jlab.jnp.hipo.data.HipoEvent;
import org.json.JSONObject;
//import org.rcdb.JDBCProvider;
//import org.rcdb.RCDB;

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
    private static final String CONF_TORUS = "torus";
    private static final String CONF_SOLENOID = "solenoid";
    private static final String CONF_RUN = "run";
    private static final String CONF_NEVENT = "nevents";
//    private PrintStream original = System.out;
    private int n_events;

    @Override
    protected EtRingToHipoReader.EtReader
    createReader(Path file, JSONObject opts) throws EventReaderException {
        n_events = getNumberOfEvents(opts);

//        if(getDebug(opts) == 0){
//            System.setOut(new PrintStream(new OutputStream() {
//                public void write(int b) {
//                    //DO NOTHING
//                }
//            }));
//        } else {
//            System.setOut(original);
//        }
        try {
            return new EtReader(getEtSystem(opts),
                getEtHost(opts),
                getEtPort(opts),
                getRunNumber(opts),
                getTorusField(opts),
                getSolenoidField(opts));
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
        return n_events;
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
        private static final Integer TIMEOUT = 2000000;
        private static final Integer QSIZE = 10;
        private int runNumber;
        private float torusField;
        private float solenoidField;

        private final EtSystem sys;
        private final EtAttachment att;

        private final CLASDecoder decoder = new CLASDecoder();
        private EvioDataDictionary dict = null;
        private final Queue<HipoEvent> evQueue = new LinkedList<>();

        EtReader(String system, String host, int port, int runNumber,
                 float torusField, float solenoidField) throws Exception {
            this.runNumber = runNumber;
            this.torusField = torusField;
            this.solenoidField = solenoidField;
            EtSystemOpenConfig config = new EtSystemOpenConfig(system, host, port);
            config.setConnectRemotely(true);
            sys = new EtSystem(config, EtConstants.debugInfo);
            sys.open();

            EtStationConfig statConfig = new EtStationConfig();
            statConfig.setFlowMode(EtConstants.stationSerial);
            statConfig.setBlockMode(EtConstants.stationNonBlocking);
            statConfig.setCue(QSIZE);
            dict = new EvioDataDictionary();
            dict.initWithEnv("CLAS12DIR", "/etc/bankdefs/clas12");
            EtStation stat = sys.createStation(statConfig, DEFAULT_STAT_NAME);
            att = sys.attach(stat);

            System.out.println("INFO: Start processing with runNumber = " + runNumber
                + ", torus = " + this.torusField
                + ", solenoid = " + this.solenoidField);
        }

        HipoEvent getEvent() throws Exception {
            if (evQueue.isEmpty()) {
                getEtEvents();
            }
            return evQueue.poll();
        }

        private void getEtEvents() throws Exception {

            EtEvent[] mevs;
            try {


                // get events from ET system
                try {
                    mevs = sys.getEvents(att, Mode.SLEEP, null, TIMEOUT, MAX_NEVENTS);
//                    mevs = sys.getEvents(att, Mode.TIMED, null, TIMEOUT, MAX_NEVENTS);
                } catch (EtTimeoutException e) {
                    System.out.println("Timed out, try again");
                    return;
                }

                // ----------- ET
                for (EtEvent mev : mevs) {
                    // Get event's data buffer
                    // buf.limit() = length of the actual data (not buffer capacity)
                    ByteBuffer buf = mev.getDataBuffer();
                    buf.limit(mev.getLength()).position(0);

//                    Utilities.printBuffer(buf,0, 30, "et-data");
//                    System.out.println("    data len = " + mev.getLength());

                    try {
                        // If using byte array you need to watch out for endianness
                        ByteBuffer evioBuffer = ByteBuffer.allocate(mev.getLength());
                        try {
                            byte[] data = mev.getData();
                            evioBuffer.put(data, 0, mev.getLength());
                        } catch (UnsupportedOperationException e) {
                            evioBuffer.put(buf);
                        }

                        evioBuffer.order(buf.order());

                        // ------------- EVIO
//                        System.out.println("------> parsing event # " + mev +
//                            " width length = " + mev.getLength());
//                        try {
                        EvioCompactReader reader = new EvioCompactReader(buf);
                        ByteBuffer a = reader.getEventBuffer(1);
                        EvioDataEvent eventEv = new EvioDataEvent(a, dict);

                        // ----------- HIPO
                        DataEvent decodedEvent = decoder.getDataEvent(eventEv);

/*
                        // get run number from the EtEvent
                        HipoDataBank tmpBank = decoder.createHeaderBank(
                            decodedEvent, -1, 10, torusField, solenoidField);
                        int tmpRn = tmpBank.getInt("run", 0);
                        // check if run number is changed
                        if (tmpRn != runNumber) {
                            runNumber = tmpRn;
                            // go to RCDB and get torus and solenoid scales
                            JDBCProvider
                                provider = RCDB.createProvider("mysql://rcdb@clasdb.jlab.org/rcdb");
                            provider.connect();

                            solenoidField =
                                (float) provider.getCondition(runNumber, "solenoid_scale").toDouble();
                            torusField =
                                (float) provider.getCondition(runNumber, "torus_scale").toDouble();
                            provider.close();
                            System.out.println("INFO: Processing conditions change. ======> runNumber = "
                                + runNumber
                                + ", torus = " + torusField
                                + ", solenoid = " + solenoidField);
                        }
*/

                        DataBank header = decoder.createHeaderBank(
                            decodedEvent, runNumber, 10, torusField, solenoidField);

                        decodedEvent.appendBanks(header);

                        HipoDataEvent hipo = (HipoDataEvent) decodedEvent;
                        evQueue.add(hipo.getHipoEvent());

                    } catch (UnsupportedOperationException e) {
                        System.out.println(e.getMessage());
                    }
                }

                // put events back into ET system
                sys.putEvents(att, mevs);
            } catch (Exception ex) {
                System.out.println("Error using ET system as consumer");
                ex.printStackTrace();
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

    private static int getRunNumber(JSONObject opts) {
        return opts.has(CONF_RUN) ? opts.getInt(CONF_RUN) : 999;
    }

    private static float getTorusField(JSONObject opts) {
        return opts.has(CONF_TORUS) ? (float) opts.getDouble(CONF_TORUS) : (float) -1.0;
    }

    private static float getSolenoidField(JSONObject opts) {
        return opts.has(CONF_SOLENOID) ? (float) opts.getDouble(CONF_SOLENOID) : (float) -1.0;
    }

    private static int getNumberOfEvents(JSONObject opts) {
        return opts.has(CONF_NEVENT) ? opts.getInt(CONF_NEVENT) : 1000;
    }


}
