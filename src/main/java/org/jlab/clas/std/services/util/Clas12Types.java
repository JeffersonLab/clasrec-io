package org.jlab.clas.std.services.util;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.ClaraSerializer;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.jnp.hipo.data.HipoEvent;

import java.nio.ByteBuffer;

// TODO: put this in a common CLAS package
// TODO: should bytes be copied?
public final class Clas12Types {

    private Clas12Types() { }

    private static class HipoSerializer implements ClaraSerializer {

        @Override
        public ByteBuffer write(Object data) throws ClaraException {
            HipoEvent event = (HipoEvent) data;
            return ByteBuffer.wrap(event.getDataBuffer());
        }

        @Override
        public Object read(ByteBuffer buffer) throws ClaraException {
            return new HipoEvent(buffer.array());
        }
    }

    public static final EngineDataType EVIO =
            new EngineDataType("binary/data-evio", EngineDataType.BYTES.serializer());

    public static final EngineDataType HIPO =
            new EngineDataType("binary/data-hipo", new HipoSerializer());
}
