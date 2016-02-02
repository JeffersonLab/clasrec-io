package org.jlab.clas.std.services.util;

import java.nio.ByteBuffer;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.ClaraSerializer;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas12.tools.MimeType;
import org.jlab.clas12.tools.property.JPropertyList;

// TODO: put this in a common CLAS package
public final class Clas12Types {

    private Clas12Types() { }

    public static final EngineDataType EVIO =
            new EngineDataType(MimeType.EVIO.type(), EngineDataType.BYTES.serializer());

    public static final EngineDataType PROPERTY_LIST =
            new EngineDataType(MimeType.PROPERTY_LIST.type(),
                new ClaraSerializer() {

                    @Override
                    public ByteBuffer write(Object data) throws ClaraException {
                        JPropertyList pl = (JPropertyList) data;
                        return ByteBuffer.wrap(pl.getStringRepresentation(true).getBytes());
                    }


                    @Override
                    public Object read(ByteBuffer buffer) throws ClaraException {
                        return new JPropertyList(new String(buffer.array()));
                    }
                });
}
