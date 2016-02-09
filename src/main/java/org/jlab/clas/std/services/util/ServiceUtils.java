package org.jlab.clas.std.services.util;

import java.nio.ByteBuffer;

import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineStatus;

public final class ServiceUtils {

    private ServiceUtils() { }

    public static ByteBuffer getDataAsByteBuffer(EngineData input) {
        return ByteBuffer.wrap((byte[]) input.getData());
    }


    public static void setError(EngineData out, String msg) {
        out.setDescription(msg);
        out.setStatus(EngineStatus.ERROR, 1);
    }


    public static void setError(EngineData out, String msg, int severityId) {
        out.setDescription(msg);
        out.setStatus(EngineStatus.ERROR, severityId);
    }


    public static void setError(EngineData out, String format, Object... args) {
        ServiceUtils.setError(out, String.format(format, args));
    }
}
