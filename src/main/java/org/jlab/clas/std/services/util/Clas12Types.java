package org.jlab.clas.std.services.util;

import org.jlab.clara.engine.EngineDataType;

// TODO: put this in a common CLAS package
public final class Clas12Types {

    private Clas12Types() { }

    public static final EngineDataType EVIO =
            new EngineDataType("binary/data-evio", EngineDataType.BYTES.serializer());
}
