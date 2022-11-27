package org.teiid.runtime;

public class Funcs {

    public static boolean something(long val) {
        return val > System.currentTimeMillis();
    }

}
