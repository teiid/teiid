/*
 * Copyright (c) 2000-2005 MetaMatrix, Inc.  All rights reserved.
 */
package org.teiid.runtime.adminapi;

/**
 */
public class MyFunctions {

    public static Object getPropertyNoArgs() {
        return "xyz"; //$NON-NLS-1$
    }

    public static Object getProperty(Object propertyName) {
        return System.getProperty((String)propertyName);
    }

}
