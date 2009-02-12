/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.common.properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.ErrorMessageKeys;


/**
 * This class represents an unmodifiable Properties object, and is used within the
 * MetaMatrix Properties framework whenever an unmodifiable Properties instance is returned.
 * This class extends Properties and overrides the modifying methods to throw
 * an UnsupportedOperationException.
 */
public class UnmodifiableProperties extends Properties  {

	private transient Set keySet = null;
	private transient Set entrySet = null;
	private transient Collection values = null;
    private Properties origProps;

    public UnmodifiableProperties( Properties p ) {
        super();
        if ( p == null ) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0010));
        }
        origProps = p;
    }

    public synchronized void load(InputStream par1) throws IOException  {
        throw new UnsupportedOperationException(CorePlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0011));
    }

    // Don't override the following method.  It is deprecated, and it doesn't alter the existing instance data,
    // so not overriding it will not be a problem
    //public synchronized void save(OutputStream par1, String par2) {
    //}
    public synchronized Object setProperty(String par1, String par2) {
        throw new UnsupportedOperationException(CorePlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0011));
    }
    // Don't override the following method.  It is deprecated, and it doesn't alter the existing instance data,
    // so not overriding it will not be a problem
    //public synchronized void store(OutputStream par1, String par2) throws IOException {
    //    throw new UnsupportedOperationException(CommonPlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0011));
    //}
    public String getProperty(String key) {
        return origProps.getProperty(key);
    }
    public String getProperty(String key, String defaultValue) {
        return origProps.getProperty(key,defaultValue);
    }
    public void list(PrintStream stream) {
        origProps.list(stream);
    }
    public void list(PrintWriter stream) {
        origProps.list(stream);
    }
    public Enumeration propertyNames() {
        return origProps.propertyNames();
    }
    public void clear() {
        throw new UnsupportedOperationException(CorePlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0011));
    }
    public boolean containsKey(Object par1) {
        return origProps.containsKey(par1);
    }
    public boolean containsValue(Object par1) {
        return origProps.containsValue(par1);
    }
    public Set entrySet() {
        if ( entrySet == null ) {
            entrySet = Collections.unmodifiableSet(origProps.entrySet());
        }
        return entrySet;
    }
    public boolean equals(Object par1) {
        return origProps.equals(par1);
    }
    public Object get(Object par1) {
        return origProps.get(par1);
    }
    public Enumeration keys() {
        return origProps.keys();
    }
    public int hashCode() {
        return origProps.hashCode();
    }
    public boolean isEmpty() {
        return origProps.isEmpty();
    }
    public Set keySet() {
        if ( keySet == null ) {
            keySet = Collections.unmodifiableSet(origProps.keySet());
        }
        return keySet;
    }
    public Object put(Object par1, Object par2) {
        throw new UnsupportedOperationException(CorePlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0011));
    }
    public void putAll(Map par1) {
        throw new UnsupportedOperationException(CorePlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0011));
    }
    public Object remove(Object par1) {
        throw new UnsupportedOperationException(CorePlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0011));
    }
    public int size() {
        return origProps.size();
    }
    public Collection values() {
        if ( values == null ) {
            values = Collections.unmodifiableCollection(origProps.values());
        }
        return values;
    }

    public String toString() {
        return origProps.toString();
    }
}

