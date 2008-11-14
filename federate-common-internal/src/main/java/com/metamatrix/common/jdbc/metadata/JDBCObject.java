/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.jdbc.metadata;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.core.util.ArgCheck;

public abstract class JDBCObject {

    public static final String DELIMITER = "."; //$NON-NLS-1$

    private String name;
    private JDBCObject owner;
    private Properties properties;
    private Properties unmodProperties;
    private boolean marked;
    private boolean hasNullName;

    protected JDBCObject() {
        this.properties = new Properties();
        this.unmodProperties = new UnmodifiableProperties(this.properties);
    }

    protected JDBCObject(String name) {
//        Assertion.isNotNull(name, "The String reference may not be null");
//        Assertion.isNotZeroLength(name, "The String reference may not be zero-length");
        this.setName(name);
        this.properties = new Properties();
        this.unmodProperties = new UnmodifiableProperties(this.properties);
    }

    public String getName() {
        return name;
    }

    /**
     * Set the name of this object.  The general approach is that some
     * objects may have null names.
     * @param name the new name; may be null
     */
    public void setName(String name) {
        this.name = name;
        if ( this.name == null || this.name.length() != 0 ) {
            this.hasNullName = true;
        }
    }
    
    public boolean hasName() {
        return this.name != null && this.name.length() != 0;
    }
    
    /**
     * This method returns whether the object's original name was
     * null.
     */
    public boolean getOriginalNameNull() {
        return hasNullName;
    }
    
    public void setOriginalNameNull( boolean nameWasNull ) {
        this.hasNullName = nameWasNull;    
    }
    
    public boolean isMarked() {
        return this.marked;
    }
    
    public void setMarked( boolean isMarked ) {
        this.marked = isMarked;
        if ( this.hasOwner() ) {
            this.getOwner().setMarked(isMarked);
        }
    }

    public String getFullName() {
        return this.addFullName(new StringBuffer(), null).toString();
    }

    public String getFullName(String delimiter) {
        return this.addFullName(new StringBuffer(), delimiter).toString();
    }

    public JDBCObject getOwner() {
        return owner;
    }

    void setOwner(JDBCObject owner) {
        this.owner = owner;
    }

    public boolean hasOwner() {
        return owner != null;
    }

    public String toString() {
        return this.getFullName();
    }

    protected StringBuffer addFullName(StringBuffer sb, String delimiter) {
        if (delimiter == null || delimiter.length() == 0) {
            delimiter = DELIMITER;
        }
        StringBuffer result = sb;
        if (this.owner != null) {
            result = this.owner.addFullName(result, delimiter);
            if (result.length() != 0) {
                result.append(delimiter);
            }
        }
        result.append(this.name);
        return result;
    }

    public Properties getProperties() {
        return unmodProperties;
    }

    public boolean hasProperties() {
        return this.properties != null && this.properties.size() != 0;
    }

    public String setProperty(String propName, String value) {
        return (String)this.properties.setProperty(propName, value);
    }

    public String removeProperty(String propName) {
        return (String)this.properties.remove(propName);
    }

    public String getProperty(String propName) {
        return this.properties.getProperty(propName);
    }

    public void print(PrintStream stream) {
        print(stream, ""); //$NON-NLS-1$
    }

    public void print(PrintStream stream, String lead) {
        if(stream == null){
            ArgCheck.isNotNull(stream, "The stream reference may not be null"); //$NON-NLS-1$
        }
        stream.println(lead + this.getName() + (this.isMarked()? " <marked>" : "") ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    protected static JDBCObject lookupJDBCObject(List domain, String name, Class type) {
        if (name == null) {
            return null;
        }
        Iterator iter = domain.iterator();
        while (iter.hasNext()) {
            JDBCObject obj = (JDBCObject)iter.next();
            if (type.isInstance(obj) && name.equals(obj.getName())) {
                return obj;
            }
        }
        return null;
    }
}



