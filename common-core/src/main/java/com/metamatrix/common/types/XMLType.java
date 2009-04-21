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

package com.metamatrix.common.types;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
//## JDBC4.0-begin ##
import java.sql.SQLXML;
//## JDBC4.0-end ##

/*## JDBC3.0-JDK1.5-begin ##
import com.metamatrix.core.jdbc.SQLXML; 
## JDBC3.0-JDK1.5-end ##*/

import javax.xml.transform.Result;
import javax.xml.transform.Source;

import com.metamatrix.core.CorePlugin;

/**
 * This class represents the SQLXML object along with the Streamable interface. This is
 * class used everywhere in the MetaMatrix framework, but clients are restricted to use
 * only SQLXML interface on top of this.
 */
public final class XMLType implements Streamable, SQLXML {

    private transient SQLXML srcXML;
    private String referenceStreamId;
    private String persistenceStreamId;
    
    public XMLType(){
        
    }
    
    public SQLXML getSourceSQLXML() {
    	return srcXML;
    }
         
    public XMLType(SQLXML xml) {      
        if (xml == null) {
            throw new IllegalArgumentException(CorePlugin.Util.getString("XMLValue.isNUll")); //$NON-NLS-1$
        }
        
        // this will serve as the in VM reference
        this.srcXML = xml;
    }    
            
    public String getReferenceStreamId() {
        return this.referenceStreamId;
    }
    
    public void setReferenceStreamId(String id) {
        this.referenceStreamId = id;
    }
    
    public String getPersistenceStreamId() {
        return persistenceStreamId;
    }

    public void setPersistenceStreamId(String id) {
        this.persistenceStreamId = id;
    }      
        
    public InputStream getBinaryStream() throws SQLException {
        checkReference();
        return this.srcXML.getBinaryStream();
    }

    public Reader getCharacterStream() throws SQLException {
        checkReference();
        return this.srcXML.getCharacterStream();
    }

    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        checkReference();
        return this.srcXML.getSource(sourceClass);
    }

    public String getString() throws SQLException {
        checkReference();
        return this.srcXML.getString();
    }

    public OutputStream setBinaryStream() throws SQLException {
        checkReference();
        return this.srcXML.setBinaryStream();
    }

    public Writer setCharacterStream() throws SQLException {
        checkReference();
        return this.srcXML.setCharacterStream();
    }

    public void setString(String value) throws SQLException {
        checkReference();
        this.srcXML.setString(value);
    }

    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	
    	if (!(o instanceof XMLType)) {
    		return false;
    	}
    	
    	XMLType other = (XMLType)o;
    	
    	if (this.srcXML != null) {
    		return this.srcXML.equals(other.srcXML);
    	}
    	
    	return this.persistenceStreamId == other.persistenceStreamId
				&& this.referenceStreamId == other.referenceStreamId;
    }

    public String toString() {
        checkReference();
        return srcXML.toString();
    }
        
    private void checkReference() {
        if (this.srcXML == null) {
            throw new InvalidReferenceException(CorePlugin.Util.getString("XMLValue.InvalidReference")); //$NON-NLS-1$
        }
    }

	public void free() throws SQLException {
		checkReference();
		this.srcXML.free();
	}

	public <T extends Result> T setResult(Class<T> resultClass)
			throws SQLException {
		checkReference();
		return this.srcXML.setResult(resultClass);
	}      
}
