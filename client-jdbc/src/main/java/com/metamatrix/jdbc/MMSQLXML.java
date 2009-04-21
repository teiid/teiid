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

package com.metamatrix.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.SQLException;
//## JDBC4.0-begin ##
import java.sql.SQLXML;
//## JDBC4.0-end ##

/*## JDBC3.0-JDK1.5-begin ##
import com.metamatrix.core.jdbc.SQLXML; 
## JDBC3.0-JDK1.5-end ##*/

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import com.metamatrix.common.lob.LobChunkInputStream;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.dqp.client.impl.StreamingLobChunckProducer;


/** 
 * A MM specific implementation of the SQLXML object which is capable of 
 * accessing the SQLXML object as local object when used in the embedded product
 * at the same time capable of using the streaming interface when used in 
 * remote clients.
 */
public class MMSQLXML implements SQLXML {
	private final StreamingLobChunckProducer.Factory lobChunckFactory;
    
    public static SQLXML newInstance(StreamingLobChunckProducer.Factory lobChunckFactory, XMLType srcXML) throws SQLException {
    	if (Boolean.getBoolean(Streamable.FORCE_STREAMING)) {
    		SQLXML sourceSQLXML = srcXML.getSourceSQLXML();
        	if (sourceSQLXML != null) {
        		return sourceSQLXML;
        	}
        }
        return new MMSQLXML(lobChunckFactory);        
    }
    
    public MMSQLXML(StreamingLobChunckProducer.Factory lobChunckFactory) throws SQLException {
    	this.lobChunckFactory = lobChunckFactory;
    }
    
    public Reader getCharacterStream() throws SQLException {
    	return new LobChunkInputStream(lobChunckFactory.getLobChunkProducer()).getUTF16Reader();
    }

    public String getString() throws SQLException {
    	LobChunkInputStream in = new LobChunkInputStream(lobChunckFactory.getLobChunkProducer());
        try {
        	//## JDBC4.0-begin ##
			return new String(in.getByteContents(), Charset.forName("UTF-16")); //$NON-NLS-1$
			//## JDBC4.0-end ##
			/*## JDBC3.0-JDK1.5-begin ##
			return new String(in.getByteContents(), "UTF-16"); //$NON-NLS-1$ 
			## JDBC3.0-JDK1.5-end ##*/
		} catch (IOException e) {
			throw MMSQLException.create(e);
		} 
    }

    public String toString() {
        try {
            return getString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
		if (sourceClass == null || sourceClass == StreamSource.class) {
			return (T)new StreamSource(getCharacterStream());
		}
		throw new SQLException(JDBCPlugin.Util.getString("MMSQLXML.unsupported_source", sourceClass)); //$NON-NLS-1$
	}

	public InputStream getBinaryStream() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
    
	public void free() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public OutputStream setBinaryStream() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public Writer setCharacterStream() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public <T extends Result> T setResult(Class<T> resultClass)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void setString(String value) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
}
