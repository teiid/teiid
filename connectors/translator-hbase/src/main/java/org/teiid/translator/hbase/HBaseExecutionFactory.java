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
package org.teiid.translator.hbase;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.core.types.BinaryType;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCPlugin;
import org.teiid.translator.jdbc.JDBCUpdateExecution;

@Translator(name="hbase", description="HBase Translator, reads and writes the data to HBase")
public class HBaseExecutionFactory extends JDBCExecutionFactory {
	
	// use to store phoenix hbase table mapping ddl
	private Set<String> cacheSet = Collections.synchronizedSet(new HashSet<String>());
	
	// use to temporally store phoenix hbase table mapping ddl
	private List<String> mappingDDLlist = Collections.synchronizedList(new ArrayList<String>());
	
//    public final static TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

	public HBaseExecutionFactory() {
		super();
		setSupportsFullOuterJoins(false);
	}
	

	@Override
	public void start() throws TranslatorException {
		super.start();
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command
													 , ExecutionContext executionContext
													 , RuntimeMetadata metadata
													 , Connection conn) throws TranslatorException {
		return new HBaseQueryExecution(command, executionContext, metadata, conn, this);
	}

	@Override
	public JDBCUpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			Connection conn) throws TranslatorException {
		// TODO Auto-generated method stub
		return new HBaseUpdateExecution(command, executionContext, metadata, conn, this);
	}

	public HBaseSQLConversionVisitor getSQLConversionVisitor() {
		return new HBaseSQLConversionVisitor(this);
	}


	/*
	 * Phoenix do not support XML, CLOB, BLOB, OBJECT
	 */
	protected boolean isBindEligible(Literal l) {
		return false;
	}

	public Set<String> getDDLCacheSet() {
		return cacheSet;
	}
	
	public List<String> getMappingDDLList() {
		return mappingDDLlist;
	}


	@Override
	public void getMetadata(MetadataFactory metadataFactory, Connection conn) throws TranslatorException {
		
		if (conn == null) {
			throw new TranslatorException(HBasePlugin.Event.TEIID27005, JDBCPlugin.Util.gs(HBasePlugin.Event.TEIID27016));
		}
		MetadataProcessor mp = getMetadataProcessor();
    	if (mp != null) {
    	    PropertiesUtils.setBeanProperties(mp, metadataFactory.getModelProperties(), "importer");
    	    mp.process(metadataFactory, conn);
    	}
	}
	
	@Override
	public MetadataProcessor<Connection> getMetadataProcessor() {
		return new HBaseMetadataProcessor();
	}


	@Override
	public void bindValue(PreparedStatement pstmt, Object param, Class<?> paramType, int i) throws SQLException {

		int type = TypeFacility.getSQLTypeFromRuntimeType(paramType);
		
		if (param == null) {
			pstmt.setNull(i, type);
            return;
        } 
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.STRING)) {
			pstmt.setString(i, String.valueOf(param));
			return;
		}
		
		if (paramType.equals(TypeFacility.RUNTIME_TYPES.VARBINARY)) {
			byte[] bytes ;
			if(param instanceof BinaryType){
				bytes = ((BinaryType)param).getBytesDirect();
			} else {
				bytes = (byte[]) param;
			}
			pstmt.setBytes(i, bytes);
        	return;
        }
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.CHAR)) {
			pstmt.setString(i, String.valueOf(param));
			return;
		}
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN)) {
			pstmt.setBoolean(i, (Boolean)param);
			return;
		}
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.BYTE)) {
			pstmt.setByte(i, (Byte)param);
			return;
		}
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.SHORT)) {
			pstmt.setShort(i, (Short)param);
			return;
		}
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.INTEGER)) {
			pstmt.setInt(i, (Integer)param);
			return;
		}
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.LONG)) {
			pstmt.setLong(i, (Long)param);
			return;
		}
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.FLOAT)) {
			pstmt.setFloat(i, (Float)param);
			return;
		}
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.DOUBLE)) {
			pstmt.setDouble(i, (Double)param);
			return;
		}
		
		if(paramType.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
			pstmt.setBigDecimal(i, (BigDecimal)param);
			return;
		}
		
		if (paramType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
            pstmt.setDate(i,(java.sql.Date)param, getDatabaseCalendar());
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            pstmt.setTime(i,(java.sql.Time)param, getDatabaseCalendar());
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            pstmt.setTimestamp(i,(java.sql.Timestamp)param, getDatabaseCalendar());
            return;
        }
        
        if (TypeFacility.RUNTIME_TYPES.BIG_DECIMAL.equals(paramType)) {
        	pstmt.setBigDecimal(i, (BigDecimal)param);
            return;
        }
        
        if (useStreamsForLobs()) {
        	// Phonix current not support Blob, Clob, XML
        }
      
        pstmt.setObject(i, param, type);
	}


	
	
	
	
}
