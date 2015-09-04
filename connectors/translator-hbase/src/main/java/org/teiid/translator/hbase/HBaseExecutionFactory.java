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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Command;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;
import org.teiid.translator.jdbc.JDBCUpdateExecution;

@Translator(name="hbase", description="HBase Translator, reads and writes the data to HBase")
public class HBaseExecutionFactory extends JDBCExecutionFactory {
    
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
        return new HBaseUpdateExecution(command, executionContext, metadata, conn, this);
    }

    public HBaseSQLConversionVisitor getSQLConversionVisitor() {
        return new HBaseSQLConversionVisitor(this);
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
        
        if (useStreamsForLobs()) {
            // Phonix current not support Blob, Clob, XML
        }
      
        pstmt.setObject(i, param, type);
    }

    @Override
    public boolean supportsInsertWithQueryExpression() {
    	return true;
    }
    
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
    	if(booleanValue.booleanValue()) {
            return "true"; //$NON-NLS-1$
        }
        return "false"; //$NON-NLS-1$
    }
    
    /**
     * Adding a specific workaround for just Pheonix and BigDecimal.
     */
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
    	if (!(obj instanceof Literal)) {
    		return super.translate(obj, context);
    	}
		Literal l = (Literal)obj;
		if (l.isBindEligible() || l.getType() != TypeFacility.RUNTIME_TYPES.BIG_DECIMAL) {
			return super.translate(obj, context);
		}
		BigDecimal bd = ((BigDecimal)l.getValue());
		if (bd.scale() == 0) {
			l.setValue(bd.setScale(1));
		}
		return null;
    }
    
    /**
     * It doesn't appear that the time component is needed, but for consistency with their
     * documentation, we'll add it.
     */
    @Override
    public String translateLiteralDate(java.sql.Date dateValue) {
    	return "DATE '" + formatDateValue(new Timestamp(dateValue.getTime())) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * A date component is required, so create a new Timestamp instead
     */
    @Override
    public String translateLiteralTime(Time timeValue) {
    	return "TIME '" + formatDateValue(new Timestamp(timeValue.getTime())) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
    	return "TIMESTAMP '" + formatDateValue(timestampValue) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * The Phoenix driver has issues using a calendar object.
     * it throws an npe on a null value and also has https://issues.apache.org/jira/browse/PHOENIX-869
     */
    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
    		Class<?> expectedType) throws SQLException {
    	Integer code = DataTypeManager.getTypeCode(expectedType);
        if(code != null) {
	    	switch (code) {
	    		case DataTypeManager.DefaultTypeCodes.TIME: {
	        		return results.getTime(columnIndex);
	            }
	            case DataTypeManager.DefaultTypeCodes.DATE: {
	        		return results.getDate(columnIndex);
	            }
	            case DataTypeManager.DefaultTypeCodes.TIMESTAMP: {
	        		return results.getTimestamp(columnIndex);
	            }
	    	}
        }
        return super.retrieveValue(results, columnIndex, expectedType);
    }
    
    @Override
    protected JDBCMetdataProcessor createMetadataProcessor() {
    	JDBCMetdataProcessor processor = new JDBCMetdataProcessor() {
    		@Override
    		protected boolean getIndexInfoForTable(String catalogName,
    				String schemaName, String tableName, boolean uniqueOnly,
    				boolean approximateIndexes, String tableType) {
    			//unique returns an empty result set that is not reusable
    			return !uniqueOnly;
    		}
    	};
    	//same issue with foreign keys
    	processor.setImportForeignKeys(false);
    	return processor;
    }
    
}
