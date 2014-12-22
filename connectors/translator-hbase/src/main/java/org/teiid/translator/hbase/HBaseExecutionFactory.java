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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.resource.cci.ConnectionFactory;
import javax.sql.rowset.serial.SerialStruct;

import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BinaryType;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.HBaseConnection;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;

@Translator(name="hbase", description="HBase Translator, reads and writes the data to HBase")
public class HBaseExecutionFactory extends ExecutionFactory<ConnectionFactory, HBaseConnection> {
	
	// use to store phoenix hbase table mapping ddl
	private Set<String> cacheSet = Collections.synchronizedSet(new HashSet<String>());
	
	private int maxInsertBatchSize = 2048;
	private boolean useBindVariables = true;
	
	private static final Map<Class<?>, Integer> TYPE_CODE_MAP = new HashMap<Class<?>, Integer>();
    
	private StructRetrieval structRetrieval = StructRetrieval.OBJECT;
	
    private static final int INTEGER_CODE = 0;
    private static final int LONG_CODE = 1;
    private static final int DOUBLE_CODE = 2;
    private static final int BIGDECIMAL_CODE = 3;
    private static final int SHORT_CODE = 4;
    private static final int FLOAT_CODE = 5;
    private static final int TIME_CODE = 6;
    private static final int DATE_CODE = 7;
    private static final int TIMESTAMP_CODE = 8;
    private static final int BLOB_CODE = 9;
    private static final int CLOB_CODE = 10;
    private static final int BOOLEAN_CODE = 11;
    
    static {
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.INTEGER, new Integer(INTEGER_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.LONG, new Integer(LONG_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.DOUBLE, new Integer(DOUBLE_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL, new Integer(BIGDECIMAL_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.SHORT, new Integer(SHORT_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.FLOAT, new Integer(FLOAT_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.TIME, new Integer(TIME_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.DATE, new Integer(DATE_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.TIMESTAMP, new Integer(TIMESTAMP_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.BLOB, new Integer(BLOB_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.CLOB, new Integer(CLOB_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.BOOLEAN, new Integer(BOOLEAN_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.BYTE, new Integer(SHORT_CODE));
    }
    
    public final static TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

	public HBaseExecutionFactory() {
		
	}
	
	public enum StructRetrieval {
    	OBJECT,
    	COPY,
    	ARRAY
    }

	@Override
	public void start() throws TranslatorException {
		super.start();
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command
													 , ExecutionContext executionContext
													 , RuntimeMetadata metadata
													 , HBaseConnection connection) throws TranslatorException {
	
		return new HBaseQueryExecution(this, command, executionContext, metadata, connection);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command
											   , ExecutionContext executionContext
											   , RuntimeMetadata metadata
											   , HBaseConnection connection) throws TranslatorException {

		return new HBaseUpdateExecution(this, command, executionContext, metadata, connection);
	}

	@Override
	public ProcedureExecution createProcedureExecution(Call command
													 , ExecutionContext executionContext
													 , RuntimeMetadata metadata
													 , HBaseConnection connection) throws TranslatorException {
		
		return super.createProcedureExecution(command, executionContext, metadata, connection);
	}

	public SQLConversionVisitor getSQLConversionVisitor() {
		return new SQLConversionVisitor(this);
	}

	public boolean usePreparedStatements() {
		return useBindVariables();
	}
	
	@TranslatorProperty(display="Use Bind Variables", description="Use prepared statements and bind variables",advanced=true)
	public boolean useBindVariables() {
		return this.useBindVariables;
	}

	public void setUseBindVariables(boolean useBindVariables) {
		this.useBindVariables = useBindVariables;
	}

	 /**
     * Get the max number of inserts to perform in one batch.
     * @return
     */
    @TranslatorProperty(display="Max Prepared Insert Batch Size", description="The max size of a prepared insert batch.  Default 2048.", advanced=true)
    public int getMaxPreparedInsertBatchSize() {
    	return maxInsertBatchSize;
    }
    
    public void setMaxPreparedInsertBatchSize(int maxInsertBatchSize) {
    	if (maxInsertBatchSize < 1) {
    		throw new AssertionError("Max prepared batch insert size must be greater than 0"); //$NON-NLS-1$
    	}
		this.maxInsertBatchSize = maxInsertBatchSize;
	}

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
			byte[] bytes = ((BinaryType)param).getBytesDirect();
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
            pstmt.setDate(i,(java.sql.Date)param);
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            pstmt.setTime(i,(java.sql.Time)param);
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            pstmt.setTimestamp(i,(java.sql.Timestamp)param);
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

	private boolean useStreamsForLobs() {
		return false;
	}

	public Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException {
		Integer code = TYPE_CODE_MAP.get(expectedType);
        if(code != null) {
            switch(code.intValue()) {
                case INTEGER_CODE:  {
                    int value = results.getInt(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }
                    return Integer.valueOf(value);
                }
                case LONG_CODE:  {
                    long value = results.getLong(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Long.valueOf(value);
                }                
                case DOUBLE_CODE:  {
                    double value = results.getDouble(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Double.valueOf(value);
                }                
                case BIGDECIMAL_CODE:  {
                    return results.getBigDecimal(columnIndex); 
                }
                case SHORT_CODE:  {
                    short value = results.getShort(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }                    
                    return Short.valueOf(value);
                }
                case FLOAT_CODE:  {
                    float value = results.getFloat(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Float.valueOf(value);
                }
                case TIME_CODE: {
            		return results.getTime(columnIndex);
                }
                case DATE_CODE: {
            		return results.getDate(columnIndex);
                }
                case TIMESTAMP_CODE: {
            		return results.getTimestamp(columnIndex);
                }
    			case BLOB_CODE: {
    				try {
    					return results.getBlob(columnIndex);
    				} catch (SQLException e) {
    					// ignore
    				}
    				try {
    					return results.getBytes(columnIndex);
    				} catch (SQLException e) {
    					// ignore
    				}
    				break;
    			}
    			case CLOB_CODE: {
    				try {
    					return results.getClob(columnIndex);
    				} catch (SQLException e) {
    					// ignore
    				}
    				break;
    			}  
    			case BOOLEAN_CODE: {
    				return results.getBoolean(columnIndex);
    			}
            }
        }

        Object result = results.getObject(columnIndex);
        if (expectedType == TypeFacility.RUNTIME_TYPES.OBJECT) {
        	return convertObject(result);
        }
		return result;
	}
	
	protected Object convertObject(Object object) throws SQLException {
    	if (object instanceof Struct) {
    		switch (structRetrieval) {
    		case OBJECT:
    			return object;
    		case ARRAY:
    			return new ArrayImpl(((Struct)object).getAttributes());
    		case COPY:
    			return new SerialStruct((Struct)object, Collections.<String, Class<?>> emptyMap());
    		}
    	}
    	return object;
	}
	
	/*
	 * Current Phoenix do not support XML, CLOB, BLOB, OBJECT
	 */
	protected boolean isBindEligible(Literal l) {
		return TypeFacility.RUNTIME_TYPES.XML.equals(l.getType())
				|| TypeFacility.RUNTIME_TYPES.CLOB.equals(l.getType())
				|| TypeFacility.RUNTIME_TYPES.BLOB.equals(l.getType())
				|| TypeFacility.RUNTIME_TYPES.OBJECT.equals(l.getType());
	}

	public Set<String> getDDLCacheSet() {
		return cacheSet;
	}

	public void setFetchSize(Command command, ExecutionContext executionContext, Statement statement, int fetchSize) throws SQLException {
		statement.setFetchSize(fetchSize);
	}

	@Override
	public MetadataProcessor<HBaseConnection> getMetadataProcessor() {
		return new HBaseMetadataProcessor();
	}
	
	

	
}
