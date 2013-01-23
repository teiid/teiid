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
package org.teiid.translator.odata;

import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

import javax.resource.cci.ConnectionFactory;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.*;
import org.teiid.language.Argument.Direction;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

/**
 * TODO:
 * Type coercion	cast(T), cast(x, T)	Perform a type coercion if possible.
 * Type comparison	isof(T), isof(x, T)	Whether targeted instance can be converted to the specified type.
 * media streams are generally not supported yet. 
 */
@Translator(name="odata", description="A translator for making OData data service calls")
public class ODataExecutionFactory extends ExecutionFactory<ConnectionFactory, WSConnection> {
    private static final Map<Class<?>, Integer> TYPE_CODE_MAP = new HashMap<Class<?>, Integer>();
    
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

    static class DatbaseCalender extends ThreadLocal<Calendar> {
    	private String timeZone;
    	public DatbaseCalender(String tz) {
    		this.timeZone = tz;
    	}
    	@Override
    	protected Calendar initialValue() {
            if(this.timeZone != null && this.timeZone.trim().length() > 0) {
            	TimeZone tz = TimeZone.getTimeZone(this.timeZone);
                if(!DEFAULT_TIME_ZONE.hasSameRules(tz)) {
            		return Calendar.getInstance(tz);
                }
            }      		
    		return Calendar.getInstance();
    	}
    };
    
	static final String INVOKE_HTTP = "invokeHttp"; //$NON-NLS-1$
	protected Map<String, FunctionModifier> functionModifiers = new TreeMap<String, FunctionModifier>(String.CASE_INSENSITIVE_ORDER);
	private EdmDataServices eds;
	private String databaseTimeZone;
	private DatbaseCalender databaseCalender;
	
	public ODataExecutionFactory() {
		setSourceRequiredForMetadata(true);
		setSupportsOrderBy(true);
		
		registerFunctionModifier(SourceSystemFunctions.CONVERT, new AliasModifier("cast")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LOCATE, new AliasModifier("indexof")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("tolower")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("toupper")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$
		addPushDownFunction("odata", "startswith", TypeFacility.RUNTIME_NAMES.BOOLEAN, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$
		addPushDownFunction("odata", "substringof", TypeFacility.RUNTIME_NAMES.BOOLEAN, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();		
		this.databaseCalender = new DatbaseCalender(this.databaseTimeZone);
    }	
	
	@TranslatorProperty(display="Database time zone", description="Time zone of the database, if different than Integration Server", advanced=true)
	public String getDatabaseTimeZone() {
		return this.databaseTimeZone;
	}

	public void setDatabaseTimeZone(String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
	}	
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory, WSConnection conn) throws TranslatorException {
		
		List<Argument> parameters = new ArrayList<Argument>();
		parameters.add(new Argument(Direction.IN, new Literal("GET", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null)); //$NON-NLS-1$
		parameters.add(new Argument(Direction.IN, new Literal(null, TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null));
		parameters.add(new Argument(Direction.IN, new Literal("$metadata", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null)); //$NON-NLS-1$
		parameters.add(new Argument(Direction.IN, new Literal(true, TypeFacility.RUNTIME_TYPES.BOOLEAN), TypeFacility.RUNTIME_TYPES.BOOLEAN, null));
		
		Call call = getLanguageFactory().createCall(ODataExecutionFactory.INVOKE_HTTP, parameters, null);
		
		BinaryWSProcedureExecution execution = new BinaryWSProcedureExecution(call, null, null, null, conn);
		execution.addHeader("Content-Type", Collections.singletonList("application/xml")); //$NON-NLS-1$ //$NON-NLS-2$
		execution.addHeader("Accept", Arrays.asList("application/xml", "application/atom+xml")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		execution.execute();
		Blob out = (Blob)execution.getOutputParameterValues().get(0);
		
		try {
			this.eds = new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new InputStreamReader(out.getBinaryStream())));
			ODataMetadataProcessor metadataProcessor = new ODataMetadataProcessor();
			PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getModelProperties(), "importer"); //$NON-NLS-1$
			metadataProcessor.getMetadata(metadataFactory, eds);
		} catch (SQLException e) {
			throw new TranslatorException(e);
		}
	}
	

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
		return new ODataQueryExecution(this, command, executionContext, metadata, connection, this.eds);
	}

	@Override
	public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
		String nativeQuery = command.getMetadataObject().getProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, false);
		if (nativeQuery != null) {
			return new ODataDirectQueryExecution(command.getArguments(), command, executionContext, metadata, connection, nativeQuery);
		}
		return new ODataProcedureExecution(command, this, executionContext, metadata, connection, this.eds);
	}

	@Override
	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
		return new ODataUpdateExecution(command, executionContext, metadata, connection);
	}
	
	@Override
	public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
		 return new ODataDirectQueryExecution(arguments.subList(1, arguments.size()), command, executionContext, metadata, connection, (String)arguments.get(0).getArgumentValue().getValue());
	}	
	
	@Override
	public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(getDefaultSupportedFunctions());

        // String functions
        supportedFunctions.add(SourceSystemFunctions.ENDSWITH); 
        supportedFunctions.add(SourceSystemFunctions.REPLACE); 
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        
        // date functions
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        
        // airthamatic functions
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        
        return supportedFunctions;
	}

    /**
     * Return a map of function name to FunctionModifier.
     * @return Map of function name to FunctionModifier.
     */
    public Map<String, FunctionModifier> getFunctionModifiers() {
    	return functionModifiers;
    }
    
    /**
     * Add the {@link FunctionModifier} to the set of known modifiers.
     * @param name
     * @param modifier
     */
    public void registerFunctionModifier(String name, FunctionModifier modifier) {
    	this.functionModifiers.put(name, modifier);
    }	
	
	
	public List<String> getDefaultSupportedFunctions(){
		return Arrays.asList(new String[] { "+", "-", "*", "/" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
	@Override
    public boolean supportsCompareCriteriaEquals() {
    	return true;
    }

	@Override
    public boolean supportsCompareCriteriaOrdered() {
    	return true;
    }

	@Override
    public boolean supportsIsNullCriteria() {
    	return true;
    }

	@Override
	public boolean supportsOrCriteria() {
    	return true;
    }

	@Override
    public boolean supportsNotCriteria() {
    	return true;
    }

	@Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
    	return false; // TODO:for ANY
    }

	@Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
    	return false; // TODO:FOR ALL
    }

	@Override
    public boolean supportsOrderByUnrelated() {
    	return true;
    }

	@Override
    public boolean supportsAggregatesCount() {
    	return true;
    }
    
	@Override
	public boolean supportsAggregatesCountStar() {
    	return true;
    }

	@Override
    public boolean supportsRowLimit() {
    	return true;
    }

	@Override
    public boolean supportsRowOffset() {
    	return true;
    }

	@Override
	public boolean supportsOnlyLiteralComparison() {
		return true;
	}
	
	@Override
    public boolean useAnsiJoin() {
    	return true;
    }

	public Object retrieveValue(Object value, Class<?> expectedType) throws TranslatorException {
		if (value == null) {
			return null;
		}
		
        try {
			Integer code = TYPE_CODE_MAP.get(expectedType);
			if(code != null) {
			    switch(code.intValue()) {
			        case INTEGER_CODE:
			            return DataTypeManager.transformValue(value, expectedType);
			        case LONG_CODE:
			        	return DataTypeManager.transformValue(value, expectedType);
			        case DOUBLE_CODE:
			        	return DataTypeManager.transformValue(value, expectedType);                    
			        case BIGDECIMAL_CODE:
			        	return DataTypeManager.transformValue(value, expectedType); 
			        case SHORT_CODE:
			        	return DataTypeManager.transformValue(value, expectedType);
			        case FLOAT_CODE:
			        	return DataTypeManager.transformValue(value, expectedType);
			        case TIME_CODE:{
			        	if (value instanceof LocalDateTime) {
			        		DateTime dateTime = ((LocalDateTime) value).toDateTime(DateTimeZone.forTimeZone(this.databaseCalender.get().getTimeZone()));
			        		value = new java.sql.Time(dateTime.getMillis());
			        	}
			        	else if (value instanceof LocalTime) {
			        		value = new java.sql.Time(((LocalTime)value).toDateTimeToday().getMillis());
			        	}
			        	return DataTypeManager.transformValue(value, expectedType);
			        }
			        case DATE_CODE: {
			        	if (value instanceof LocalDateTime) {
			        		DateTime dateTime = ((LocalDateTime) value).toDateTime(DateTimeZone.forTimeZone(this.databaseCalender.get().getTimeZone()));
			        		value = new java.sql.Date(dateTime.getMillis());
			        	}
			        	else if (value instanceof DateTime) {
			        		value = new java.sql.Date(((DateTime)value).getMillis());
			        	}
			        	return DataTypeManager.transformValue(value, expectedType);
			        }
			        case TIMESTAMP_CODE: {
			        	if (value instanceof LocalDateTime) {
			        		DateTime dateTime = ((LocalDateTime) value).toDateTime(DateTimeZone.forTimeZone(this.databaseCalender.get().getTimeZone()));
			        		value = new Timestamp(dateTime.getMillis());
			        	}
			        	else if (value instanceof DateTime) {
			        		value = new Timestamp(((DateTime)value).getMillis());
			        	}
			        	return DataTypeManager.transformValue(value, expectedType);
			        }
					case BLOB_CODE:
						return DataTypeManager.transformValue(value, expectedType);
					case CLOB_CODE:
						return DataTypeManager.transformValue(value, expectedType);
					case BOOLEAN_CODE:
						return DataTypeManager.transformValue(value, expectedType);
			    }
			}
		} catch (TransformationException e) {
			throw new TranslatorException(e);
		}

        // otherwise fall through and call getObject() and rely on the normal
		// translation routines
		return value;
	}
    	
}
