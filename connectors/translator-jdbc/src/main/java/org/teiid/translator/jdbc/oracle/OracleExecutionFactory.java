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

/*
 */
package org.teiid.translator.jdbc.oracle;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SetQuery.Operation;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCPlugin;
import org.teiid.translator.jdbc.LocateFunctionModifier;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TranslatedCommand;


@Translator(name="oracle", description="A translator for Oracle 9i Database or later")
public class OracleExecutionFactory extends JDBCExecutionFactory {
	
	private static final String TIME_FORMAT = "HH24:MI:SS"; //$NON-NLS-1$
	private static final String DATE_FORMAT = "YYYY-MM-DD"; //$NON-NLS-1$
	private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT; //$NON-NLS-1$
	private static final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".FF";  //$NON-NLS-1$

    public final static String HINT_PREFIX = "/*+"; //$NON-NLS-1$
    public static final String HINT_SUFFIX = "*/";  //$NON-NLS-1$
    public final static String DUAL = "DUAL"; //$NON-NLS-1$
    public final static String ROWNUM = "ROWNUM"; //$NON-NLS-1$
    public final static String SEQUENCE = ":SEQUENCE="; //$NON-NLS-1$
	/*
	 * Spatial Functions
	 */
	public static final String RELATE = "sdo_relate"; //$NON-NLS-1$
	public static final String NEAREST_NEIGHBOR = "sdo_nn"; //$NON-NLS-1$
	public static final String FILTER = "sdo_filter"; //$NON-NLS-1$
	public static final String WITHIN_DISTANCE = "sdo_within_distance"; //$NON-NLS-1$
	public static final String NEAREST_NEIGHBOR_DISTANCE = "sdo_nn_distance"; //$NON-NLS-1$
	public static final String ORACLE_SDO = "Oracle-SDO"; //$NON-NLS-1$

	/*
	 * Handling for cursor return values
	 */
	static final class RefCursorType {}
	static int CURSOR_TYPE = -10;

	private boolean oracleSuppliedDriver = true;
    
    public void start() throws TranslatorException {
        super.start();
        
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("nvl")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOG10, new Log10FunctionModifier(getLanguageFactory())); 
        registerFunctionModifier(SourceSystemFunctions.HOUR, new ExtractFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.SECOND, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Month"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Day"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.WEEK, new DayWeekQuarterFunctionModifier("WW"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new DayWeekQuarterFunctionModifier("Q"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new DayWeekQuarterFunctionModifier("D"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new DayWeekQuarterFunctionModifier("DDD"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory(), "INSTR", true)); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new ConcatFunctionModifier(getLanguageFactory())); 
        registerFunctionModifier(SourceSystemFunctions.COT, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				function.setName(SourceSystemFunctions.TAN);
				return Arrays.asList(getLanguageFactory().createFunction(SourceSystemFunctions.DIVIDE_OP, new Expression[] {new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER), function}, TypeFacility.RUNTIME_TYPES.DOUBLE));
			}
		});
        
        //spatial functions
        registerFunctionModifier(OracleExecutionFactory.RELATE, new OracleSpatialFunctionModifier());
        registerFunctionModifier(OracleExecutionFactory.NEAREST_NEIGHBOR, new OracleSpatialFunctionModifier());
        registerFunctionModifier(OracleExecutionFactory.FILTER, new OracleSpatialFunctionModifier());
        registerFunctionModifier(OracleExecutionFactory.WITHIN_DISTANCE, new OracleSpatialFunctionModifier());
        
        //add in type conversion
        ConvertModifier convertModifier = new ConvertModifier();
    	convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
    	convertModifier.addTypeMapping("date", FunctionModifier.DATE, FunctionModifier.TIME); //$NON-NLS-1$
    	convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
    		@Override
    		public List<?> translate(Function function) {
    			return Arrays.asList("case when ", function.getParameters().get(0), " is null then null else to_date('1970-01-01 ' || to_char(",function.getParameters().get(0),", 'HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') end"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    		}
    	});
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("trunc(cast(",function.getParameters().get(0)," AS date))"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", DATE_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", TIME_FORMAT)); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				//if column and type is date, just use date format
				Expression ex = function.getParameters().get(0);
				String format = TIMESTAMP_FORMAT; 
				if (ex instanceof ColumnReference && "date".equalsIgnoreCase(((ColumnReference)ex).getMetadataObject().getNativeType())) { //$NON-NLS-1$
					format = DATETIME_FORMAT; 
				} else if (!(ex instanceof Literal) && !(ex instanceof Function)) {
					//this isn't needed in every case, but it's simpler than inspecting the expression more
					ex = ConvertModifier.createConvertFunction(getLanguageFactory(), function.getParameters().get(0), TypeFacility.RUNTIME_NAMES.TIMESTAMP);
				}
				return Arrays.asList("to_char(", ex, ", '", format, "')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
		});
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("to_date", DATE_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("to_date", TIME_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("to_timestamp", TIMESTAMP_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addTypeConversion(new ConvertModifier.FormatModifier("to_char"), FunctionModifier.STRING); //$NON-NLS-1$
    	//NOTE: numeric handling in Oracle is split only between integral vs. floating/decimal types
    	convertModifier.addTypeConversion(new ConvertModifier.FormatModifier("to_number"), //$NON-NLS-1$
    			FunctionModifier.FLOAT, FunctionModifier.DOUBLE, FunctionModifier.BIGDECIMAL);
    	convertModifier.addTypeConversion(new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				if (Number.class.isAssignableFrom(function.getParameters().get(0).getType())) {
					return Arrays.asList("trunc(", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
				}
				return Arrays.asList("trunc(to_number(", function.getParameters().get(0), "))"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}, 
		FunctionModifier.BYTE, FunctionModifier.SHORT, FunctionModifier.INTEGER, FunctionModifier.LONG,	FunctionModifier.BIGINTEGER);
    	convertModifier.addNumericBooleanConversions();
    	convertModifier.setWideningNumericImplicit(true);
    	registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
    	
    	addPushDownFunction(ORACLE_SDO, RELATE, STRING, STRING, STRING, STRING);
    	addPushDownFunction(ORACLE_SDO, RELATE, STRING, OBJECT, OBJECT, STRING);
    	addPushDownFunction(ORACLE_SDO, RELATE, STRING, STRING, OBJECT, STRING);
    	addPushDownFunction(ORACLE_SDO, RELATE, STRING, OBJECT, STRING, STRING);
    	addPushDownFunction(ORACLE_SDO, NEAREST_NEIGHBOR, STRING, STRING, OBJECT, STRING, INTEGER);
    	addPushDownFunction(ORACLE_SDO, NEAREST_NEIGHBOR, STRING, OBJECT, OBJECT, STRING, INTEGER);
    	addPushDownFunction(ORACLE_SDO, NEAREST_NEIGHBOR, STRING, OBJECT, STRING, STRING, INTEGER);
    	addPushDownFunction(ORACLE_SDO, NEAREST_NEIGHBOR_DISTANCE, INTEGER, INTEGER);
    	addPushDownFunction(ORACLE_SDO, WITHIN_DISTANCE, STRING, OBJECT, OBJECT, STRING);
    	addPushDownFunction(ORACLE_SDO, WITHIN_DISTANCE, STRING, STRING, OBJECT, STRING);
    	addPushDownFunction(ORACLE_SDO, WITHIN_DISTANCE, STRING, OBJECT, STRING, STRING);
    	addPushDownFunction(ORACLE_SDO, FILTER, STRING, OBJECT, STRING, STRING);
    	addPushDownFunction(ORACLE_SDO, FILTER, STRING, OBJECT, OBJECT, STRING);
    	addPushDownFunction(ORACLE_SDO, FILTER, STRING, STRING, OBJECT, STRING);
    }
    
    public void handleInsertSequences(Insert insert) throws TranslatorException {
        /* 
         * If a missing auto_increment column is modeled with name in source indicating that an Oracle Sequence 
         * then pull the Sequence name out of the name in source of the column.
         */
    	if (!(insert.getValueSource() instanceof ExpressionValueSource)) {
    		return;
    	}
    	ExpressionValueSource values = (ExpressionValueSource)insert.getValueSource();
    	List<Column> allElements = insert.getTable().getMetadataObject().getColumns();
    	if (allElements.size() == values.getValues().size()) {
    		return;
    	}
    	
    	int index = 0;
    	List<ColumnReference> elements = insert.getColumns();
    	
    	for (Column element : allElements) {
    		if (!element.isAutoIncremented()) {
    			continue;
    		}
    		String name = element.getNameInSource();
    		int seqIndex = name.indexOf(SEQUENCE);
    		if (seqIndex == -1) {
    			continue;
    		}
    		boolean found = false;
    		while (index < elements.size()) {
    			if (element.equals(elements.get(index).getMetadataObject())) {
    				found = true;
    				break;
    			}
    			index++;
    		}
    		if (found) {
    			continue;
    		}
    		
            String sequence = name.substring(seqIndex + SEQUENCE.length());
            
            int delimiterIndex = sequence.indexOf(Tokens.DOT);
            if (delimiterIndex == -1) {
            	throw new TranslatorException("Invalid name in source sequence format.  Expected <element name>" + SEQUENCE + "<sequence name>.<sequence value>, but was " + name); //$NON-NLS-1$ //$NON-NLS-2$
            }
            String sequenceGroupName = sequence.substring(0, delimiterIndex);
            String sequenceElementName = sequence.substring(delimiterIndex + 1);
                
            NamedTable sequenceGroup = this.getLanguageFactory().createNamedTable(sequenceGroupName, null, null);
            ColumnReference sequenceElement = this.getLanguageFactory().createColumnReference(sequenceElementName, sequenceGroup, null, element.getJavaType());
            insert.getColumns().add(index, this.getLanguageFactory().createColumnReference(element.getName(), insert.getTable(), element, element.getJavaType()));
            values.getValues().add(index, sequenceElement);
		}
    }
    
    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
    	if (command instanceof Insert) {
    		try {
				handleInsertSequences((Insert)command);
			} catch (TranslatorException e) {
				throw new RuntimeException(e);
			}
    	}
    	
    	if (!(command instanceof QueryExpression)) {
    		return null;
    	}
		QueryExpression queryCommand = (QueryExpression)command;
		if (queryCommand.getLimit() == null) {
			return null;
    	}
		Limit limit = queryCommand.getLimit();
		queryCommand.setLimit(null);
    	List<Object> parts = new ArrayList<Object>();
    	parts.add("SELECT "); //$NON-NLS-1$
    	/*
    	 * if all of the columns are aliased, assume that names matter - it actually only seems to matter for
    	 * the first query of a set op when there is a order by.  Rather than adding logic to traverse up,
    	 * we just use the projected names 
    	 */
    	boolean allAliased = true;
    	for (DerivedColumn selectSymbol : queryCommand.getProjectedQuery().getDerivedColumns()) {
			if (selectSymbol.getAlias() == null) {
				allAliased = false;
				break;
			}
		}
    	if (allAliased) {
	    	String[] columnNames = queryCommand.getColumnNames();
	    	for (int i = 0; i < columnNames.length; i++) {
	    		if (i > 0) {
	    			parts.add(", "); //$NON-NLS-1$
	    		}
	    		parts.add(columnNames[i]);
			}
    	} else {
        	parts.add("*"); //$NON-NLS-1$
    	}
		if (limit.getRowOffset() > 0) {
			parts.add(" FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM ("); //$NON-NLS-1$
		} else {
			parts.add(" FROM ("); //$NON-NLS-1$ 
		}
		parts.add(queryCommand);
		if (limit.getRowOffset() > 0) {
			parts.add(") VIEW_FOR_LIMIT WHERE ROWNUM <= "); //$NON-NLS-1$
			parts.add(limit.getRowLimit() + limit.getRowOffset());
			parts.add(") WHERE ROWNUM_ > "); //$NON-NLS-1$
			parts.add(limit.getRowOffset());
		} else {
			parts.add(") WHERE ROWNUM <= "); //$NON-NLS-1$
			parts.add(limit.getRowLimit());
		}
		return parts;
    }

    @Override
    public boolean useAsInGroupAlias(){
        return false;
    }
    
    @Override
    public String getSetOperationString(Operation operation) {
    	if (operation == Operation.EXCEPT) {
    		return "MINUS"; //$NON-NLS-1$
    	}
    	return super.getSetOperationString(operation);
    }
    
    @Override
    public String getSourceComment(ExecutionContext context, Command command) {
    	String comment = super.getSourceComment(context, command);
    	
    	boolean usingPayloadComment = false;
    	if (context != null) {
	    	// Check for db hints
		    Object payload = context.getExecutionPayload();
		    if (payload instanceof String) {
		        String payloadString = (String)payload;
		        if (payloadString.startsWith(HINT_PREFIX)) {
		        	int i = payloadString.indexOf(HINT_SUFFIX);
		        	if (i > 0 && payloadString.substring(i + 2).trim().length() == 0) {
			            comment += payloadString + " "; //$NON-NLS-1$
			            usingPayloadComment = true;
		        	} else {
		        		String msg = JDBCPlugin.Util.getString("OraleExecutionFactory.invalid_hint", "Execution Payload", payloadString); //$NON-NLS-1$ //$NON-NLS-2$ 
		        		context.addWarning(new TranslatorException(msg));
		        		LogManager.logWarning(LogConstants.CTX_CONNECTOR, msg);
		        	}
		        }
		    }
    	}
    	
    	if (!usingPayloadComment && context != null) {
    		String hint = null;
    		hint = context.getSourceHint();
    		if (hint == null) {
    			hint = context.getGeneralHint();
    		}
    		if (hint != null) {
    			//append a source hint
    			if (!hint.contains(HINT_PREFIX)) {
    				comment += HINT_PREFIX + ' ' + hint + ' ' + HINT_SUFFIX + ' ';
    			} else {
    				String msg = JDBCPlugin.Util.getString("OraleExecutionFactory.invalid_hint", "Source Hint", hint); //$NON-NLS-1$ //$NON-NLS-2$
    				context.addWarning(new TranslatorException(msg));
	        		LogManager.logWarning(LogConstants.CTX_CONNECTOR, msg);
    			}
    		}
    	}
    	
		if (command instanceof Select) {
	        //
	        // This simple algorithm determines the hint which will be added to the
	        // query.
	        // Right now, we look through all functions passed in the query
	        // (returned as a collection)
	        // Then we check if any of those functions are sdo_relate
	        // If so, the ORDERED hint is added, if not, it isn't
	        Collection<Function> col = CollectorVisitor.collectObjects(Function.class, command);
	        for (Function func : col) {
	            if (func.getName().equalsIgnoreCase(OracleExecutionFactory.RELATE)) {
	                return comment + "/*+ ORDERED */ "; //$NON-NLS-1$
	            }
	        }
		}
    	return comment;
    }
    
    /**
     * Don't fully qualify elements if table = DUAL or element = ROWNUM or special stuff is packed into name in source value.
     *  
     * @see org.teiid.language.visitor.SQLStringVisitor#skipGroupInElement(java.lang.String, java.lang.String)
     * @since 5.0
     */
    @Override
    public String replaceElementName(String group, String element) {        

        // Check if the element was modeled as using a Sequence
        int useIndex = element.indexOf(SEQUENCE);
        if (useIndex >= 0) {
        	String name = element.substring(0, useIndex);
        	if (group != null) {
        		return group + Tokens.DOT + name;
        	}
        	return name;
        }

        // Check if the group name should be discarded
        if((group != null && DUAL.equalsIgnoreCase(group)) || element.equalsIgnoreCase(ROWNUM)) {
            // Strip group if group or element are pseudo-columns
            return element;
        }
        
        return null;
    }
    
    @Override
    public boolean hasTimeType() {
    	return false;
    }
       
    @Override
    public void bindValue(PreparedStatement stmt, Object param, Class<?> paramType, int i) throws SQLException {
    	if(param == null && Object.class.equals(paramType)){
    		//Oracle drive does not support JAVA_OBJECT type
    		stmt.setNull(i, Types.LONGVARBINARY);
    		return;
    	}
    	super.bindValue(stmt, param, paramType, i);
    }
    
    @Override
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.HIGH;
    }
    
    @Override
    public boolean supportsOrderByNullOrdering() {
    	return true;
    }    
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.COT); 
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        supportedFunctions.add("CEILING"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("DAY"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        //supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
        //supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
        //supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$ 
        //supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
        //supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
        //supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$          
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL");      //$NON-NLS-1$ 
        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
        supportedFunctions.add(RELATE);
        supportedFunctions.add(NEAREST_NEIGHBOR);
        supportedFunctions.add(NEAREST_NEIGHBOR_DISTANCE);
        supportedFunctions.add(WITHIN_DISTANCE);
        supportedFunctions.add(FILTER);
        return supportedFunctions;
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
    	if (timestampValue.getNanos() == 0) {
    		String val = formatDateValue(timestampValue);
    		val = val.substring(0, val.length() - 2);
    		return "to_date('" + val + "', '" + DATETIME_FORMAT + "')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	}
    	return super.translateLiteralTimestamp(timestampValue);
    }
    
    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
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
    public boolean supportsExcept() {
        return true;
    }
    
    @Override
    public boolean supportsIntersect() {
        return true;
    }
    
    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
    	return true;
    }
    
    @Override
    public boolean supportsElementaryOlapOperations() {
    	return true;
    }
    
    @Override
    public boolean supportsLikeRegex() {
    	return true;
    }
    
    @Override
    public String getLikeRegexString() {
    	return "REGEXP_LIKE"; //$NON-NLS-1$
    }
    
    public void setOracleSuppliedDriver(boolean oracleNative) {
		this.oracleSuppliedDriver = oracleNative;
	}
    
	@TranslatorProperty(display="Oracle Native Driver", description="True if the driver is an Oracle supplied driver",advanced=true)
    public boolean isOracleSuppliedDriver() {
		return oracleSuppliedDriver;
	}
		
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
    	if (oracleSuppliedDriver && obj instanceof Call) {
    		Call call = (Call)obj;
    		if (call.getReturnType() == null && call.getMetadataObject() != null && call.getMetadataObject().getProperty(SQLConversionVisitor.TEIID_NATIVE_QUERY, false) == null) { 
    			//oracle returns the resultset as a parameter
    			call.setReturnType(RefCursorType.class);
    		}
    	}
    	return super.translate(obj, context);
    }
    
    @Override
    protected void registerSpecificTypeOfOutParameter(
    		CallableStatement statement, Class<?> runtimeType, int index)
    		throws SQLException {
    	if (oracleSuppliedDriver && index == 1 && runtimeType == RefCursorType.class) {
    		statement.registerOutParameter(1, CURSOR_TYPE);
    	} else {
    		super.registerSpecificTypeOfOutParameter(statement, runtimeType, index);
    	}
    }
    
    @Override
    public ResultSet executeStoredProcedure(CallableStatement statement,
    		TranslatedCommand command, Class<?> returnType) throws SQLException {
    	ResultSet rs = super.executeStoredProcedure(statement, command, returnType);
    	if (!oracleSuppliedDriver || returnType != RefCursorType.class) {
    		return rs;
    	}
    	return (ResultSet)statement.getObject(1);
    }
    
}
