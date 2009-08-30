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
package org.teiid.connector.jdbc.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.jdbc.JDBCPlugin;
import org.teiid.connector.jdbc.translator.AliasModifier;
import org.teiid.connector.jdbc.translator.ConvertModifier;
import org.teiid.connector.jdbc.translator.ExtractFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.jdbc.translator.LocateFunctionModifier;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.ISetQuery.Operation;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.visitor.util.CollectorVisitor;
import org.teiid.connector.visitor.util.SQLReservedWords;

public class OracleSQLTranslator extends Translator {

	/*
	 * Spatial Functions
	 */
	public static final String RELATE = "sdo_relate"; //$NON-NLS-1$
	public static final String NEAREST_NEIGHBOR = "sdo_nn"; //$NON-NLS-1$
	public static final String FILTER = "sdo_filter"; //$NON-NLS-1$
	public static final String WITHIN_DISTANCE = "sdo_within_distance"; //$NON-NLS-1$
	public static final String NEAREST_NEIGHBOR_DISTANCE = "sdo_nn_distance"; //$NON-NLS-1$

    public final static String HINT_PREFIX = "/*+"; //$NON-NLS-1$
    public final static String DUAL = "DUAL"; //$NON-NLS-1$
    public final static String ROWNUM = "ROWNUM"; //$NON-NLS-1$
    public final static String SEQUENCE = ":SEQUENCE="; //$NON-NLS-1$
	
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        super.initialize(env);
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
        
        //spatial functions
        registerFunctionModifier(RELATE, new OracleSpatialFunctionModifier());
        registerFunctionModifier(NEAREST_NEIGHBOR, new OracleSpatialFunctionModifier());
        registerFunctionModifier(FILTER, new OracleSpatialFunctionModifier());
        registerFunctionModifier(WITHIN_DISTANCE, new OracleSpatialFunctionModifier());
        
        //add in type conversion
        ConvertModifier convertModifier = new ConvertModifier();
    	convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
    	convertModifier.addTypeMapping("date", FunctionModifier.DATE, FunctionModifier.TIME); //$NON-NLS-1$
    	convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
    		@Override
    		public List<?> translate(IFunction function) {
    			return Arrays.asList("to_date('1970-01-01 ' || to_char(",function.getParameters().get(0),", 'HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')"); //$NON-NLS-1$ //$NON-NLS-2$
    		}
    	});
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE, new FunctionModifier() {
			@Override
			public List<?> translate(IFunction function) {
				return Arrays.asList("trunc(cast(",function.getParameters().get(0)," AS date))"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "HH24:MI:SS")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD HH24:MI:SS.FF")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("to_date", "YYYY-MM-DD")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("to_date", "HH24:MI:SS")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("to_timestamp", "YYYY-MM-DD HH24:MI:SS.FF")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addTypeConversion(new ConvertModifier.FormatModifier("to_char"), FunctionModifier.STRING); //$NON-NLS-1$
    	//NOTE: numeric handling in Oracle is split only between integral vs. floating/decimal types
    	convertModifier.addTypeConversion(new ConvertModifier.FormatModifier("to_number"), //$NON-NLS-1$
    			FunctionModifier.FLOAT, FunctionModifier.DOUBLE, FunctionModifier.BIGDECIMAL);
    	convertModifier.addTypeConversion(new FunctionModifier() {
			@Override
			public List<?> translate(IFunction function) {
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
    }
    
    public void handleInsertSequences(IInsert insert) throws ConnectorException {
        /* 
         * If a missing auto_increment column is modeled with name in source indicating that an Oracle Sequence 
         * then pull the Sequence name out of the name in source of the column.
         */
    	if (!(insert.getValueSource() instanceof IInsertExpressionValueSource)) {
    		return;
    	}
    	IInsertExpressionValueSource values = (IInsertExpressionValueSource)insert.getValueSource();
    	List<Element> allElements = insert.getGroup().getMetadataObject().getChildren();
    	if (allElements.size() == values.getValues().size()) {
    		return;
    	}
    	
    	int index = 0;
    	List<IElement> elements = insert.getElements();
    	
    	for (Element element : allElements) {
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
            
            int delimiterIndex = sequence.indexOf(SQLReservedWords.DOT);
            if (delimiterIndex == -1) {
            	throw new ConnectorException("Invalid name in source sequence format.  Expected <element name>" + SEQUENCE + "<sequence name>.<sequence value>, but was " + name); //$NON-NLS-1$ //$NON-NLS-2$
            }
            String sequenceGroupName = sequence.substring(0, delimiterIndex);
            String sequenceElementName = sequence.substring(delimiterIndex + 1);
                
            IGroup sequenceGroup = this.getLanguageFactory().createGroup(sequenceGroupName, null, null);
            IElement sequenceElement = this.getLanguageFactory().createElement(sequenceElementName, sequenceGroup, null, element.getJavaType());
            insert.getElements().add(index, this.getLanguageFactory().createElement(element.getName(), insert.getGroup(), element, element.getJavaType()));
            values.getValues().add(index, sequenceElement);
		}
    }
    
    @Override
    public List<?> translateCommand(ICommand command, ExecutionContext context) {
    	if (command instanceof IInsert) {
    		try {
				handleInsertSequences((IInsert)command);
			} catch (ConnectorException e) {
				throw new RuntimeException(e);
			}
    	}
    	
    	if (!(command instanceof IQueryCommand)) {
    		return null;
    	}
		IQueryCommand queryCommand = (IQueryCommand)command;
		if (queryCommand.getLimit() == null) {
			return null;
    	}
		ILimit limit = queryCommand.getLimit();
		queryCommand.setLimit(null);
    	List<Object> parts = new ArrayList<Object>();
    	parts.add("SELECT "); //$NON-NLS-1$
    	/*
    	 * if all of the columns are aliased, assume that names matter - it actually only seems to matter for
    	 * the first query of a set op when there is a order by.  Rather than adding logic to traverse up,
    	 * we just use the projected names 
    	 */
    	boolean allAliased = true;
    	for (ISelectSymbol selectSymbol : queryCommand.getProjectedQuery().getSelect().getSelectSymbols()) {
			if (!selectSymbol.hasAlias()) {
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
    public String getSourceComment(ExecutionContext context, ICommand command) {
    	String comment = super.getSourceComment(context, command);
    	
    	if (context != null) {
	    	// Check for db hints
		    Object payload = context.getExecutionPayload();
		    if (payload instanceof String) {
		        String payloadString = (String)payload;
		        if (payloadString.startsWith(HINT_PREFIX)) {
		            comment += payloadString + " "; //$NON-NLS-1$
		        }
		    }
    	}
    	
		if (command instanceof IQuery) {
	        //
	        // This simple algorithm determines the hint which will be added to the
	        // query.
	        // Right now, we look through all functions passed in the query
	        // (returned as a collection)
	        // Then we check if any of those functions are sdo_relate
	        // If so, the ORDERED hint is added, if not, it isn't
	        Collection<IFunction> col = CollectorVisitor.collectObjects(IFunction.class, command);
	        for (IFunction func : col) {
	            if (func.getName().equalsIgnoreCase(RELATE)) {
	                return comment + "/*+ ORDERED */ "; //$NON-NLS-1$
	            }
	        }
		}
    	return comment;
    }
    
    /**
     * Don't fully qualify elements if table = DUAL or element = ROWNUM or special stuff is packed into name in source value.
     *  
     * @see org.teiid.connector.visitor.util.SQLStringVisitor#skipGroupInElement(java.lang.String, java.lang.String)
     * @since 5.0
     */
    @Override
    public String replaceElementName(String group, String element) {        

        // Check if the element was modeled as using a Sequence
        int useIndex = element.indexOf(SEQUENCE);
        if (useIndex >= 0) {
        	String name = element.substring(0, useIndex);
        	if (group != null) {
        		return group + SQLReservedWords.DOT + name;
        	}
        	return name;
        }

        // Check if the group name should be discarded
        if((group != null && group.equalsIgnoreCase(DUAL)) || element.equalsIgnoreCase(ROWNUM)) {
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
    public String getDefaultConnectionTestQuery() {
    	return "Select 'x' from DUAL"; //$NON-NLS-1$
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
    public void afterInitialConnectionCreation(Connection connection) {
    	String errorStr = JDBCPlugin.Util.getString("ConnectionListener.failed_to_report_oracle_connection_details"); //$NON-NLS-1$
    	ResultSet rs = null;
        Statement stmt = null;
        try {                
            stmt = connection.createStatement();
            rs = stmt.executeQuery("select * from v$instance"); //$NON-NLS-1$ 
            
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuffer sb = new StringBuffer();
                for (int i = 1; i <= columnCount; i++) {
                    sb.append(rs.getMetaData().getColumnName(i)).append("=").append(rs.getString(i)).append(";"); //$NON-NLS-1$ //$NON-NLS-2$
                }                    
                // log the queried information
                getEnvironment().getLogger().logInfo(sb.toString());                    
            }                
            
        } catch (SQLException e) {
            getEnvironment().getLogger().logInfo(errorStr); 
        }finally {
            try {
                if (rs != null) {
                    rs.close();
                } 
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e1) {
                getEnvironment().getLogger().logInfo(errorStr);
            }
        }
    }
    
    @Override
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.HIGH;
    }
    
    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return OracleCapabilities.class;
    }
}
