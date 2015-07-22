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

package org.teiid.translator.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialStruct;

import org.teiid.core.TeiidException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.util.MixinProxy;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.*;
import org.teiid.language.Argument.Direction;
import org.teiid.language.SetQuery.Operation;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;


/**
 * JDBC implementation of Connector interface.
 */
@Translator(name="jdbc-ansi", description="JDBC ANSI translator, can used with any ANSI compatible JDBC Driver")
public class JDBCExecutionFactory extends ExecutionFactory<DataSource, Connection> {

	public static final int DEFAULT_MAX_IN_CRITERIA = 1000;
	public static final int DEFAULT_MAX_DEPENDENT_PREDICATES = 50;
	
    public enum StructRetrieval {
    	OBJECT,
    	COPY,
    	ARRAY
    }
	
    private final ThreadLocal<MessageFormat> comment = new ThreadLocal<MessageFormat>() {
    	@Override
        protected MessageFormat initialValue() {
    		return new MessageFormat(commentFormat);
    	}
    };
    public final static TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

    static class DatabaseCalender extends ThreadLocal<Calendar> {
    	private String timeZone;
    	public DatabaseCalender(String tz) {
    		this.timeZone = tz;
    	}
    	@Override
    	protected Calendar initialValue() {
            if(this.timeZone != null && this.timeZone.trim().length() > 0) {
            	TimeZone tz = TimeZone.getTimeZone(this.timeZone);
                if(!TimestampWithTimezone.getCalendar().getTimeZone().hasSameRules(tz)) {
            		return Calendar.getInstance(tz);
                }
            }      		
    		return Calendar.getInstance();
    	}
    };
    
    private Map<String, FunctionModifier> functionModifiers = new TreeMap<String, FunctionModifier>(String.CASE_INSENSITIVE_ORDER);
	
	private boolean useBindVariables = true;
	private String databaseTimeZone;
	private boolean trimStrings;
	private boolean useCommentsInSourceQuery;
	private Version version;
	private int maxInsertBatchSize = 2048;
	private DatabaseCalender databaseCalender;
	private boolean supportsGeneratedKeys;
	private StructRetrieval structRetrieval = StructRetrieval.OBJECT;
	protected SQLDialect dialect; 
	private boolean enableDependentJoins;
	private boolean useBindingsForDependentJoin = true;
	private String commentFormat = "/*teiid sessionid:{0}, requestid:{1}.{2}*/ "; //$NON-NLS-1$
	
	private AtomicBoolean initialConnection = new AtomicBoolean(true);
	
	public JDBCExecutionFactory() {
		setSupportsFullOuterJoins(true);
		setSupportsOrderBy(true);
		setSupportsOuterJoins(true);
		setSupportsSelectDistinct(true);
		setSupportsInnerJoins(true);
		setMaxInCriteriaSize(DEFAULT_MAX_IN_CRITERIA);
		setMaxDependentInPredicates(DEFAULT_MAX_DEPENDENT_PREDICATES);
	}
    
	@Override
	public void start() throws TranslatorException {
		super.start();		
		this.databaseCalender = new DatabaseCalender(this.databaseTimeZone);
		if (useCommentsInSourceQuery) {
			//will throw an exception if not valid
			new MessageFormat(commentFormat);
		}
    }
	
    @TranslatorProperty(display="Database Version", description= "Database Version")
    public String getDatabaseVersion() {
    	return this.version.toString();
    }
    
    /**
     * Sets the database version.  See also {@link #getVersion()}
     * @param version
     */
    public void setDatabaseVersion(String version) {
    	this.version = Version.getVersion(version);
    }
    
    public void setDatabaseVersion(Version version) {
		this.version = version;
    }
    
    /**
     * Get the database version as a comparable object
     * @return
     */
    protected Version getVersion() {
    	if (version == null) {
    		throw new IllegalStateException("version not set"); //$NON-NLS-1$
    	}
    	return this.version;
    }
    
	@TranslatorProperty(display="Use Bind Variables", description="Use prepared statements and bind variables",advanced=true)
	public boolean useBindVariables() {
		return this.useBindVariables;
	}

	public void setUseBindVariables(boolean useBindVariables) {
		this.useBindVariables = useBindVariables;
	}

	@TranslatorProperty(display="Database time zone", description="Time zone of the database, if different than Integration Server", advanced=true)
	public String getDatabaseTimeZone() {
		return this.databaseTimeZone;
	}

	public void setDatabaseTimeZone(String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
	}
	
	@TranslatorProperty(display="Trim string flag", description="Right Trim fixed character types returned as Strings - note that the native type must be char or nchar and the source must support the rtrim function.",advanced=true)
	public boolean isTrimStrings() {
		return this.trimStrings;
	}

	public void setTrimStrings(boolean trimStrings) {
		this.trimStrings = trimStrings;
	}

	@TranslatorProperty(display="Use informational comments in Source Queries", description="This will embed a /*comment*/ leading comment with session/request id in source SQL query for informational purposes", advanced=true)
	public boolean useCommentsInSourceQuery() {
		return this.useCommentsInSourceQuery;
	}

	public void setUseCommentsInSourceQuery(boolean useCommentsInSourceQuery) {
		this.useCommentsInSourceQuery = useCommentsInSourceQuery;
	}

	@Override
	public boolean isSourceRequired() {
		return true;
	}
	
	@Override
	public boolean isSourceRequiredForCapabilities() {
		return isSourceRequired() && this.version == null && usesDatabaseVersion();
	}
	
	protected boolean usesDatabaseVersion() {
		return false;
	}
	
	@Override
	public void initCapabilities(Connection connection)
			throws TranslatorException {
    	try {
			DatabaseMetaData metadata = connection.getMetaData();
    		supportsGeneratedKeys = metadata.supportsGetGeneratedKeys();
    		if (this.version == null) {
    			String fullVersion = metadata.getDatabaseProductVersion();
    			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Setting the database version to", fullVersion); //$NON-NLS-1$
    			setDatabaseVersion(fullVersion);
    		}
		} catch (SQLException e) {
			if (this.version == null) {
				throw new TranslatorException(e);
			}
		}
	}

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
    		throws TranslatorException {
    	return new JDBCQueryExecution(command, conn, executionContext, this);
    }
    
    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
    		throws TranslatorException {
    	return new JDBCDirectQueryExecution(arguments, command, conn, executionContext, this);
    }    
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
    		throws TranslatorException {
		return new JDBCProcedureExecution(command, conn, executionContext, this);
    }

    @Override
    public JDBCUpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
    		throws TranslatorException {
		return new JDBCUpdateExecution(command, conn, executionContext, this);
    }	
    
    @Override
    public Connection getConnection(DataSource ds)
    		throws TranslatorException {
		try {
	    	Connection c = ds.getConnection();
	    	obtainedConnection(c);
	    	return c;
		} catch (SQLException e) {
			 throw new TranslatorException(JDBCPlugin.Event.TEIID11009, e);
		}
    }
    
    @Override
    public void closeConnection(Connection connection, DataSource factory) {
    	if (connection == null) {
    		return;
    	}
    	try {
			connection.close();
		} catch (SQLException e) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Error closing"); //$NON-NLS-1$
		}
    }
    
	@Override
	public void getMetadata(MetadataFactory metadataFactory, Connection conn) throws TranslatorException {
		try {
			if (conn == null) {
				throw new TranslatorException(JDBCPlugin.Event.TEIID11018, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11018));
			}
			JDBCMetdataProcessor metadataProcessor = createMetadataProcessor();
			PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getModelProperties(), "importer"); //$NON-NLS-1$
			metadataProcessor.getConnectorMetadata(conn, metadataFactory);
		} catch (SQLException e) {
			 throw new TranslatorException(JDBCPlugin.Event.TEIID11010, e);
		}
	}
	
	/**
	 * @deprecated
	 * @see getMetadataProcessor
	 */
    @Deprecated
    protected JDBCMetdataProcessor createMetadataProcessor() {
		return (JDBCMetdataProcessor)getMetadataProcessor();
	}    
	
    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new JDBCMetdataProcessor();
    }
		
	@Override
    public List<String> getSupportedFunctions() {
        return getDefaultSupportedFunctions();
    }

	public List<String> getDefaultSupportedFunctions(){
		return Arrays.asList(new String[] { "+", "-", "*", "/" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
    @Override
    public boolean supportsGroupBy() {
    	return true;
    }

    @Override
    public boolean supportsAggregatesAvg() {
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
    public boolean supportsAggregatesDistinct() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return true;
    }

    @Override
    public boolean supportsAggregatesSum() {
        return true;
    }

    @Override
    public boolean supportsAliasedTable() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
        return true;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return true;
    }

    @Override
    public boolean supportsNotCriteria() {
        return true;
    }

    @Override
    public boolean supportsOrCriteria() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
    	return true;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return true;
    }

    @Override
    public boolean supportsScalarSubqueries() {
        return true;
    }

    @Override
    public boolean supportsSearchedCaseExpressions() {
        return true;
    }

    @Override
    public boolean supportsSelfJoins() {
        return true;
    }

    @Override
    public boolean supportsInlineViews() {
        return false;
    }       
    
    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return true;
    }
    
    @Override
    public boolean supportsSetQueryOrderBy() {
        return true;
    }
    
    @Override
    public boolean supportsUnions() {
        return true;
    }
    
    @Override
    public boolean supportsBulkUpdate() {
    	return true;
    }
    
    @Override
    public boolean supportsBatchedUpdates() {
    	return true;
    }
    
    @Override
    public boolean supportsCompareCriteriaOrdered() {
    	return true;
    }
    
    @Override
    public boolean supportsHaving() {
    	return true;
    }
    
    @Override
    public boolean supportsSelectExpression() {
    	return true;
    }
    
    @Override
    public boolean supportsInsertWithQueryExpression() {
    	return true;
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
    
    /**
     * Gets the database calendar.  This will be set to the time zone
     * specified by the property {@link JDBCPropertyNames#DATABASE_TIME_ZONE}, or
     * the local time zone if none is specified. 
     * @return the database calendar
     */
    public Calendar getDatabaseCalendar() {
    	return this.databaseCalender.get();
    }
    
    /**
     * Return a List of translated parts ({@link LanguageObject}s and Objects), or null
     * if to rely on the default translation.  Override with care.
     * @param command
     * @param context
     * @return list of translated parts
     */
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
		List<?> parts = null;
    	if (obj instanceof Function) {
    		Function function = (Function)obj;
    		if (functionModifiers != null) {
    			String name = function.getName();
    			while (true) {
	    			FunctionModifier modifier = functionModifiers.get(name);
	    			if (modifier != null) {
	    				parts = modifier.translate(function);
	    				break;
	    			}
	    			int index = name.indexOf('.');
	    			if (index < 0 || index == name.length() - 1) {
	    				break;
	    			}
    				name = name.substring(index + 1);
    			}
    		}
    	} else if (obj instanceof Command) {
    		parts = translateCommand((Command)obj, context);
    	} else if (obj instanceof Limit) {
    		parts = translateLimit((Limit)obj, context);
    	} else if (obj instanceof ColumnReference) {
    		ColumnReference elem = (ColumnReference)obj;
			if (isTrimStrings() && elem.getType() == TypeFacility.RUNTIME_TYPES.STRING && elem.getMetadataObject() != null 
					&& ("char".equalsIgnoreCase(elem.getMetadataObject().getNativeType()) || "nchar".equalsIgnoreCase(elem.getMetadataObject().getNativeType()))) { //$NON-NLS-1$ //$NON-NLS-2$
				return Arrays.asList(getLanguageFactory().createFunction(SourceSystemFunctions.RTRIM, new Expression[] {elem}, TypeFacility.RUNTIME_TYPES.STRING));
			}
    	} else if (obj instanceof DerivedColumn) {
            DerivedColumn dc = (DerivedColumn) obj;
            if (dc.isProjected()) {
	            Expression expr = dc.getExpression();
	            if (expr.getType() == TypeFacility.RUNTIME_TYPES.GEOMETRY) {
	                dc.setExpression(translateGeometrySelect(expr));
	            }
            }
        } else if (obj instanceof Literal) {
            Literal l = (Literal) obj;
            if (l.getType() == TypeFacility.RUNTIME_TYPES.GEOMETRY && l.getValue() != null) {
                return translateGeometryLiteral(l);
            }
        }
    	return parts;
    }
    
    /**
     * Translate GEOMETRY column reference into an expression that 
     * will return SRID & WKB.
     * 
     * @param expr
     * @return 
     */
    public Expression translateGeometrySelect(Expression expr) {
        return new Function(SourceSystemFunctions.ST_ASBINARY, Arrays.asList(expr), TypeFacility.RUNTIME_TYPES.BLOB);
    }
    
    /**
     * Translate GEOMETRY literal into an expression that will convert to database 
     * geometry type.
     * 
     * @param l
     * @return 
     */
    public List<?> translateGeometryLiteral(Literal l) {
        Literal srid = getLanguageFactory().createLiteral(
                ((GeometryType) l.getValue()).getSrid(),
                Integer.class
        );
        return Arrays.asList(getLanguageFactory().createFunction(
                SourceSystemFunctions.ST_GEOMFROMWKB, 
                new Expression[] { l, srid }, 
                TypeFacility.RUNTIME_TYPES.GEOMETRY)
        );
    }
    
    public Object retrieveGeometryValue(ResultSet results, int paramIndex) throws SQLException {
        GeometryType geom = null;
        Blob val = results.getBlob(paramIndex);
        if (val != null) {
            geom = new GeometryType(val);
        }
        return geom;
    }    
    
    public GeometryType retrieveGeometryValue(CallableStatement results, int parameterIndex) throws SQLException {
    	throw new SQLException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11022));
	}
    
    /**
     * Return a List of translated parts ({@link LanguageObject}s and Objects), or null
     * if to rely on the default translation. 
     * @param command
     * @param context
     * @return a list of translated parts
     */
    public List<?> translateCommand(Command command, ExecutionContext context) {
    	return null;
    }

    /**
     * Return a List of translated parts ({@link LanguageObject}s and Objects), or null
     * if to rely on the default translation. 
     * @param limit
     * @param context
     * @return a list of translated parts
     */
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
    	return null;
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
    
    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal boolean value.  By default, a boolean literal is represented as:
     * <code>'0'</code> or <code>'1'</code>.
     * @param booleanValue Boolean value, never null
     * @return Translated string
     */
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "1"; //$NON-NLS-1$
        }
        return "0"; //$NON-NLS-1$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal date value.  By default, a date literal is represented as:
     * <code>{d '2002-12-31'}</code>
     * @param dateValue Date value, never null
     * @return Translated string
     */
    public String translateLiteralDate(java.sql.Date dateValue) {
        return "{d '" + formatDateValue(dateValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal time value.  By default, a time literal is represented as:
     * <code>{t '23:59:59'}</code>
     * 
     * See {@link JDBCExecutionFactory#hasTimeType()} to represent literal times as timestamps.
     * 
     * @param timeValue Time value, never null
     * @return Translated string
     */
    public String translateLiteralTime(Time timeValue) {
        return "{t '" + formatDateValue(timeValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal timestamp value.  By default, a timestamp literal is
     * represented as: <code>{ts '2002-12-31 23:59:59'}</code>.
     * 
     * See {@link JDBCExecutionFactory#getTimestampNanoPrecision()} to control the literal 
     * precision. 
     * 
     * @param timestampValue Timestamp value, never null
     * @return Translated string
     */
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "{ts '" + formatDateValue(timestampValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Format the dateObject (of type date, time, or timestamp) into a string
     * optionally using the DatabaseTimeZone.
     * @param dateObject
     * @return Formatted string
     */
    public String formatDateValue(java.util.Date dateObject, boolean useTimezone) {
    	if (dateObject instanceof Timestamp && getTimestampNanoPrecision() < 9) {
        	Timestamp ts = (Timestamp)dateObject;
        	Timestamp newTs = new Timestamp(ts.getTime());
        	if (getTimestampNanoPrecision() > 0) {
	        	int mask = (int)Math.pow(10, 9-getTimestampNanoPrecision());
	        	newTs.setNanos(ts.getNanos()/mask*mask);
        	} else {
        		newTs.setNanos(0);
        	}
        	dateObject = newTs;
        }
    	if (!useTimezone) {
    		return dateObject.toString();
    	}
        return getTypeFacility().convertDate(dateObject, getDatabaseCalendar().getTimeZone(), TimestampWithTimezone.getCalendar(), dateObject.getClass()).toString();	
    }
    
    /**
     * Format the dateObject (of type date, time, or timestamp) into a string
     * using the DatabaseTimeZone.
     * @param dateObject
     * @return Formatted string
     */
    public String formatDateValue(java.util.Date dateObject) {
    	return formatDateValue(dateObject, true);
    }    
    
    /**
     * Returns true to indicate that SQL should include a comment
     * indicating the session and request ids.
     */
    public boolean addSourceComment() {
        return useCommentsInSourceQuery();
    }   
    
    /**
     * Indicates whether group alias should be of the form
     * "...FROM groupA AS X" or "...FROM groupA X".  Certain
     * data sources (such as Oracle) may not support the first
     * form. 
     * @return boolean
     */
    public boolean useAsInGroupAlias(){
        return true;
    }
    
    /**
     * Use PreparedStatements (or CallableStatements) as
     * appropriate for all commands.  Bind values will be 
     * determined by the {@link BindValueVisitor}.  {@link Literal#setBindValue(boolean)}
     * can be used to force a literal to be a bind value.  
     */
    public boolean usePreparedStatements() {
    	return useBindVariables();
    }
    
    /**
     * Set to true to indicate that every branch of a set query
     * should have parenthesis, i.e. (query) union (query)
     * @return true if parenthesis should be used for each set branch
     */
    public boolean useParensForSetQueries() {
    	return false;
    }
    
    /**
     * Return false to indicate that time support should be emulated 
     * with timestamps.
     * @return true if database has a time type
     */
    public boolean hasTimeType() {
    	return true;
    }
    
    /**
     * Returns the name for a given {@link Operation}
     * @param operation
     * @return the name for the set operation
     */
    public String getSetOperationString(SetQuery.Operation operation) {
    	return operation.toString();
    }
    
    /**
     * Returns the source comment for the given command
     * @param context
     * @param command
     * @return the comment
     */
    public String getSourceComment(ExecutionContext context, Command command) {
	    if (addSourceComment() && context != null) {
            return comment.get().format(new Object[] {context.getConnectionId(), context.getRequestId(), context.getPartIdentifier(), 
            		context.getExecutionCountIdentifier(), context.getSession().getUserName(), context.getVdbName(), context.getVdbVersion(), context.isTransactional() });
	    }
	    return ""; //$NON-NLS-1$ 
    }
    
    /**
     * Override to return a name other than the default [group.]element
     * @param group
     * @param element
     * @return thre replacement name
     */
    public String replaceElementName(String group, String element) {
    	return null;
    }
    
    /**
     * Return the precision of timestamp literals.  Defaults to 9.
     * @return digits of timestamp nano precision.
     */
    public int getTimestampNanoPrecision() {
    	return 9;
    }
    
    /**
     * This is a generic implementation. Because different databases handle
     * stored procedures differently, subclasses should override this method
     * if necessary.
     */
    public ResultSet executeStoredProcedure(CallableStatement statement, List<Argument> preparedValues, Class<?> returnType) throws SQLException {
        int index = 1;
        
        if(returnType != null){
            registerSpecificTypeOfOutParameter(statement, returnType, index++);
        }
        
        Iterator<?> iter = preparedValues.iterator();
        while(iter.hasNext()){
            Argument param = (Argument)iter.next();
                    
            if(param.getDirection() == Direction.INOUT){
                registerSpecificTypeOfOutParameter(statement,param.getType(), index);
            }else if(param.getDirection() == Direction.OUT){
                registerSpecificTypeOfOutParameter(statement,param.getType(), index++);
            }
                    
            if(param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT){
                bindValue(statement, param.getArgumentValue().getValue(), param.getType(), index++);
            }
        }
        boolean resultSetNext = statement.execute();
        
        while (!resultSetNext) {
            int update_count = statement.getUpdateCount();
            if (update_count == -1) {
                break;
            }            
            resultSetNext = statement.getMoreResults();
        }
        return statement.getResultSet();
    }

    /**
     * For registering specific output parameter types we need to translate these into the appropriate
     * java.sql.Types output parameters
     * We will need to match these up with the appropriate standard sql types
     * @param cstmt
     * @param parameter
     * @throws SQLException
     */
    protected void registerSpecificTypeOfOutParameter(CallableStatement statement, Class<?> runtimeType, int index) throws SQLException {
        int typeToSet = TypeFacility.getSQLTypeFromRuntimeType(runtimeType);
        
        statement.registerOutParameter(index,typeToSet);
    }
    
    /**
     * Sets prepared statement parameter i with param.
     * 
     * Performs special handling to translate dates using the database time zone and to
     * translate biginteger, float, and char to JDBC safe objects.
     *  
     * @param stmt
     * @param param
     * @param paramType
     * @param i
     * @param cal
     * @throws SQLException
     */
    public void bindValue(PreparedStatement stmt, Object param, Class<?> paramType, int i) throws SQLException {
        int type = TypeFacility.getSQLTypeFromRuntimeType(paramType);
                
        if (param == null) {
        	if (type == Types.JAVA_OBJECT) {
        		stmt.setNull(i, Types.OTHER);
        	} else {
        		stmt.setNull(i, type);
        	}
            return;
        } 
        //if this is a Date object, then use the database calendar
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
            stmt.setDate(i,(java.sql.Date)param, getDatabaseCalendar());
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            stmt.setTime(i,(java.sql.Time)param, getDatabaseCalendar());
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            stmt.setTimestamp(i,(java.sql.Timestamp)param, getDatabaseCalendar());
            return;
        }
        //not all drivers handle the setObject call with BigDecimal correctly (namely jConnect 6.05)
        if (TypeFacility.RUNTIME_TYPES.BIG_DECIMAL.equals(paramType)) {
        	stmt.setBigDecimal(i, (BigDecimal)param);
            return;
        }
        
        if (useStreamsForLobs()) {
        	if (param instanceof Blob) {
        		Blob blob = (Blob)param;
        		stmt.setBinaryStream(i, blob.getBinaryStream(), blob.length());
        		return;
        	}
        	if (param instanceof Clob) {
        		Clob clob = (Clob)param;
        		stmt.setCharacterStream(i, clob.getCharacterStream(), clob.length());
        		return;
        	}
        }
        //convert these the following to jdbc safe values
        if (TypeFacility.RUNTIME_TYPES.BIG_INTEGER.equals(paramType)) {
            param = new BigDecimal((BigInteger)param);
        } else if (TypeFacility.RUNTIME_TYPES.FLOAT.equals(paramType)) {
            param = new Double(((Float)param).doubleValue());
        } else if (TypeFacility.RUNTIME_TYPES.CHAR.equals(paramType)) {
            param = ((Character)param).toString();
        } else if (paramType.equals(TypeFacility.RUNTIME_TYPES.VARBINARY)) {
        	param = ((BinaryType)param).getBytesDirect();
        }
        
        if (type != Types.JAVA_OBJECT) {
        	stmt.setObject(i, param, type);
        } else {
        	stmt.setObject(i, param);
        }
    }
    
    /**
     * If streams should be used for Blob/Clob sets on {@link PreparedStatement}s
     * @return
     */
    public boolean useStreamsForLobs() {
    	return false;
    }
    
	/**
	 * Retrieve the value on the current resultset row for the given column index.
	 * @param results
	 * @param columnIndex
	 * @param expectedType
	 * @return the value
	 * @throws SQLException
	 */
    public Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException {
        Integer code = DataTypeManager.getTypeCode(expectedType);
        if(code != null) {
            // Calling the specific methods here is more likely to get uniform (and fast) results from different
            // data sources as the driver likely knows the best and fastest way to convert from the underlying
            // raw form of the data to the expected type.  We use a switch with codes in order without gaps
            // as there is a special bytecode instruction that treats this case as a map such that not every value 
            // needs to be tested, which means it is very fast.
            switch(code.intValue()) {
                case DataTypeManager.DefaultTypeCodes.INTEGER:  {
                    int value = results.getInt(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }
                    return Integer.valueOf(value);
                }
                case DataTypeManager.DefaultTypeCodes.LONG:  {
                    long value = results.getLong(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Long.valueOf(value);
                }                
                case DataTypeManager.DefaultTypeCodes.DOUBLE:  {
                    double value = results.getDouble(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Double.valueOf(value);
                }                
                case DataTypeManager.DefaultTypeCodes.BIGDECIMAL:  {
                    return results.getBigDecimal(columnIndex); 
                }
                case DataTypeManager.DefaultTypeCodes.SHORT:  {
                    short value = results.getShort(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }                    
                    return Short.valueOf(value);
                }
                case DataTypeManager.DefaultTypeCodes.FLOAT:  {
                    float value = results.getFloat(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Float.valueOf(value);
                }
                case DataTypeManager.DefaultTypeCodes.TIME: {
            		return results.getTime(columnIndex, getDatabaseCalendar());
                }
                case DataTypeManager.DefaultTypeCodes.DATE: {
            		return results.getDate(columnIndex, getDatabaseCalendar());
                }
                case DataTypeManager.DefaultTypeCodes.TIMESTAMP: {
            		return results.getTimestamp(columnIndex, getDatabaseCalendar());
                }
    			case DataTypeManager.DefaultTypeCodes.BLOB: {
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
    			case DataTypeManager.DefaultTypeCodes.GEOMETRY: {
                    return retrieveGeometryValue(results, columnIndex);
    			}
    			case DataTypeManager.DefaultTypeCodes.CLOB: {
    				try {
    					return results.getClob(columnIndex);
    				} catch (SQLException e) {
    					// ignore
    				}
    				break;
    			}  
    			case DataTypeManager.DefaultTypeCodes.BOOLEAN: {
    				boolean result = results.getBoolean(columnIndex);
    				if(results.wasNull()) {
                        return null;
                    } 
    				return result;
    			}
            }
        }

        Object result = results.getObject(columnIndex);
        if (expectedType == TypeFacility.RUNTIME_TYPES.OBJECT) {
        	return convertObject(result);
        }
		return result;
    }

    /**
     * Retrieve the value for the given parameter index
     * @param results
     * @param parameterIndex
     * @param expectedType
     * @return the value
     * @throws SQLException
     */
    public Object retrieveValue(CallableStatement results, int parameterIndex, Class<?> expectedType) throws SQLException{
        Integer code = DataTypeManager.getTypeCode(expectedType);
        if(code != null) {
            switch(code.intValue()) {
                case DataTypeManager.DefaultTypeCodes.INTEGER:  {
                    int value = results.getInt(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }
                    return Integer.valueOf(value);
                }
                case DataTypeManager.DefaultTypeCodes.LONG:  {
                    long value = results.getLong(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Long.valueOf(value);
                }                
                case DataTypeManager.DefaultTypeCodes.DOUBLE:  {
                    double value = results.getDouble(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Double(value);
                }                
                case DataTypeManager.DefaultTypeCodes.BIGDECIMAL:  {
                    return results.getBigDecimal(parameterIndex); 
                }
                case DataTypeManager.DefaultTypeCodes.SHORT:  {
                    short value = results.getShort(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }                    
                    return Short.valueOf(value);
                }
                case DataTypeManager.DefaultTypeCodes.FLOAT:  {
                    float value = results.getFloat(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Float(value);
                }
                case DataTypeManager.DefaultTypeCodes.TIME: {
            		return results.getTime(parameterIndex, getDatabaseCalendar());
                }
                case DataTypeManager.DefaultTypeCodes.DATE: {
            		return results.getDate(parameterIndex, getDatabaseCalendar());
                }
                case DataTypeManager.DefaultTypeCodes.TIMESTAMP: {
            		return results.getTimestamp(parameterIndex, getDatabaseCalendar());
                }
    			case DataTypeManager.DefaultTypeCodes.BLOB: {
    				try {
    					return results.getBlob(parameterIndex);
    				} catch (SQLException e) {
    					// ignore
    				}
    				try {
    					return results.getBytes(parameterIndex);
    				} catch (SQLException e) {
    					// ignore
    				}
    			}
    			case DataTypeManager.DefaultTypeCodes.GEOMETRY: {
    				return retrieveGeometryValue(results, parameterIndex);
    			}
    			case DataTypeManager.DefaultTypeCodes.CLOB: {
    				try {
    					return results.getClob(parameterIndex);
    				} catch (SQLException e) {
    					// ignore
    				}
    			}
    			case DataTypeManager.DefaultTypeCodes.BOOLEAN: {
    				boolean result = results.getBoolean(parameterIndex);
    				if(results.wasNull()) {
                        return null;
                    } 
    				return result;
    			}
            }
        }

        // otherwise fall through and call getObject() and rely on the normal
		// translation routines
        Object result = results.getObject(parameterIndex);
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

	/**
     * Called exactly once for this source.
     * @param connection
     */
    protected void afterInitialConnectionObtained(Connection connection) {
        // now dig some details about this driver/database for log.
        try {
            StringBuffer sb = new StringBuffer(getClass().getSimpleName());
            DatabaseMetaData dbmd = connection.getMetaData();
            sb.append(" Commit=").append(connection.getAutoCommit()); //$NON-NLS-1$
            sb.append(";DatabaseProductName=").append(dbmd.getDatabaseProductName()); //$NON-NLS-1$
            sb.append(";DatabaseProductVersion=").append(dbmd.getDatabaseProductVersion()); //$NON-NLS-1$
            sb.append(";DriverMajorVersion=").append(dbmd.getDriverMajorVersion()); //$NON-NLS-1$
            sb.append(";DriverMajorVersion=").append(dbmd.getDriverMinorVersion()); //$NON-NLS-1$
            sb.append(";DriverName=").append(dbmd.getDriverName()); //$NON-NLS-1$
            sb.append(";DriverVersion=").append(dbmd.getDriverVersion()); //$NON-NLS-1$
            sb.append(";IsolationLevel=").append(dbmd.getDefaultTransactionIsolation()); //$NON-NLS-1$
            LogManager.logInfo(LogConstants.CTX_CONNECTOR, sb.toString());
        } catch (SQLException e) {
            LogManager.logInfo(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11002)); 
        }
    }
    
    /**
     * Provides a hook to call source specific logic when 
     * a connection is obtained.
     * 
     * defect request 13979 & 13978
     */
    public void obtainedConnection(Connection connection) {
        if (initialConnection.compareAndSet(true, false)) {
            afterInitialConnectionObtained(connection);
        }
    }
    
    /**
     * Create the {@link SQLConversionVisitor} that will perform translation.  Typical custom
     * JDBC connectors will not need to create custom conversion visitors, rather implementors 
     * should override existing {@link JDBCExecutionFactory} methods.
     * @return the {@link SQLConversionVisitor}
     */
    public SQLConversionVisitor getSQLConversionVisitor() {
    	return new SQLConversionVisitor(this);
    }
    
    /**
     * Set to true to indicate that every branch of a join
     * should have parenthesis.
     * @return true if every branch of a join should use parenthesis
     */
    public boolean useParensForJoins() {
    	return false;
    }
    
    @Override
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.LOW;
    }
    
    /**
     * Returns whether the limit clause is applied to the select clause.
     * @return true if the limit clause is part of the select
     */
    public boolean useSelectLimit() {
    	return false;
    }
    
	public static List<String> parseName(String tableName, char escape, char delim) {
		boolean quoted = false;
		boolean escaped = false;
        List<String> nameParts = new LinkedList<String>();
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < tableName.length(); i++) {
			char c = tableName.charAt(i);
			if (quoted) {
				if (c == escape) {
					if (escaped) {
						current.append(c);
					}
					escaped = !escaped;
				} else if (c == delim) {
					if (escaped) {
						escaped = false;
						quoted = false;
						nameParts.add(current.toString());
						current = new StringBuilder();
					} else {
						current.append(c);
					}
				} else {
					current.append(c);
				}
			} else {
				if (c == escape) {
					if (current.length() == 0) {
						quoted = true;
					} else {
						current.append(c);
					}
				} else if (c == delim) {
					quoted = false;
					nameParts.add(current.toString());
					current = new StringBuilder();
				} else {
					current.append(c);
				}
			}
		}
        if (current.length() > 0) {
        	nameParts.add(current.toString());
        }
        return nameParts;
	}

	/**
	 * Get the predicate name for LIKE_REGEX
	 * @return 
	 */
	public String getLikeRegexString() {
		return SQLConstants.Reserved.LIKE_REGEX;
	}

	/**
	 * Set the fetch size on the given statement.
	 * @param context 
	 * @param command 
	 * @param statement
	 * @param fetchSize
	 * @throws SQLException
	 */
	public void setFetchSize(Command command, ExecutionContext context, Statement statement, int fetchSize) throws SQLException {
		statement.setFetchSize(fetchSize);
	}
	
	public boolean supportsGeneratedKeys() {
		return supportsGeneratedKeys;
	}
	
	@TranslatorProperty(display="Struct retrieval", description="Struct retrieval mode (OBJECT, COPY, ARRAY)",advanced=true)
	public StructRetrieval getStructRetrieval() {
		return structRetrieval;
	}
	
	public void setStructRetrieval(StructRetrieval structRetrieval) {
		this.structRetrieval = structRetrieval;
	}

	/**
	 * 
	 * @param context
	 * @param command
	 * @return true if generated keys can be returned
	 */
	protected boolean supportsGeneratedKeys(ExecutionContext context,
			Command command) {
		return supportsGeneratedKeys() && command instanceof Insert;
	}

    /**
     * Create a temp table with the given name prefix and columns
     * @param prefix
	 * @param cols
	 * @param context
	 * @param connection
	 * @return the name of the table created
	 * @throws SQLException 
	 */
	public String createTempTable(String prefix, List<ColumnReference> cols, ExecutionContext context, Connection connection) throws SQLException {
    	String name = getTemporaryTableName(prefix);
		String sql = getCreateTempTableSQL(name, cols, !connection.getAutoCommit());
    	Statement s = connection.createStatement();
    	try {
    		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "creating temporary table with:", sql); //$NON-NLS-1$ 
    		s.execute(sql);
    	} finally {
    		try {
    			s.close();
    		} catch (SQLException e) {
    			
    		}
    	}
    	return name;
	}

	public String getCreateTempTableSQL(String name, List<ColumnReference> cols, boolean transactional) {
		SQLDialect d = getDialect();
    	StringBuilder sb = new StringBuilder(getCreateTemporaryTableString(transactional)).append(" "); //$NON-NLS-1$
    	sb.append(name).append(" ("); //$NON-NLS-1$
    	for (Iterator<ColumnReference> iter = cols.iterator(); iter.hasNext();) {
    		ColumnReference col = iter.next();
    		sb.append(col.getName());
    		sb.append(" "); //$NON-NLS-1$
    		Integer defaultValue = JDBCSQLTypeInfo.getDefaultPrecision(col.getType()); 
    		int precision = defaultValue == null?255:defaultValue;
    		int scale = col.getType() == TypeFacility.RUNTIME_TYPES.BIG_DECIMAL?2:0;
    		long length = precision;
    		if (col.getMetadataObject() != null) {
    			precision = col.getMetadataObject().getPrecision();
    			scale = col.getMetadataObject().getScale();
    			length = col.getMetadataObject().getLength();
    		}
    		sb.append(d.getTypeName(TypeFacility.getSQLTypeFromRuntimeType(col.getType()), length, precision, scale));
    		//sb.append(" NOT NULL"); -- needed if we will add an index
    		if (iter.hasNext()) {
    			sb.append(", "); //$NON-NLS-1$
    		}
    	}
    	sb.append(") ").append(getCreateTemporaryTablePostfix(transactional)); //$NON-NLS-1$
    	String sql = sb.toString();
		return sql;
	}

	/**
	 * 
	 * @param prefix
	 * @return a valid temporary table name
	 */
	public String getTemporaryTableName(String prefix) {
		return prefix;
	}
	
	/**
	 * 
	 * @param inTransaction
	 * @return the post script for the temp table create
	 */
	public String getCreateTemporaryTablePostfix(boolean inTransaction) {
		return getDialect().getCreateTemporaryTablePostfix();
	}
	
	/**
	 * 
	 * @param inTransaction
	 * @return the temp table creation ddl
	 */
	public String getCreateTemporaryTableString(boolean inTransaction) {
		return getDialect().getCreateTemporaryTableString();
	}
	
	public SQLDialect getDialect() {
		if (dialect == null) {
			String name = getHibernateDialectClassName();
			if (name != null) {
				try {
					Object impl = ReflectionHelper.create(name, null, this.getClass().getClassLoader());
					InvocationHandler handler = new MixinProxy(new Object[] {impl});
					this.dialect = (SQLDialect) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[]{SQLDialect.class}, handler);
				} catch (TeiidException e) {
					LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, name, "could not be loaded"); //$NON-NLS-1$
				}
			}
			if (dialect == null) {
				dialect = new DefaultSQLDialect();
			}
		}
		return dialect;
	}
	
	public String getHibernateDialectClassName() {
		return null;
	}
	
	@Override
	public boolean supportsDependentJoins() {
		return enableDependentJoins && getDialect().supportsTemporaryTables();
	}
	
	@Override
	public boolean supportsFullDependentJoins() {
		return this.supportsDependentJoins();
	}
	
	public boolean tempTableRequiresTransaction() {
		return false;
	}

	/**
	 * Called after the temporary table has been loaded
	 * @param tableName
	 * @param context
	 * @param connection
	 * @throws SQLException 
	 */
	public void loadedTemporaryTable(String tableName,
			ExecutionContext context, Connection connection) throws SQLException {
		
	}
	
	@TranslatorProperty(display="Enable Dependent Joins", description="Enable Dependent Join Pushdown",advanced=true)
	public boolean isEnableDependentJoins() {
		return enableDependentJoins;
	}

	public void setEnableDependentJoins(boolean enableDependentJoins) {
		this.enableDependentJoins = enableDependentJoins;
	}

	/**
	 * @return true if the rollup syntax is WITH ROLLUP
	 */
	public boolean useWithRollup() {
		return false;
	}
	
	@TranslatorProperty(display="Comment Format", description= "Comment format string used with useCommentsInSourceQuery")
	public String getCommentFormat() {
		return commentFormat;
	}
	
	public void setCommentFormat(String commentFormat) {
		this.commentFormat = commentFormat;
	}

	/**
	 * @return true if scientific notation should be used for float/double types
	 */
	public boolean useScientificNotation() {
		return false;
	}

	/**
	 * @return true if the N prefix should be used for strings containing non-ascii characters
	 */
	public boolean useUnicodePrefix() {
		return false;
	}

	
	/**
	 * Implemented if the {@link Connection} needs initialized after a statement cancel
	 * @param c
	 * @throws SQLException 
	 */
	public void intializeConnectionAfterCancel(Connection c) throws SQLException {
	}

	@Override
	@TranslatorProperty(display="Use Bindings For Dependent Join", description= "If PreparedStatement bindings should be used for dependent join values.")
	public boolean useBindingsForDependentJoin() {
		return useBindingsForDependentJoin;
	}
	
	public void setUseBindingsForDependentJoin(
			boolean useBindingsForDependentJoin) {
		this.useBindingsForDependentJoin = useBindingsForDependentJoin;
	}
	
}
