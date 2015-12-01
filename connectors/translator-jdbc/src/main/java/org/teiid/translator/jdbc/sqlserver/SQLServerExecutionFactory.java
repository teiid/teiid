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
package org.teiid.translator.jdbc.sqlserver;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.teiid.core.util.StringUtil;
import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Function;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.QueryExpression;
import org.teiid.language.With;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;
import org.teiid.translator.jdbc.Version;
import org.teiid.translator.jdbc.sybase.SybaseExecutionFactory;

/**
 * Updated to assume the use of the DataDirect, 2005 driver, or later.
 */
@Translator(name="sqlserver", description="A translator for Microsoft SQL Server Database")
public class SQLServerExecutionFactory extends SybaseExecutionFactory {
	
	public static final String V_2000 = "2000"; //$NON-NLS-1$
	public static final String V_2005 = "2005"; //$NON-NLS-1$
	public static final String V_2008 = "2008"; //$NON-NLS-1$
	public static final String V_2012 = "2012"; //$NON-NLS-1$
	
	public static final Version SEVEN_0 = Version.getVersion("7.0"); //$NON-NLS-1$
	public static final Version NINE_0 = Version.getVersion("9.0"); //$NON-NLS-1$
	public static final Version TEN_0 = Version.getVersion("10.0"); //$NON-NLS-1$
	public static final Version ELEVEN_0 = Version.getVersion("11.0"); //$NON-NLS-1$
	
	
	//TEIID-31 remove mod modifier for SQL Server 2008
	public SQLServerExecutionFactory() {
		setMaxInCriteriaSize(JDBCExecutionFactory.DEFAULT_MAX_IN_CRITERIA);
		setMaxDependentInPredicates(2);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		registerFunctionModifier(SourceSystemFunctions.WEEK, new FunctionModifier() {
			
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("DATEPART(ISO_WEEK, ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
		registerFunctionModifier(SourceSystemFunctions.LOCATE, new AliasModifier("CHARINDEX")); //$NON-NLS-1$
	}

	@Override
	protected void populateDateFormats() {
		formatMap.put("MM/dd/yy", 1); //$NON-NLS-1$
		formatMap.put("yy.MM.dd", 2); //$NON-NLS-1$
		formatMap.put("dd/MM/yy", 3); //$NON-NLS-1$
		formatMap.put("dd.MM.yy", 4); //$NON-NLS-1$
		formatMap.put("dd-MM-yy", 5); //$NON-NLS-1$
		formatMap.put("dd MMM yy", 6); //$NON-NLS-1$
		formatMap.put("MMM dd, yy", 7); //$NON-NLS-1$
		formatMap.put("MM-dd-yy", 10); //$NON-NLS-1$
		formatMap.put("yy/MM/dd", 11); //$NON-NLS-1$
		formatMap.put("yyMMdd", 12); //$NON-NLS-1$
		for (Map.Entry<String, Integer> entry : new HashSet<Map.Entry<String, Integer>>(formatMap.entrySet())) {
			formatMap.put(entry.getKey().replace("yy", "yyyy"), entry.getValue() + 100); //$NON-NLS-1$ //$NON-NLS-2$
		}

		formatMap.put("MMM d yyyy hh:mma", 100); //$NON-NLS-1$
		formatMap.put("HH:mm:ss", 8); //$NON-NLS-1$
		formatMap.put("MMM d yyyy hh:mm:ss:SSSa", 109); //$NON-NLS-1$
		formatMap.put("dd MMM yyyy HH:mm:ss:SSS", 113); //$NON-NLS-1$
		formatMap.put("kk:MM:ss:SSS", 14); //$NON-NLS-1$
		formatMap.put("yyyy-MM-dd HH:mm:ss", 120); //$NON-NLS-1$
		formatMap.put("yyyy-MM-dd HH:mm:ss.SSS", 121); //$NON-NLS-1$
		formatMap.put("yyyy-MM-dd'T'HH:mm:ss.SSS", 126); //$NON-NLS-1$
		//formatMap.put("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", 127); //$NON-NLS-1$
	}
	
	@Override
	protected List<Object> convertDateToString(Function function) {
		return Arrays.asList("replace(convert(varchar, ", function.getParameters().get(0), ", 102), '.', '-')"); //$NON-NLS-1$ //$NON-NLS-2$
	}
    
	@Override
	protected List<?> convertTimestampToString(Function function) {
		return Arrays.asList("convert(varchar, ", function.getParameters().get(0), ", 21)"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
    	if (obj instanceof ColumnReference) {
    		ColumnReference elem = (ColumnReference)obj;
			if (TypeFacility.RUNTIME_TYPES.STRING.equals(elem.getType()) && elem.getMetadataObject() != null && "uniqueidentifier".equalsIgnoreCase(elem.getMetadataObject().getNativeType())) { //$NON-NLS-1$
				return Arrays.asList("cast(", elem, " as char(36))"); //$NON-NLS-1$ //$NON-NLS-2$
			}
    	} else if (obj instanceof AggregateFunction) {
    		AggregateFunction af = (AggregateFunction)obj;
    		if (af.getName().equals(AggregateFunction.STDDEV_POP)) {
    			af.setName("STDDEVP"); //$NON-NLS-1$
    		} else if (af.getName().equals(AggregateFunction.STDDEV_SAMP)) {
    			af.setName("STDDEV"); //$NON-NLS-1$
    		} else if (af.getName().equals(AggregateFunction.VAR_POP)) {
    			af.setName("VARP"); //$NON-NLS-1$
    		} else if (af.getName().equals(AggregateFunction.VAR_SAMP)) {
    			af.setName("VAR"); //$NON-NLS-1$
    		}
    	}
    	return super.translate(obj, context);
    }
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getDefaultSupportedFunctions());
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("PI"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        //supportedFunctons.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        //supportedFunctons.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        //supportedFunctions.add("RAND"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        //supportedFunctons.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SPACE"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        //supportedFunctons.add("TRANSLATE"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        //supportedFunctons.add("CURDATE"); //$NON-NLS-1$
        //supportedFunctons.add("CURTIME"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        //supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL");      //$NON-NLS-1$ 
        supportedFunctions.add(SourceSystemFunctions.FORMATTIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        return supportedFunctions;
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
    public boolean supportsIntersect() {
    	return true;
    }
    @Override
    public boolean supportsExcept() {
    	return true;
    };
    
    @Override
    public int getMaxFromGroups() {
        return DEFAULT_MAX_FROM_GROUPS;
    } 
    
    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
    	return true;
    }
     
    @Override
    public boolean nullPlusNonNullIsNull() {
    	return true;
    }
    
    @Override
    public boolean booleanNullable() {
    	return true;
    }
    
    /**
     * Overridden to allow for year based versions
     */
    @Override
    public void setDatabaseVersion(String version) {
    	if (version != null) {
    		if (version.equals(V_2000)) {
    			setDatabaseVersion(SEVEN_0);
    			return;
    		} else if (version.equals(V_2005)) {
    			setDatabaseVersion(NINE_0);
    			return;
    		} else if (version.equals(V_2008)) {
    			setDatabaseVersion(TEN_0);
    			return;
    		} else if (version.equals(V_2012)) {
    			setDatabaseVersion(ELEVEN_0);
    			return;
    		}
    	}
    	super.setDatabaseVersion(version);
    }
    
    @Override
    public String translateLiteralDate(Date dateValue) {
    	if (getVersion().compareTo(TEN_0) >= 0) {
    		return super.translateLiteralDate(dateValue);
    	}
    	return super.translateLiteralTimestamp(new Timestamp(dateValue.getTime()));
    }
    
    @Override
    public boolean hasTimeType() {
    	return getVersion().compareTo(TEN_0) >= 0;
    }
    
    /**
     * The SQL Server driver maps the time escape to a timestamp/datetime, so
     * use a cast of the string literal instead.
     */
    @Override
    public String translateLiteralTime(Time timeValue) {
    	return "cast('" +  formatDateValue(timeValue) + "' as time)"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Override
    public boolean supportsCommonTableExpressions() {
    	return true;
    }
    
    @Override
    public boolean supportsSubqueryCommonTableExpressions() {
    	return false;
    }
    
    @Override
    public boolean supportsRecursiveCommonTableExpressions() {
    	return getVersion().compareTo(TEN_0) >= 0;
    }
    
    @Override
    protected boolean supportsCrossJoin() {
    	return true;
    }
    
    @Override
    public boolean supportsElementaryOlapOperations() {
    	return true;
    }
    
    @Override
    public boolean supportsWindowDistinctAggregates() {
    	return false;
    }
    
    @Override
    public boolean supportsWindowOrderByWithAggregates() {
    	return false;
    }
    
    @Override
    public boolean supportsFormatLiteral(String literal,
    		org.teiid.translator.ExecutionFactory.Format format) {
    	if (format == Format.NUMBER) {
    		return false; //TODO: add support
    	}
    	return formatMap.containsKey(literal);
    }
    
    @Override
    public boolean supportsOnlyFormatLiterals() {
    	return true;
    }
    
    @Override
    protected boolean setFetchSize() {
    	return true;
    }
    
    @Override
    @Deprecated
    protected JDBCMetdataProcessor createMetadataProcessor() {
        return (JDBCMetdataProcessor)getMetadataProcessor();
    }
    
    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new JDBCMetdataProcessor() {
            @Override
            protected Column addColumn(ResultSet columns, Table table,
                    MetadataFactory metadataFactory, int rsColumns)
                    throws SQLException {
                Column c = super.addColumn(columns, table, metadataFactory, rsColumns);
                //The ms jdbc driver does not correctly report the auto incremented column
                if (!c.isAutoIncremented() && c.getNativeType() != null && StringUtil.endsWithIgnoreCase(c.getNativeType(), " identity")) { //$NON-NLS-1$
                    c.setAutoIncremented(true);
                }
                return c;
            }
        };
    }
    
    
	@Override
	protected boolean usesDatabaseVersion() {
		return true;
	}
	
	@Override
	public boolean useStreamsForLobs() {
		return true;
	}
	
    @Override
    public boolean supportsSelectWithoutFrom() {
    	return true;
    }
    
    @Override
    public String getHibernateDialectClassName() {
    	if (getVersion().compareTo(NINE_0) >= 0) {
    		if (getVersion().compareTo(TEN_0) >= 0) {
    			return "org.hibernate.dialect.SQLServer2008Dialect"; //$NON-NLS-1$
    		}
    		return "org.hibernate.dialect.SQLServer2005Dialect"; //$NON-NLS-1$
    	}
    	return "org.hibernate.dialect.SQLServerDialect"; //$NON-NLS-1$
    }
    
    @Override
    public boolean supportsGroupByRollup() {
    	return getVersion().compareTo(NINE_0) >= 0;
    }
    
    @Override
    public boolean useWithRollup() {
    	return getVersion().compareTo(TEN_0) < 0;
    }
    
    @Override
    public boolean supportsConvert(int fromType, int toType) {
    	if (fromType == TypeFacility.RUNTIME_CODES.OBJECT && this.convertModifier.hasTypeMapping(toType)) {
			return true;
    	}
    	return super.supportsConvert(fromType, toType);
    }
    
    @Override
    public boolean supportsLiteralOnlyWithGrouping() {
    	return true;
    }
    
    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
    	if (command instanceof Insert) {
    		Insert insert = (Insert)command;
    		if (insert.getValueSource() instanceof QueryExpression) {
    			QueryExpression qe = (QueryExpression)insert.getValueSource();
    			if (qe.getWith() != null) {
    				With with = qe.getWith();
    				qe.setWith(null);
    				return Arrays.asList(with, insert);
    			}
    		}
    	}
    	return super.translateCommand(command, context);
    }
    
}
