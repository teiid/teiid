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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.sybase.SybaseExecutionFactory;

/**
 * Updated to assume the use of the DataDirect, 2005 driver, or later.
 */
@Translator(name="sqlserver", description="A translator for Microsoft SQL Server Database")
public class SQLServerExecutionFactory extends SybaseExecutionFactory {
	
	private class ParseFormatFunctionModifier extends FunctionModifier {
		@Override
		public List<?> translate(Function function) {
			return Arrays.asList("CONVERT(" + getTarget() + ", ", function.getParameters().get(0), ", ", formatMap.get(((Literal)function.getParameters().get(1)).getValue()), ")" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		}
		
		String getTarget() {
			return "DATETIME"; //$NON-NLS-1$ 
		}
	}

	public static final String V_2005 = "2005"; //$NON-NLS-1$
	public static final String V_2008 = "2008"; //$NON-NLS-1$
	
	private Map<String, Integer> formatMap = new HashMap<String, Integer>();
	
	//TEIID-31 remove mod modifier for SQL Server 2008
	public SQLServerExecutionFactory() {
		setDatabaseVersion(V_2005);
		setMaxInCriteriaSize(JDBCExecutionFactory.DEFAULT_MAX_IN_CRITERIA);
		setMaxDependentInPredicates(JDBCExecutionFactory.DEFAULT_MAX_DEPENDENT_PREDICATES);
		formatMap.put("MM/dd/yy", 1); //$NON-NLS-1$
		formatMap.put("yy.MM.dd", 2); //$NON-NLS-1$
		formatMap.put("dd/MM/yy", 3); //$NON-NLS-1$
		formatMap.put("dd-MM-yy", 4); //$NON-NLS-1$
		formatMap.put("dd-MM-yy", 5); //$NON-NLS-1$
		formatMap.put("dd MMM yy", 6); //$NON-NLS-1$
		formatMap.put("MMM dd, yy", 7); //$NON-NLS-1$
		formatMap.put("hh:mm:ss", 8); //$NON-NLS-1$
		formatMap.put("MM-dd-yy", 10); //$NON-NLS-1$
		formatMap.put("yy/MM/dd", 11); //$NON-NLS-1$
		formatMap.put("yyMMdd", 12); //$NON-NLS-1$
		formatMap.put("kk:MM:ss:SSS", 14); //$NON-NLS-1$
		for (Map.Entry<String, Integer> entry : new HashSet<Map.Entry<String, Integer>>(formatMap.entrySet())) {
			formatMap.put(entry.getKey().replace("yy", "yyyy"), entry.getValue() + 100); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		formatMap.put("MMM dd yyyy hh:mma", 100); //$NON-NLS-1$
		formatMap.put("MMM dd yyyy hh:mm:ss:SSSa", 109); //$NON-NLS-1$
		formatMap.put("dd MMM yyyy kk:mm:ss:SSS", 113); //$NON-NLS-1$
		formatMap.put("yyyy-MM-dd kk:mm:ss", 120); //$NON-NLS-1$
		formatMap.put("yyyy-MM-dd kk:mm:ss.SSS", 121); //$NON-NLS-1$
		formatMap.put("yyyy-MM-ddTkk:mm:ss.SSS", 126); //$NON-NLS-1$
		formatMap.put("yyyy-MM-ddTkk:mm:ss.SSST", 127); //$NON-NLS-1$
		formatMap.put("dd MMM yyyy hh:mm:ss:SSSa", 130); //$NON-NLS-1$
		formatMap.put("dd/mm/yyyy hh:mm:ss:SSSa", 131); //$NON-NLS-1$
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new ParseFormatFunctionModifier());
		registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new ParseFormatFunctionModifier() {
			@Override
			String getTarget() {
				return "VARCHAR"; //$NON-NLS-1$
			}
		}); 
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
        //supportedFunctons.add("LOCATE"); //$NON-NLS-1$
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
    
    @Override
    public String translateLiteralDate(Date dateValue) {
    	if (getDatabaseVersion().compareTo(V_2008) >= 0) {
    		return super.translateLiteralDate(dateValue);
    	}
    	return super.translateLiteralTimestamp(new Timestamp(dateValue.getTime()));
    }
    
    @Override
    public boolean hasTimeType() {
    	return getDatabaseVersion().compareTo(V_2008) >= 0;
    }
    
    @Override
    public boolean supportsCommonTableExpressions() {
    	return true;
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
    public boolean supportsOnlyLiteralFormat() {
    	return true;
    }
    
}
