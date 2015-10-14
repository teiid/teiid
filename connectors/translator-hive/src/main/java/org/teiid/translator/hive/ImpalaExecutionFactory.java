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
package org.teiid.translator.hive;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.Join.JoinType;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.Version;

@Translator(name="impala", description="A translator for Coludera's Impala based database on HDFS")
public class ImpalaExecutionFactory extends BaseHiveExecutionFactory {
    
    public static String IMPALA = "impala"; //$NON-NLS-1$
    public static final Version TWO_0 = Version.getVersion("2.0"); //$NON-NLS-1$
    
    @Override
    public void start() throws TranslatorException {
        super.start();

        convert.addTypeMapping("tinyint", FunctionModifier.BYTE); //$NON-NLS-1$
        convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
        convert.addTypeMapping("int", FunctionModifier.INTEGER); //$NON-NLS-1$
        convert.addTypeMapping("bigint", FunctionModifier.BIGINTEGER, FunctionModifier.LONG); //$NON-NLS-1$
        convert.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convert.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convert.addTypeMapping("float", FunctionModifier.FLOAT); //$NON-NLS-1$
        convert.addTypeMapping("string", FunctionModifier.STRING); //$NON-NLS-1$
        convert.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("unix_timestamp")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("isnull")); //$NON-NLS-1$
        
        
        addPushDownFunction(IMPALA, "lower", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "upper", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "positive", INTEGER, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "positive", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "negitive", INTEGER, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "negitive", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "ln", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "reverse", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "space", STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "hex", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "unhex", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "bin", STRING, BIG_INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "day", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "datediff", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "date_add", INTEGER, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "date_sub", INTEGER, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "from_unixtime", STRING, BIG_INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "from_unixtime", STRING, BIG_INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "unix_timestamp", BIG_INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "unix_timestamp", BIG_INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "to_date", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "from_utc_timestamp", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "to_utc_timestamp", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "conv", STRING, BIG_INTEGER, INTEGER, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "greatest", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "greatest", TIMESTAMP, TIMESTAMP, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "greatest", BIG_INTEGER, BIG_INTEGER, BIG_INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "least", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "least", TIMESTAMP, TIMESTAMP, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "least", BIG_INTEGER, BIG_INTEGER, BIG_INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "log2", STRING, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "pow", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "quotient", INTEGER, INTEGER, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "radians", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "sign", INTEGER, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "weekofyear", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "initcap", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "instr", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "parse_url", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "regexp_extract", STRING, STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "regexp_replace", STRING, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "group_concat", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "find_in_set", INTEGER, STRING, STRING); //$NON-NLS-1$
    }
    
    @Override
    public void initCapabilities(Connection connection)
    		throws TranslatorException {
    	super.initCapabilities(connection);
    	if (getVersion().compareTo(TWO_0) >= 0) {
    		convert.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
    	}
    }
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        //supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        //supportedFunctions.add(SourceSystemFunctions.BITAND);
        //supportedFunctions.add(SourceSystemFunctions.BITNOT);
        //supportedFunctions.add(SourceSystemFunctions.BITOR);
        //supportedFunctions.add(SourceSystemFunctions.BITXOR);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
//        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.DEGREES);
        supportedFunctions.add(SourceSystemFunctions.DAYNAME);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LCASE); //lower
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MOD); //pmod
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.NOW);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.RADIANS);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SIN);

        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING); //substr
        supportedFunctions.add(SourceSystemFunctions.TAN);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        return supportedFunctions;
    }   
    
    @Override
    public boolean supportsCommonTableExpressions() {
        return true; // WITH clause
    }
    
    @Override
    public boolean supportsHaving() {
        /*
         * From Coludera DOC, different from Hive
         * Performs a filter operation on a SELECT query, by examining the results of 
         * aggregation functions rather than testing each individual table row. Thus 
         * always used in conjunction with a function such as COUNT(), SUM(), AVG(), 
         * MIN(), or MAX(), and typically with the GROUP BY clause also.
         */
        return true; 
    }    
    
    @Override
    public boolean supportsRowLimit() {
        /*
         * In Impala 1.2.1 and higher, you can combine a LIMIT clause with an OFFSET clause 
         * to produce a small result set that is different from a top-N query
         */
        return true;
    }
    
    @Override
    public org.teiid.translator.ExecutionFactory.NullOrder getDefaultNullOrder() {
    	return NullOrder.HIGH;
    }
    
    @Override
    public boolean supportsOrderByNullOrdering() {
    	return true;
    }
    
    @Override
    public org.teiid.translator.ExecutionFactory.SupportedJoinCriteria getSupportedJoinCriteria() {
    	return SupportedJoinCriteria.ANY;
    }
    
    @Override
    public boolean requiresLeftLinearJoin() {
    	return true;
    }
    
    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
    	if (command instanceof Select) {
    		Select select = (Select)command;
    		//compensate for an impala issue - https://issues.jboss.org/browse/TEIID-3743
    		if (select.getGroupBy() == null && select.getHaving() == null) {
    			boolean rewrite = false;
    			String distinctVal = null;
    			for (DerivedColumn col : select.getDerivedColumns()) {
    				if (col.getExpression() instanceof AggregateFunction && ((AggregateFunction)col.getExpression()).isDistinct()) {
    					if (distinctVal == null) {
    						distinctVal = ((AggregateFunction)col.getExpression()).getParameters().toString();
    					} else if (!((AggregateFunction)col.getExpression()).getParameters().toString().equals(distinctVal)){
    						rewrite = true;
    						break;
    					}
    				}
    			}
    			if (rewrite) {
    				Select top = new Select();
    				top.setWith(select.getWith());
    				top.setDerivedColumns(new ArrayList<DerivedColumn>());
    				top.setFrom(new ArrayList<TableReference>());
    				//rewrite as a cross join of single groups
    				Select viewSelect = new Select();
    				viewSelect.setFrom(select.getFrom());
    				viewSelect.setDerivedColumns(new ArrayList<DerivedColumn>());
    				viewSelect.setWhere(select.getWhere());
    				distinctVal = null;
        			int viewCount = 0;
        			NamedTable view = new NamedTable("v" + viewCount++, null, null); //$NON-NLS-1$
        			for (int i = 0; i < select.getDerivedColumns().size(); i++) {
        				DerivedColumn col = select.getDerivedColumns().get(i);
        				if (col.getExpression() instanceof AggregateFunction && ((AggregateFunction)col.getExpression()).isDistinct()) {
        					if (distinctVal == null) {
        						distinctVal = ((AggregateFunction)col.getExpression()).getParameters().toString();
        					} else if (!((AggregateFunction)col.getExpression()).getParameters().toString().equals(distinctVal)){
        						DerivedTable dt = new DerivedTable(viewSelect, view.getName());
        						if (top.getFrom().isEmpty()) {
        							top.getFrom().add(dt);
        						} else {
        							Join join = new Join(top.getFrom().remove(0), dt, JoinType.CROSS_JOIN, null);
        							top.getFrom().add(join);
        						}
        						view = new NamedTable("v" + viewCount++, null, null); //$NON-NLS-1$
        						viewSelect = new Select();
        						viewSelect.setFrom(select.getFrom());
        						viewSelect.setDerivedColumns(new ArrayList<DerivedColumn>());
        						viewSelect.setWhere(select.getWhere());
        						distinctVal = ((AggregateFunction)col.getExpression()).getParameters().toString();
        					}
        				}
        				col.setAlias("c" + i); //$NON-NLS-1$
        				top.getDerivedColumns().add(new DerivedColumn(null, new ColumnReference(view, col.getAlias(), null, col.getExpression().getType())));
        				viewSelect.getDerivedColumns().add(col);
        			}
        			DerivedTable dt = new DerivedTable(viewSelect, view.getName());
        			Join join = new Join(top.getFrom().remove(0), dt, JoinType.CROSS_JOIN, null);
					top.getFrom().add(join);
					return Arrays.asList(top);
    			}
    		}
    	}
    	return super.translateCommand(command, context);
    }
    
}
