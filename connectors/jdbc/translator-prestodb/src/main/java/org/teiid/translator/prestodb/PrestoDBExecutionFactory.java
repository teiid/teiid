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

package org.teiid.translator.prestodb;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BIG_INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BOOLEAN;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.CHAR;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DOUBLE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.TIMESTAMP;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.VARBINARY;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;
import org.teiid.translator.jdbc.JDBCUpdateExecution;

@Translator(name="prestodb", description="PrestoDB custom translator")
public class PrestoDBExecutionFactory extends JDBCExecutionFactory {
    private static final String PRESTODB = "prestodb"; //$NON-NLS-1$
    
    public PrestoDBExecutionFactory() {
        setSupportsSelectDistinct(true);
        setSupportsInnerJoins(true);
        setSupportsOuterJoins(true);
        setSupportsFullOuterJoins(true);
        setUseBindVariables(false);
    }
    
    @Override
    public JDBCUpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        throw new TranslatorException(PrestoDBPlugin.Event.TEIID26000, PrestoDBPlugin.Util.gs(PrestoDBPlugin.Event.TEIID26000, command));
    }   
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        throw new TranslatorException(PrestoDBPlugin.Event.TEIID26000, PrestoDBPlugin.Util.gs(PrestoDBPlugin.Event.TEIID26000, command));
    }    
    
    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        throw new TranslatorException(PrestoDBPlugin.Event.TEIID26000, PrestoDBPlugin.Util.gs(PrestoDBPlugin.Event.TEIID26000, command));
    } 
    
    @Override
    public boolean useAnsiJoin() {
        return true;
    }
    
    @Deprecated
    @Override
    protected JDBCMetdataProcessor createMetadataProcessor() {
        return (PrestoDBMetadataProcessor)getMetadataProcessor();
    }
    
    @Override
    public MetadataProcessor<Connection> getMetadataProcessor(){
        return new PrestoDBMetadataProcessor();
    }
    
    @Override
    public boolean isSourceRequiredForMetadata() {
        return true;
    }
    
    @Override
    public void start() throws TranslatorException {
        super.start();
        
        ConvertModifier convert = new ConvertModifier();
        convert.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convert.addTypeMapping("bigint", FunctionModifier.BIGINTEGER, FunctionModifier.LONG); //$NON-NLS-1$
        convert.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convert.addTypeMapping("varchar", FunctionModifier.STRING); //$NON-NLS-1$
        convert.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        convert.addTypeMapping("time with timezone", FunctionModifier.TIME); //$NON-NLS-1$
        convert.addTypeMapping("timestamp with timezone", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convert.addTypeMapping("varbinary", FunctionModifier.BLOB); //$NON-NLS-1$
        convert.addTypeMapping("json", FunctionModifier.BLOB); //$NON-NLS-1$
        
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);        
        
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("current_date")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("current_time")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("nullif")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new AliasModifier("format_datetime")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new AliasModifier("parse_datetime")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.POWER, new AliasModifier("pow")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new AliasModifier("strpos")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
        
        addPushDownFunction(PRESTODB, "cbrt", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "chr", STRING, CHAR); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "ceil", INTEGER, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "current_timestamp", TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "current_timezone", STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "e", DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "ln", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "log2", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "random", DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "cosh", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "tanh", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "infinity", DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "is_finite", BOOLEAN, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "is_infinite", BOOLEAN, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "is_nan", BOOLEAN, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "nan", DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "reverse", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "split_part", STRING, STRING, CHAR, INTEGER); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "to_base64", STRING, VARBINARY); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "from_base64", VARBINARY, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "to_base64url", STRING, VARBINARY); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "from_base64url", VARBINARY, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "to_hex", STRING, VARBINARY); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "from_hex", VARBINARY, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "timezone_hour", BIG_INTEGER, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "timezone_minute", BIG_INTEGER, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "regexp_extract", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "regexp_extract", STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "regexp_like", BOOLEAN, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "regexp_replace", STRING, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "url_extract_fragment", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "url_extract_host", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "url_extract_parameter", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "url_extract_path", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "url_extract_port", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "url_extract_protocol", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "url_extract_query", STRING, STRING); //$NON-NLS-1$
        
        // TODO: JSON functions, not sure how to represent the JSON type?
        // Array Functions, MAP functions?
        // aggregate functions?
    }    
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        //supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        //supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
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
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
//        supportedFunctions.add(SourceSystemFunctions.DEGREES);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.FORMATTIMESTAMP);
        
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
//        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.NOW);
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.QUARTER);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);

//        supportedFunctions.add(SourceSystemFunctions.RADIANS);
        
        supportedFunctions.add(SourceSystemFunctions.ROUND);       
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
//        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.TAN);
        supportedFunctions.add(SourceSystemFunctions.TRIM);        
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        return supportedFunctions;
    }     

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }
    
    @Override
    public boolean supportsSubqueryInOn() {
        return true;
    }

    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return false;
    }    
    
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }
    
    @Override
    public boolean supportsOrderByNullOrdering() {
        return true;
    }    
    
    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        return true;
    }    
    
    @Override
    public boolean supportsIntersect() {
        return false;
    }
    
    @Override
    public boolean supportsExcept() {
        return false;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }
    
    @Override
    public boolean supportsRowOffset() {
        return false;
    }
    
    @Override
    public boolean supportsFunctionsInGroupBy() {
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
    public boolean supportsCommonTableExpressions() {
        return true;
    }    
    
    @Override
    public boolean supportsElementaryOlapOperations() {
        return true;
    }
    
    @Override
    public boolean supportsArrayType() {
        return true;
    }    
}
