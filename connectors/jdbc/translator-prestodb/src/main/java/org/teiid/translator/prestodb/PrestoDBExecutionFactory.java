/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Call;
import org.teiid.language.Function;
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
import org.teiid.translator.jdbc.JDBCMetadataProcessor;
import org.teiid.util.Version;

@Translator(name="prestodb", description="PrestoDB custom translator")
public class PrestoDBExecutionFactory extends JDBCExecutionFactory {
    private static final String PRESTODB = "prestodb"; //$NON-NLS-1$

    public static final Version V_0_153 = Version.getVersion("0.153"); //$NON-NLS-1$

    private ConvertModifier convert = new ConvertModifier();

    public PrestoDBExecutionFactory() {
        setSupportsSelectDistinct(true);
        setSupportsInnerJoins(true);
        setSupportsOuterJoins(true);
        setSupportsFullOuterJoins(true);
        setUseBindVariables(false);
        setTransactionSupport(TransactionSupport.NONE);
    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        throw new TranslatorException(PrestoDBPlugin.Event.TEIID26000, PrestoDBPlugin.Util.gs(PrestoDBPlugin.Event.TEIID26000, command));
    }

    @Override
    public boolean useAnsiJoin() {
        return true;
    }

    @Deprecated
    @Override
    protected JDBCMetadataProcessor createMetadataProcessor() {
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
    public boolean supportsConvert(int fromType, int toType) {
        if (!super.supportsConvert(fromType, toType)) {
            return false;
        }
        if (convert.hasTypeMapping(toType)) {
            return true;
        }
        return false;
    }


    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public void initCapabilities(Connection connection)
            throws TranslatorException {
        super.initCapabilities(connection);
        if (getVersion().compareTo(V_0_153) >= 0) {
            convert.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
            convert.addTypeMapping("tinyint", FunctionModifier.BYTE); //$NON-NLS-1$
            convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
            convert.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
        }
    }

    @Override
    public void start() throws TranslatorException {
        super.start();

        convert.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convert.addTypeMapping("bigint", FunctionModifier.BIGINTEGER, FunctionModifier.LONG); //$NON-NLS-1$
        convert.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convert.addTypeMapping("varchar", FunctionModifier.STRING); //$NON-NLS-1$
        convert.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        convert.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
        convert.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convert.addTypeMapping("varbinary", FunctionModifier.BLOB); //$NON-NLS-1$
        convert.addTypeMapping("json", FunctionModifier.BLOB); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);

        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("current_date")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("current_time")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day_of_month")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new AliasModifier("day_of_week")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new AliasModifier("day_of_year")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new AliasModifier("format_datetime")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new AliasModifier("parse_datetime")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.POWER, new AliasModifier("pow")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln"){ //$NON-NLS-1$
            @Override
            protected void modify(Function function) {
                if(function.getParameters().size() == 1){
                    super.modify(function);
                }
            }});

        addPushDownFunction(PRESTODB, "cbrt", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "ceil", INTEGER, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "current_timestamp", TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "current_timezone", STRING); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "e", DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "ln", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "log2", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(PRESTODB, "log", DOUBLE, DOUBLE, INTEGER); //$NON-NLS-1$
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
        supportedFunctions.add(SourceSystemFunctions.CHAR);
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
          //TEIID-4680 TEIID-4679 3 arg locate is not supported
//        supportedFunctions.add(SourceSystemFunctions.LOCATE);
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

    /**
     * Base on https://prestodb.io/docs/current/functions/datetime.html, the support format are
     * date '2012-08-08', time '01:00', timestamp '2012-08-08 01:00'
     */
    @Override
    public String translateLiteralDate(Date dateValue) {
        return "date '" + formatDateValue(dateValue) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "time '" + formatDateValue(timeValue) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "timestamp '" + formatDateValue(timestampValue) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
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

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }
}
