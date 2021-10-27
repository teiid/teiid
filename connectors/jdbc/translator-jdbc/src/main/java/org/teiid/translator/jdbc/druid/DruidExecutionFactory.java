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

package org.teiid.translator.jdbc.druid;

import org.teiid.language.*;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.jdbc.*;
import org.teiid.translator.jdbc.oracle.ConcatFunctionModifier;
import org.teiid.translator.jdbc.oracle.OracleFormatFunctionModifier;

import java.sql.Connection;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

/**
 * Translator class for Apache Druid.
 * Created by Don Krapohl 04/02/2021
 */
@Translator(name="druid", description="druid custom translator")
public class DruidExecutionFactory extends JDBCExecutionFactory {

    public static String DRUID = "druid"; //$NON-NLS-1$

    static final String UUID_TYPE = "uuid"; //$NON-NLS-1$
    private static final String INTEGER_TYPE = "integer"; //$NON-NLS-1$
    private String druidBrokerURL;   //required, defaults to null
    private String serialization="json"; //defaults to json
    private String authentication="NONE";    //defaults to no authentication
    private static final String TIME_FORMAT = "HH:mm:ss"; //$NON-NLS-1$
    private static final String DATE_FORMAT = "yyyy-MM-dd"; //$NON-NLS-1$
    private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT; //$NON-NLS-1$
    private static final String TIMESTAMP_FORMAT = DATETIME_FORMAT; // + ".FF";  //$NON-NLS-1$
    protected Map<String, Object> formatMap = new HashMap<String, Object>();

    public static final Version ZERO_17 = Version.getVersion("0.17"); //$NON-NLS-1$

    protected ConvertModifier convertModifier;
    private OracleFormatFunctionModifier parseModifier = new OracleFormatFunctionModifier("TIME_PARSE(", true); //$NON-NLS-1$


    public DruidExecutionFactory() {
        setSupportsFullOuterJoins(false);
        setSupportsOuterJoins(false);
        setSupportsInnerJoins(false);
        setSupportsOrderBy(true);
    }

    public void start() throws TranslatorException {

        super.start();
        //FUNCTION REFERENCE FOR AVATICA JDBC DRIVER: http://druid.io/docs/latest/querying/sql
        //NUMERIC FUNCTIONS
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.TRUNCATE, new AliasModifier("trunc")); //$NON-NLS-1$

        //STRING FUNCTIONS
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new AliasModifier("strpos")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("textcat")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LENGTH, new AliasModifier("char_length")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LENGTH, new AliasModifier("character_length")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LENGTH, new AliasModifier("strlen")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new ConcatFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.CONCAT2, new AliasModifier("||")); //$NON-NLS-1$

        //TIME FUNCTIONS
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("CURRENT_TIMESTAMP")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("CURRENT_DATE")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.NOW, new AliasModifier("CURRENT_TIMESTAMP")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new TimestampAddModifier());
        /*
        registerFunctionModifier(SourceSystemFunctions.YEAR, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("EXTRACT( YEAR FROM ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });

        registerFunctionModifier(SourceSystemFunctions.MONTH, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("EXTRACT(MONTH FROM ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        */

        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR,
                new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("EXTRACT(DOY FROM ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });

        //source: time_format(ts, pattern), teiid: FORMATTIMESTAMP(ts, pattern)
        registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("TIME_FORMAT(", function.getParameters().get(0), ", ",
                        function.getParameters().get(1), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        //source: time_parse(str, pattern) teiid: PARSETIMESTAMP(string, pattern)
        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("TIME_PARSE(", function.getParameters().get(0), ", ",
                        function.getParameters().get(1), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        //OTHER FUNCTIONS
        //registerFunctionModifier(SourceSystemFunctions.COALESCE, new AliasModifier("nvl")); //$NON-NLS-1$
        //registerFunctionModifier(SourceSystemFunctions.COALESCE, new AliasModifier("ifnull")); //$NON-NLS-1$

        //standard function form of several predicates
        addPushDownFunction(DRUID, "concat", STRING, STRING).setVarArgs(true); //$NON-NLS-1$
        addPushDownFunction(DRUID, "coalesce", STRING, STRING, STRING).setVarArgs(true); //$NON-NLS-1$
        addPushDownFunction(DRUID, "lower", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "upper", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "string_format", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "char_length", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "character_length", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "strlen", STRING, STRING); //$NON-NLS-1$
        //STRING_FORMAT



        addPushDownFunction(DRUID, "APPROX_COUNT_DISTINCT",BIG_INTEGER, STRING); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(DRUID, "BLOOM_FILTER",BLOB, STRING, BIG_INTEGER); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(DRUID, "substr",BLOB, STRING, BIG_INTEGER); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(DRUID, "strpos",BIG_INTEGER, STRING, STRING); //$NON-NLS-1$ //$NON-NLS-2$

        addPushDownFunction(DRUID, "regexp_extract", STRING, STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(DRUID, "btrim",STRING, STRING, STRING); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(DRUID, "ltrim",STRING, STRING, STRING); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(DRUID, "rtrim",STRING, STRING, STRING); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(DRUID, "trim",STRING, STRING, STRING, STRING); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(DRUID, "date_trunc", TIMESTAMP, STRING, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_FLOOR", TIMESTAMP, TIMESTAMP, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_FLOOR", TIMESTAMP, TIMESTAMP, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_FLOOR", TIMESTAMP, TIMESTAMP, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_FLOOR", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_SHIFT", TIMESTAMP, TIMESTAMP, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_EXTRACT", TIMESTAMP, TIMESTAMP, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_EXTRACT", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_EXTRACT", TIMESTAMP, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_PARSE", TIMESTAMP, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_PARSE", TIMESTAMP, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_FORMAT", TIMESTAMP, TIMESTAMP, STRING, STRING ); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_FORMAT", TIMESTAMP, TIMESTAMP, STRING ); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIME_FORMAT", TIMESTAMP, BIG_INTEGER, STRING ); //$NON-NLS-1$
        addPushDownFunction(DRUID, "MILLIS_TO_TIMESTAMP", TIMESTAMP, BIG_INTEGER); //$NON-NLS-1$
        addPushDownFunction(DRUID, "TIMESTAMP_TO_MILLIS", BIG_INTEGER, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(DRUID, "EXTRACT", BIG_INTEGER, STRING, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(DRUID, "FLOOR", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "CEIL", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "PARSETIMESTAMP", TIMESTAMP, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(DRUID, "YEAR", INTEGER, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(DRUID, "YEAR", INTEGER, DATE); //$NON-NLS-1$



        convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("char", FunctionModifier.STRING); //$NON-NLS-1$
        convertModifier.addTypeMapping("varchar", FunctionModifier.STRING); //$NON-NLS-1$
        convertModifier.addTypeMapping("decimal", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("float", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("real", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("boolean", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("tinyint", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("smallint", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("integer", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$

        // type conversions
        convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("TIME_FORMAT", DATE_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("TIME_FORMAT", TIME_FORMAT)); //$NON-NLS-1$
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
                return Arrays.asList("TIME_FORMAT(", ex, ", '", format, "')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("TIME_PARSE", DATE_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("TIME_PARSE", TIME_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP,
                new ConvertModifier.FormatModifier("TIME_PARSE", TIMESTAMP_FORMAT)); //$NON-NLS-1$

        convertModifier.addNumericBooleanConversions();
        convertModifier.setWideningNumericImplicit(true);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
    }

    @Override
    public void initCapabilities(Connection connection)
            throws TranslatorException {
        super.initCapabilities(connection);
    }

    // overriding execution because avatica exception information is horrible
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        return new DruidQueryExecution(command, conn, executionContext, this);
    }

    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
        /*
        if (command.getClass().toString().equals("class org.teiid.language.Select")) {
            if (null!= ((Select) command).getGroupBy() && ((Select) command).getGroupBy().getElements().size() > 0) {
                // remove any columns from group by that are not in the select
                ((Select) command).getGroupBy().getElements().removeAll(
                        ((Select) command).getGroupBy().getElements().stream().filter(
                                g -> !((Select) command).getDerivedColumns().stream().anyMatch(
                                        d -> d.toString().equals(g.toString()))).collect(Collectors.toList()));
            }
        }*/
        return super.translateCommand(command, context);
    }

        /*
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "TRUE"; //$NON-NLS-1$
        }
        return "FALSE"; //$NON-NLS-1$
    }
    */

    @Override
    public String translateLiteralDate(java.sql.Date dateValue) {
        return "DATE '" + formatDateValue(dateValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME '" + formatDateValue(timeValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        if (timestampValue.getNanos() == 0) {
            String val = formatDateValue(timestampValue);
            val = val.substring(0, val.length() - 2);
            return "TIME_PARSE('" + val + "', '" + DATETIME_FORMAT + "')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        return super.translateLiteralTimestamp(timestampValue);
    }

    @Override
    public boolean usePreparedStatements() {
        return false;
    }

    @Override
    public int getTimestampNanoPrecision() {
        return 6;
    }

    /**
     * Postgres doesn't provide min/max(boolean), so this conversion writes a min(BooleanValue) as
     * bool_and(BooleanValue)
     * @see org.teiid.language.visitor.LanguageObjectVisitor#visit(org.teiid.language.AggregateFunction)
     * @since 4.3
     */
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        /*if (obj.getClass().toString().equals("class org.teiid.language.Select")) {
            if (null!= ((Select) obj).getGroupBy() && ((Select) obj).getGroupBy().getElements().size() > 0) {
                // remove any columns from group by that are not in the select
                ((Select) obj).getGroupBy().getElements().removeAll(
                        ((Select) obj).getGroupBy().getElements().stream().filter(
                                g -> !((Select) obj).getDerivedColumns().stream().anyMatch(
                                        d -> d.toString().equals(g.toString()))).collect(Collectors.toList()));
                System.out.println(obj.toString());
            }
        }*/
        return super.translate(obj, context);
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.HIGH;
    }


    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
//        supportedFunctions.addAll(super.getSupportedFunctions());
        //DATA TYPE MAPPING AND CONVERSION

        //NUMERIC FUNCTIONS
        supportedFunctions.add(SourceSystemFunctions.MULTIPLY_OP);
        supportedFunctions.add(SourceSystemFunctions.ADD_OP);
        supportedFunctions.add(SourceSystemFunctions.DIVIDE_OP);
        supportedFunctions.add(SourceSystemFunctions.SUBTRACT_OP);
        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TRUNCATE);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.ROUND);

        //STRING FUNCTIONS
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        
        //Druid doesn't support NVL 
        //supportedFunctions.add("NVL");      //$NON-NLS-1$
        
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        //supportedFunctions.add("IFNULL"); //$NON-NLS-1$

        //TIME FUNCTIONS
        supportedFunctions.add(SourceSystemFunctions.FORMATTIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPADD);
        //supportedFunctions.add(SourceSystemFunctions.TIMESTAMPDIFF);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        supportedFunctions.add(SourceSystemFunctions.QUARTER);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.SECOND);

        //COMPARISON OPERATORS
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);
        supportedFunctions.add("BETWEEN");

        //OTHER FUNCTIONS
       
        return supportedFunctions;
    }
    @Override
    public boolean supportsHaving() {
        return true;
    }
    @Override
    public boolean supportsOrderByNullOrdering() {
        return false;
    }

    @Override
    public boolean supportsSelfJoins() {
        return false;
    }
    @Override
    public boolean supportsBulkUpdate() {
        return false;
    }
    @Override
    public boolean supportsBatchedUpdates() {
        return false;
    }
    @Override
    public boolean supportsInsertWithQueryExpression() {
        return false;
    }
    @Override
    public boolean supportsCommonTableExpressions() {
        return false; // WITH clause
    }
    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
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
    public boolean supportsNotCriteria() {
        return true;
    }
    @Override
    public boolean supportsOrCriteria() {
        return true;
    }
    @Override
    public boolean supportsInCriteriaSubquery() {
        return true;
    }

    // aggregate functions
    @Override
    public boolean supportsAggregatesSum() {
        return true;
    }
    @Override
    public boolean supportsAggregatesAvg() {
        return true;
    }
    @Override
    public boolean supportsAggregatesMin() {
        return true;
    }
    @Override
    public boolean supportsAggregatesMax() {
        return true;
    }
    @Override
    public boolean supportsAggregatesCount() {
        return true;
    }
    @Override
    public boolean supportsAggregatesCountStar() {
        return false;
    }
    @Override
    public boolean supportsAggregatesDistinct() {
        return false;
    }   // disable count(distinct)

    @Override
    public boolean supportsScalarSubqueries() {
        return false;
    }
    @Override
    public boolean supportsLikeRegex() {
        return true;
    }   // we'll be rewriting LIKE as regexp
    @Override
    public boolean supportsSearchedCaseExpressions() {
        return true;
    }
    @Override
    public boolean supportsUnions() {
        return false;
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
    public boolean supportsElementaryOlapOperations() {
        return false;
    }
    @Override
    public boolean supportsFunctionsInGroupBy() {
        return true;
    }

    @Override
    public boolean supportsOnlyTimestampAddLiteral() {
        return true;
    }
    @Override
    public boolean supportsFormatLiteral(String literal,
                                         org.teiid.translator.ExecutionFactory.Format format) {
        if (format == Format.NUMBER) {
            return false;
        }
        return parseModifier.supportsLiteral(literal);
    }

}