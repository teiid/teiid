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
package org.teiid.translator.hive;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.*;
import org.teiid.language.Join.JoinType;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AggregateAttributes;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.util.Version;

@Translator(name="impala", description="A translator for Coludera's Impala based database on HDFS")
public class ImpalaExecutionFactory extends BaseHiveExecutionFactory {

    public static String IMPALA = "impala"; //$NON-NLS-1$
    public static final Version TWO_2 = Version.getVersion("2.2"); //$NON-NLS-1$
    public static final Version TWO_0 = Version.getVersion("2.0"); //$NON-NLS-1$
    public static final Version ONE_2_1 = Version.getVersion("1.2.1"); //$NON-NLS-1$
    public static final Version ONE_3_1 = Version.getVersion("1.3.1"); //$NON-NLS-1$

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
        registerFunctionModifier("string_agg", new AliasModifier("group_concat")); //$NON-NLS-1$ //$NON-NLS-2$

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
        addPushDownFunction(IMPALA, "bin", STRING, LONG); //$NON-NLS-1$

        //date functions
        addPushDownFunction(IMPALA, "add_months", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "adddate", TIMESTAMP, TIMESTAMP, INTEGER ); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "date_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "date_part", INTEGER, STRING, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "date_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "datediff", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "day", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "dayofyear", INTEGER, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "days_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "days_add", TIMESTAMP, TIMESTAMP , LONG ); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "days_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "days_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "extract", INTEGER, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "from_unixtime", STRING, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "from_unixtime", STRING, LONG, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "from_utc_timestamp", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "hour",INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "hours_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "hours_add", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "hours_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "hours_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "microseconds_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "microseconds_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "milliseconds_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "milliseconds_add", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "milliseconds_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "milliseconds_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "minute", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "minutes_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "minutes_add", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "minutes_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "minutes_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "month", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "months_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "months_add", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "months_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "months_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "nanoseconds_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "nanoseconds_add", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "nanoseconds_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "nanoseconds_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "second", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "seconds_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "seconds_add", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "seconds_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "seconds_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "subdate", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "to_date", STRING, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "to_utc_timestamp", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "trunc", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "unix_timestamp", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "unix_timestamp", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "unix_timestamp", INTEGER, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "weekofyear", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "weeks_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "weeks_add", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "weeks_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "weeks_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "years_add", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "years_add", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "years_sub", TIMESTAMP, TIMESTAMP, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "years_sub", TIMESTAMP, TIMESTAMP, LONG); //$NON-NLS-1$

        addPushDownFunction(IMPALA, "conv", STRING, LONG, INTEGER, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "greatest", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "greatest", TIMESTAMP, TIMESTAMP, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "greatest", LONG, LONG, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "least", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "least", TIMESTAMP, TIMESTAMP, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "least", LONG, LONG, LONG); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "log2", STRING, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "pow", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "quotient", INTEGER, INTEGER, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "radians", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "sign", INTEGER, DOUBLE); //$NON-NLS-1$

        addPushDownFunction(IMPALA, "parse_url", STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "regexp_extract", STRING, STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "regexp_replace", STRING, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "group_concat", STRING, STRING, STRING).setAggregateAttributes(new AggregateAttributes()); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "concat_ws", STRING, STRING, STRING).setVarArgs(true); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "concat", STRING, STRING).setVarArgs(true); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "initcap", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "instr", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(IMPALA, "find_in_set", INTEGER, STRING, STRING); //$NON-NLS-1$

        //standard function form of several predicates
        addPushDownFunction(IMPALA, "ilike", BOOLEAN, STRING, STRING).setProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, "($1 ilike $2)"); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(IMPALA, "rlike", BOOLEAN, STRING, STRING).setProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, "($1 rlike $2)"); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(IMPALA, "iregexp", BOOLEAN, STRING, STRING).setProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, "($1 iregexp $2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public void initCapabilities(Connection connection)
            throws TranslatorException {
        super.initCapabilities(connection);
        //supported data types post-Impala v2
        if (getVersion().compareTo(TWO_0) >= 0) {
            convert.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
            convert.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
            convert.addTypeMapping("varchar", FunctionModifier.STRING); //$NON-NLS-1$
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
        if (getVersion().compareTo(TWO_2) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.FROM_UNIXTIME);
            supportedFunctions.add(SourceSystemFunctions.UNIX_TIMESTAMP);
        }
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
    public boolean supportsElementaryOlapOperations() {
        /*
        Impala supports window functions
        From Cloudera doc:  http://www.cloudera.com/documentation/archive/impala/2-x/2-1-x/topics/impala_analytic_functions.html#window_clause_unique_1
         */
        return getVersion().compareTo(TWO_0) >= 0;
    }

    @Override
    public boolean supportsHaving() {
        /*
         * From Cloudera DOC, different from Hive
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
    public boolean supportsRowOffset() {
        return getVersion().compareTo(ONE_2_1) >= 0;
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
    public boolean supportsLikeRegex() {
        return getVersion().compareTo(ONE_3_1) >= 0;
    }

    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
        if (command instanceof Select) {
            Select select = (Select)command;

            if (select.getLimit() != null && select.getLimit().getRowOffset() != 0 && select.getOrderBy() == null) {
                select.setOrderBy(new OrderBy(Arrays.asList(new SortSpecification(Ordering.ASC, new Literal(1, DataTypeManager.DefaultDataClasses.INTEGER)))));
            }

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

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof WithItem) {
            WithItem item = (WithItem)obj;
            List<ColumnReference> cols = item.getColumns();
            item.setColumns(null);
            Select select = item.getSubquery().getProjectedQuery();
            List<DerivedColumn> selectClause = select.getDerivedColumns();
            for (int i = 0; i < cols.size(); i++) {
                selectClause.get(i).setAlias(cols.get(i).getName());
            }
        }
        return super.translate(obj, context);
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        if (limit.getRowOffset() > 0) {
            return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return null;
    }

    @Override
    public String translateLiteralDate(java.sql.Date dateValue) {
        return '\'' + formatDateValue(dateValue) + '\'';
    }

    @Override
    public boolean supportsGroupByMultipleDistinctAggregates() {
        return false;
    }

    @Override
    public boolean supportsStringAgg() {
        return true;
    }

    @Override
    public boolean supportsIsDistinctCriteria() {
        return true;
    }

    @Override
    public boolean rewriteBooleanFunctions() {
        return true;
    }

}
