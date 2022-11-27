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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Function;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.ModFunctionModifier;
import org.teiid.util.Version;

@Translator(name="hive", description="A translator for hive based database on HDFS")
public class HiveExecutionFactory extends BaseHiveExecutionFactory {

    public static final Version V_3 = Version.getVersion("3.0"); //$NON-NLS-1$

    public static String HIVE = "hive"; //$NON-NLS-1$
    public HiveExecutionFactory() {
        setSupportedJoinCriteria(SupportedJoinCriteria.EQUI);
        setSupportsOrderBy(false); //TEIID-4858 hive order by performance is slow
    }

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
        convert.addTypeMapping("binary", FunctionModifier.BLOB, FunctionModifier.VARBINARY); //$NON-NLS-1$
        convert.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convert.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        // unsupported types
        //FunctionModifier.TIME,
        //FunctionModifier.CHAR,
        //FunctionModifier.CLOB,
        //FunctionModifier.XML

        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);

        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("&")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("~")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("|")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("^")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("unix_timestamp")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier("%", getLanguageFactory(), Arrays.asList(TypeFacility.RUNTIME_TYPES.BIG_INTEGER, TypeFacility.RUNTIME_TYPES.BIG_DECIMAL))); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ARRAY_GET, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                return Arrays.asList(function.getParameters().get(0), '[', function.getParameters().get(1), ']');
            }
        });

        addPushDownFunction(HIVE, "lower", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "upper", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "positive", INTEGER, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(HIVE, "positive", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(HIVE, "negitive", INTEGER, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(HIVE, "negitive", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(HIVE, "ln", DOUBLE, DOUBLE); //$NON-NLS-1$
        addPushDownFunction(HIVE, "reverse", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "space", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "split", OBJECT, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "hex", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "unhex", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "bin", STRING, LONG); //$NON-NLS-1$
        addPushDownFunction(HIVE, "day", INTEGER, DATE); //$NON-NLS-1$
        addPushDownFunction(HIVE, "datediff", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "date_add", INTEGER, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(HIVE, "date_sub", INTEGER, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(HIVE, "from_unixtime", STRING, LONG); //$NON-NLS-1$
        addPushDownFunction(HIVE, "from_unixtime", STRING, LONG, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "unix_timestamp", LONG, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "unix_timestamp", LONG, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "to_date", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "from_utc_timestamp", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$
        addPushDownFunction(HIVE, "to_utc_timestamp", TIMESTAMP, TIMESTAMP, STRING); //$NON-NLS-1$

        addAggregatePushDownFunction(HIVE, "LEAD", OBJECT, OBJECT); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "LEAD", OBJECT, OBJECT, INTEGER); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "LEAD", OBJECT, OBJECT, INTEGER, OBJECT); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "LAG", OBJECT, OBJECT); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "LAG", OBJECT, OBJECT, INTEGER); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "LAG", OBJECT, OBJECT, INTEGER, OBJECT); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "FIRST_VALUE", OBJECT, OBJECT); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "LAST_VALUE", OBJECT, OBJECT); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "PERCENT_RANK", FLOAT); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "CUME_DIST", FLOAT); //$NON-NLS-1$
        addAggregatePushDownFunction(HIVE, "NTILE", LONG, INTEGER); //$NON-NLS-1$
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        supportedFunctions.add(SourceSystemFunctions.BITNOT);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.DEGREES);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.FROM_UNIXTIME);
        supportedFunctions.add(SourceSystemFunctions.UNIX_TIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.RADIANS);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.TAN);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        return supportedFunctions;
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
    public boolean supportsGroupByRollup() {
        //https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation,+Cube,+Grouping+and+Rollup
        return true;
    }

    @Override
    public org.teiid.translator.ExecutionFactory.SupportedJoinCriteria getSupportedJoinCriteria() {
        return SupportedJoinCriteria.EQUI;
    }

    @Override
    public boolean useParensForJoins() {
        return false;
    }

    @Override
    public String translateLiteralDate(java.sql.Date dateValue) {
        return "DATE '" + formatDateValue(dateValue) + '\'';
    }

    @Override
    public boolean requiresLeftLinearJoin() {
        return true;
    }

    @Override
    public boolean supportsIsDistinctCriteria() {
        return getVersion().compareTo(V_3) >= 0;
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }
}
