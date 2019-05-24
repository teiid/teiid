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
package org.teiid.translator.jdbc.actian;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BIG_DECIMAL;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.CHAR;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DATE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.FLOAT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.TIME;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.TIMESTAMP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Function;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;

@Translator(name="actian-vector", description="A translator for Actian Vector in Hadoop")
public class ActianVectorInHadoopExecutionFactory extends JDBCExecutionFactory{

    public static final String ACTIAN = "actian"; //$NON-NLS-1$


    @Override
    public void start() throws TranslatorException {
        super.start();

        ConvertModifier convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("CHAR(1)", FunctionModifier.CHAR);  //$NON-NLS-1$
        convertModifier.addTypeMapping("VARCHAR", FunctionModifier.STRING);  //$NON-NLS-1$
        convertModifier.addTypeMapping("NVARCHAR", FunctionModifier.STRING);  //$NON-NLS-1$
        convertModifier.addTypeMapping("INTEGER1", FunctionModifier.BYTE);  //$NON-NLS-1$
        convertModifier.addTypeMapping("INTEGER2", FunctionModifier.SHORT);  //$NON-NLS-1$
        convertModifier.addTypeMapping("INTEGER4", FunctionModifier.INTEGER);  //$NON-NLS-1$
        convertModifier.addTypeMapping("INTEGER8", FunctionModifier.LONG);  //$NON-NLS-1$
        convertModifier.addTypeMapping("DECIMAL", FunctionModifier.BIGDECIMAL);  //$NON-NLS-1$
        convertModifier.addTypeMapping("MONEY", FunctionModifier.BIGDECIMAL);  //$NON-NLS-1$
        convertModifier.addTypeMapping("FLOAT", FunctionModifier.DOUBLE);  //$NON-NLS-1$
        convertModifier.addTypeMapping("FLOAT4", FunctionModifier.FLOAT);  //$NON-NLS-1$
        convertModifier.addTypeMapping("ANSIDATE", FunctionModifier.DATE);  //$NON-NLS-1$
        convertModifier.addTypeMapping("TIME WITHOUT TIME ZONE", FunctionModifier.TIME);  //$NON-NLS-1$
        convertModifier.addTypeMapping("TIME WITH TIME ZONE", FunctionModifier.TIME);  //$NON-NLS-1$
        convertModifier.addTypeMapping("TIME WITH LOCAL TIME ZONE", FunctionModifier.TIME);  //$NON-NLS-1$
        convertModifier.addTypeMapping("TIMESTAMP WITHOUT TIME ZONE", FunctionModifier.TIMESTAMP);  //$NON-NLS-1$
        convertModifier.addTypeMapping("TIMESTAMP WITH TIME ZONE", FunctionModifier.TIMESTAMP);  //$NON-NLS-1$
        convertModifier.addTypeMapping("TIMESTAMP WITH LOCAL TIME ZONE", FunctionModifier.TIMESTAMP);  //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);

        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("CHR"));
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("LOWERCASE"));
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UPPERCASE"));
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("CEIL"));
        registerFunctionModifier(SourceSystemFunctions.NULLIF, new AliasModifier("NVL"));
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new Constant("CURRENT_DATE"));
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new Constant("CURRENT_TIME"));
        registerFunctionModifier(SourceSystemFunctions.RAND, new AliasModifier("RANDOM"));

        registerFunctionModifier("CURRENT_TIMESTAMP", new Constant());
        registerFunctionModifier("CURRENT_USER", new Constant());
        registerFunctionModifier("INITIAL_USER", new Constant());
        registerFunctionModifier("LOCAL_TIME", new Constant());
        registerFunctionModifier("LOCAL_TIMESTAMP", new Constant());
        registerFunctionModifier("SESSION_USER", new Constant());
        registerFunctionModifier("SYSTEM_USER", new Constant());
        registerFunctionModifier("USER", new Constant());

        //pushdown
        addPushDownFunction(ACTIAN, "CHAREXTRACT", CHAR, STRING, INTEGER);
        addPushDownFunction(ACTIAN, "SHIFT", STRING, STRING, INTEGER);
        addPushDownFunction(ACTIAN, "SIZE", INTEGER, STRING);
        addPushDownFunction(ACTIAN, "SOUNDEX", STRING, STRING);
        addPushDownFunction(ACTIAN, "SQUEEZE", STRING, STRING);
        addPushDownFunction(ACTIAN, "TRUNC", BIG_DECIMAL, BIG_DECIMAL, INTEGER);
        addPushDownFunction(ACTIAN, "DAY", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "DAY", INTEGER, DATE);
        addPushDownFunction(ACTIAN, "CURRENT_TIMESTAMP", TIMESTAMP);
        addPushDownFunction(ACTIAN, "CURRENT_USER", STRING);
        addPushDownFunction(ACTIAN, "INITIAL_USER", STRING);
        addPushDownFunction(ACTIAN, "LOCAL_TIME", TIME);
        addPushDownFunction(ACTIAN, "LOCAL_TIMESTAMP", TIMESTAMP);
        addPushDownFunction(ACTIAN, "SESSION_USER", STRING);
        addPushDownFunction(ACTIAN, "SYSTEM_USER", STRING);
        addPushDownFunction(ACTIAN, "USER", STRING);
        addPushDownFunction(ACTIAN, "ADD_MONTHS", TIMESTAMP, TIMESTAMP, INTEGER);
        addPushDownFunction(ACTIAN, "DATE_FORMAT", STRING, TIMESTAMP, STRING);
        addPushDownFunction(ACTIAN, "DATE_PART", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "DATE_TRUNC", TIMESTAMP, TIMESTAMP);
        addPushDownFunction(ACTIAN, "DAY", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "DAYOFMONTH", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "DAYOFWEEK", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "DAYOFYEAR", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "FROM_UNIXTIME", TIMESTAMP, INTEGER);
        addPushDownFunction(ACTIAN, "HOUR", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "LASTDAY", TIMESTAMP, TIMESTAMP);
        addPushDownFunction(ACTIAN, "MICROSECOND", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "MILLISECOND", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "MINUTE", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "MONTH", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "MONTHS_BETWEEN", FLOAT, TIMESTAMP, DATE, DATE);
        addPushDownFunction(ACTIAN, "MONTHS_BETWEEN", FLOAT, TIMESTAMP, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(ACTIAN, "NANOSECOND", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "QUATER", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "SECOND", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "WEEK", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "YEAR", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "YEAR_WEEK", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "UNIX_TIMESTAMP", INTEGER, TIMESTAMP);
        addPushDownFunction(ACTIAN, "WEEK_ISO", INTEGER, TIMESTAMP);
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.CONVERT);

        // string functions
        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.CHAR);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.INITCAP);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.UCASE);

        // numeric functions
        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TAN);

        //date time functions
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.MONTHNAME);
        supportedFunctions.add(SourceSystemFunctions.NOW);
        supportedFunctions.add(SourceSystemFunctions.QUARTER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);

        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);

        return supportedFunctions;
    }

    static class Constant extends FunctionModifier{
        private String name;
        public Constant() {
        }
        public Constant(String name) {
            this.name = name;
        }

        @Override
        public List<?> translate(Function function) {
            if (this.name != null) {
                function.setName(this.name);
            }
            return Arrays.asList(function.getName());
        }
    }

    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsIntersect() {
        return true;
    }

    @Override
    public boolean supportsOrderByWithExtendedGrouping() {
        return true;
    }

    @Override
    public boolean supportsGroupByRollup() {
        return true;
    }

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        return true;
    }

    @Override
    public boolean supportsExcept() {
        return true;
    }

    @Override
    public boolean supportsSetQueryOrderBy() {
        return true;
    }

    @Override
    public boolean supportsCommonTableExpressions() {
        return true;
    }

    @Override
    public boolean supportsSubqueryCommonTableExpressions() {
        return true;
    }

    @Override
    public boolean supportsElementaryOlapOperations(){
        return true;
    }

    @Override
    public boolean supportsWindowOrderByWithAggregates() {
        return true;
    }

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }

    @Override
    public boolean supportsSelectExpression() {
        return true;
    }
}
