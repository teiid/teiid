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
 *
 */
package org.teiid.translator.jdbc.exasol;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Function;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.TypeFacility.RUNTIME_CODES;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.oracle.ConcatFunctionModifier;

/**
 * Translator for the EXASOL database.
 *
 * Acknowledgements to Nevena Tacheva from Exasol for the initial work on
 * identifying most of pushdown and supported functions.
 */
@Translator(name="exasol", description="Translator for EXASOL database")
public class ExasolExecutionFactory extends JDBCExecutionFactory {

    public static final String EXASOL = "exasol";
    public static final String TIME_FORMAT = "HH24:MI:SS";
    public static final String DATE_FORMAT = "YYYY-MM-DD";
    public static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;
    public static final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".FF1";

    //Numeric
    public static final String LN = "LN";

    //Bitwise
    public static final String BIT_AND = "BIT_AND";
    public static final String BIT_NOT = "BIT_NOT";
    public static final String BIT_OR = "BIT_OR";
    public static final String BIT_XOR = "BIT_XOR";

    //String
    public static final String BIT_LENGTH = "BIT_LENGTH";
    public static final String CHARACTER_LENGTH = "CHARACTER_LENGTH";
    public static final String CHR = "CHR";
    public static final String COLOGNE_PHONETIC = "COLOGNE_PHONETIC";
    public static final String EDIT_DISTANCE = "EDIT_DISTANCE";
    public static final String INSTR = "INSTR";
    public static final String LOWER = "LOWER";
    public static final String LTRIM = "LTRIM";
    public static final String MID = "MID";
    public static final String OCTET_LENGTH = "OCTET_LENGTH";
    public static final String REGEXP_INSTR = "REGEXP_INSTR";
    public static final String REGEXP_REPLACE = "REGEXP_REPLACE";
    public static final String REGEXP_SUBSTR = "REGEXP_SUBSTR";
    public static final String REPLACE = "REPLACE";
    public static final String REVERSE = "REVERSE";
    public static final String RTRIM = "RTRIM";
    public static final String SOUNDEX = "SOUNDEX";
    public static final String SPACE = "SPACE";
    public static final String TO_NUMBER = "TO_NUMBER";
    public static final String UNICODE = "UNICODE";
    public static final String UNICODECHR = "UNICODECHR";
    public static final String UPPER = "UPPER";

    //Date
    public static final String ADD_DAYS = "ADD_DAYS";
    public static final String ADD_HOURS = "ADD_HOURS";
    public static final String ADD_MINUTES = "ADD_MINUTES";
    public static final String ADD_MONTHS = "ADD_MONTHS";
    public static final String ADD_SECONDS = "ADD_SECONDS";
    public static final String ADD_WEEKS = "ADD_WEEKS";
    public static final String ADD_YEARS = "ADD_YEARS";
    public static final String DATE_TRUNC = "DATE_TRUNC";
    public static final String DAY = "DAY";
    public static final String DAYS_BETWEEN = "DAYS_BETWEEN";
    public static final String FROM_POSIX_TIME = "FROM_POSIX_TIME";
    public static final String HOURS_BETWEEN = "HOURS_BETWEEN";
    public static final String MINUTES_BETWEEN = "MINUTES_BETWEEN";
    public static final String MONTHS_BETWEEN = "MONTHS_BETWEEN";
    public static final String POSIX_TIME = "POSIX_TIME";
    public static final String ROUND = "ROUND";
    public static final String SECONDS_BETWEEN = "SECONDS_BETWEEN";
    public static final String TO_CHAR = "TO_CHAR";
    public static final String TO_TIMESTAMP = "TO_TIMESTAMP";
    public static final String TRUNC = "TRUNC";
    public static final String TRUNCATE = "TRUNCATE";
    public static final String YEARS_BETWEEN = "YEARS_BETWEEN";

    @Override
    public void start() throws TranslatorException {
        super.start();

        //Numeric
        addPushDownFunction(EXASOL, SourceSystemFunctions.SUBTRACT_OP, BOOLEAN, BOOLEAN, FLOAT);
        addPushDownFunction(EXASOL, SourceSystemFunctions.SUBTRACT_OP, BOOLEAN, BOOLEAN, DOUBLE);
        addPushDownFunction(EXASOL, SourceSystemFunctions.SUBTRACT_OP, BOOLEAN, BOOLEAN, FLOAT);

        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier(LN));

        //Bitwise
        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier(BIT_AND));
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier(BIT_NOT));
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier(BIT_OR));
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier(BIT_XOR));

        //String
        addPushDownFunction(EXASOL, BIT_LENGTH, INTEGER, STRING);
        addPushDownFunction(EXASOL, COLOGNE_PHONETIC, STRING, STRING);
        addPushDownFunction(EXASOL, EDIT_DISTANCE, INTEGER, STRING, STRING);
        addPushDownFunction(EXASOL, INSTR, INTEGER, STRING, STRING);
        addPushDownFunction(EXASOL, INSTR, INTEGER, STRING, STRING, INTEGER);
        addPushDownFunction(EXASOL, INSTR, INTEGER, STRING, STRING, INTEGER, INTEGER);
        addPushDownFunction(EXASOL, LTRIM, STRING, STRING);
        addPushDownFunction(EXASOL, LTRIM, STRING, STRING, STRING);
        addPushDownFunction(EXASOL, MID, STRING, STRING, INTEGER);
        addPushDownFunction(EXASOL, MID, STRING, STRING, INTEGER, INTEGER);
        addPushDownFunction(EXASOL, OCTET_LENGTH, INTEGER, STRING);
        addPushDownFunction(EXASOL, REGEXP_INSTR, INTEGER, STRING, STRING);
        addPushDownFunction(EXASOL, REGEXP_INSTR, INTEGER, STRING, STRING, INTEGER);
        addPushDownFunction(EXASOL, REGEXP_INSTR, INTEGER, STRING, STRING, INTEGER, INTEGER);
        addPushDownFunction(EXASOL, REGEXP_REPLACE, STRING, STRING, STRING);
        addPushDownFunction(EXASOL, REGEXP_REPLACE, STRING, STRING, STRING, STRING);
        addPushDownFunction(EXASOL, REGEXP_REPLACE, STRING, STRING, STRING, STRING, INTEGER);
        addPushDownFunction(EXASOL, REGEXP_REPLACE, STRING, STRING, STRING, STRING, INTEGER, INTEGER);
        addPushDownFunction(EXASOL, REGEXP_SUBSTR, STRING, STRING, STRING);
        addPushDownFunction(EXASOL, REGEXP_SUBSTR, STRING, STRING, STRING, INTEGER);
        addPushDownFunction(EXASOL, REGEXP_SUBSTR, STRING, STRING, STRING, INTEGER, INTEGER);
        addPushDownFunction(EXASOL, REPLACE, STRING, STRING, STRING);
        addPushDownFunction(EXASOL, REPLACE, STRING, STRING, STRING, STRING);
        addPushDownFunction(EXASOL, REVERSE, STRING, STRING);
        addPushDownFunction(EXASOL, RTRIM, STRING, STRING);
        addPushDownFunction(EXASOL, RTRIM, STRING, STRING, STRING);
        addPushDownFunction(EXASOL, SOUNDEX, STRING, STRING);
        addPushDownFunction(EXASOL, SPACE, STRING, INTEGER);
        addPushDownFunction(EXASOL, TO_NUMBER, DOUBLE, STRING);
        addPushDownFunction(EXASOL, TO_NUMBER, DOUBLE, STRING, STRING);
        addPushDownFunction(EXASOL, UNICODE, INTEGER, CHAR);
        addPushDownFunction(EXASOL, UNICODECHR, CHAR, INTEGER);

        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier(UPPER));
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new ConcatFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.LENGTH, new AliasModifier(CHARACTER_LENGTH));
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier(LOWER));

        //Date
        addPushDownFunction(EXASOL, ADD_DAYS, DATE, DATE, INTEGER);
        addPushDownFunction(EXASOL, ADD_DAYS, TIMESTAMP, TIMESTAMP, INTEGER);
        addPushDownFunction(EXASOL, ADD_HOURS, TIMESTAMP, TIMESTAMP, INTEGER);
        addPushDownFunction(EXASOL, ADD_MINUTES, TIMESTAMP, TIMESTAMP, INTEGER);
        addPushDownFunction(EXASOL, ADD_MONTHS, DATE, DATE, INTEGER);
        addPushDownFunction(EXASOL, ADD_MONTHS, TIMESTAMP, TIMESTAMP, INTEGER);
        addPushDownFunction(EXASOL, ADD_SECONDS, TIMESTAMP, TIMESTAMP, DOUBLE);
        addPushDownFunction(EXASOL, ADD_WEEKS, DATE, DATE, INTEGER);
        addPushDownFunction(EXASOL, ADD_WEEKS, TIMESTAMP, TIMESTAMP, INTEGER);
        addPushDownFunction(EXASOL, ADD_YEARS, DATE, DATE, INTEGER);
        addPushDownFunction(EXASOL, ADD_YEARS, TIMESTAMP, TIMESTAMP, INTEGER);
        addPushDownFunction(EXASOL, DATE_TRUNC, DATE, STRING, DATE);
        addPushDownFunction(EXASOL, DATE_TRUNC, TIMESTAMP, STRING, TIMESTAMP);
        addPushDownFunction(EXASOL, DAYS_BETWEEN, INTEGER, DATE, DATE);
        addPushDownFunction(EXASOL, DAYS_BETWEEN, INTEGER, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(EXASOL, HOURS_BETWEEN, DOUBLE, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(EXASOL, MINUTES_BETWEEN, DOUBLE, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(EXASOL, MONTHS_BETWEEN, DOUBLE, DATE, DATE);
        addPushDownFunction(EXASOL, MONTHS_BETWEEN, DOUBLE, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(EXASOL, POSIX_TIME, DOUBLE, TIMESTAMP);
        addPushDownFunction(EXASOL, ROUND, DATE, DATE);
        addPushDownFunction(EXASOL, ROUND, DATE, DATE, STRING);
        addPushDownFunction(EXASOL, ROUND, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(EXASOL, ROUND, TIMESTAMP, TIMESTAMP, STRING);
        addPushDownFunction(EXASOL, SECONDS_BETWEEN, DOUBLE, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(EXASOL, TRUNC, DATE, DATE);
        addPushDownFunction(EXASOL, TRUNC, DATE, DATE, STRING);
        addPushDownFunction(EXASOL, TRUNC, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(EXASOL, TRUNC, TIMESTAMP, TIMESTAMP, STRING);
        addPushDownFunction(EXASOL, TRUNCATE, DATE, DATE);
        addPushDownFunction(EXASOL, TRUNCATE, DATE, DATE, STRING);
        addPushDownFunction(EXASOL, TRUNCATE, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(EXASOL, TRUNCATE, TIMESTAMP, TIMESTAMP, STRING);
        addPushDownFunction(EXASOL, YEARS_BETWEEN, DOUBLE, DATE, DATE);
        addPushDownFunction(EXASOL, YEARS_BETWEEN, DOUBLE, TIMESTAMP, TIMESTAMP);

        registerFunctionModifier(SourceSystemFunctions.FROM_UNIXTIME, new AliasModifier(FROM_POSIX_TIME));
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier(DAY));
        registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new AliasModifier(TO_CHAR));
        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new AliasModifier(TO_TIMESTAMP));

        //Convert
        ConvertModifier convertModifier = new ConvertModifier();

        convertModifier.addTypeMapping("VARCHAR(4000)", FunctionModifier.STRING);
        convertModifier.addTypeMapping("DECIMAL(3)", FunctionModifier.BYTE);
        convertModifier.addTypeMapping("DECIMAL(5)", FunctionModifier.SHORT);
        convertModifier.addTypeMapping("DECIMAL(18)", FunctionModifier.INTEGER);
        convertModifier.addTypeMapping("DECIMAL(18)", FunctionModifier.LONG);
        convertModifier.addTypeMapping("DOUBLE PRECISION", FunctionModifier.FLOAT);
        convertModifier.addTypeMapping("DOUBLE PRECISION", FunctionModifier.DOUBLE);
        convertModifier.addTypeMapping("DECIMAL(36)", FunctionModifier.BIGINTEGER);
        convertModifier.addTypeMapping("DECIMAL(36,18)", FunctionModifier.BIGDECIMAL);

        convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", DATE_FORMAT));
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", TIMESTAMP_FORMAT));
        convertModifier.addNumericBooleanConversions();
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.DOUBLE, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(", function.getParameters().get(0), " as double precision)");
            }
        });
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.BYTE, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(", function.getParameters().get(0), " as decimal(3))");
            }
        });

        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();

        supportedFunctions.addAll(super.getSupportedFunctions());

        //Numeric
        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.COT);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TAN);

        //Bitwise
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);
        supportedFunctions.add(SourceSystemFunctions.BITNOT);

        //String
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.CHAR);
        supportedFunctions.add(SourceSystemFunctions.INSERT);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.TRANSLATE);
        supportedFunctions.add(SourceSystemFunctions.UCASE);

        //Date
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.FROM_UNIXTIME);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.NOW);
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.YEAR);

        //Convert, see supportsConvert for excluded types
        supportedFunctions.add(SourceSystemFunctions.CONVERT);

        return supportedFunctions;
    }

    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex,
            Class<?> expectedType) throws SQLException {
        if (expectedType == TypeFacility.RUNTIME_TYPES.CLOB) {
            expectedType = TypeFacility.RUNTIME_TYPES.STRING;
        }
        return super.retrieveValue(results, parameterIndex, expectedType);
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
            Class<?> expectedType) throws SQLException {
        if (expectedType == TypeFacility.RUNTIME_TYPES.CLOB) {
            expectedType = TypeFacility.RUNTIME_TYPES.STRING;
        }
        return super.retrieveValue(results, columnIndex, expectedType);
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        if (fromType == RUNTIME_CODES.BYTE || fromType == RUNTIME_CODES.OBJECT || fromType == RUNTIME_CODES.VARBINARY || fromType == RUNTIME_CODES.TIME || fromType == RUNTIME_CODES.BLOB || fromType == RUNTIME_CODES.XML
                || toType == RUNTIME_CODES.BYTE || toType == RUNTIME_CODES.OBJECT || toType == RUNTIME_CODES.VARBINARY || toType == RUNTIME_CODES.TIME || toType == RUNTIME_CODES.BLOB || toType == RUNTIME_CODES.XML) {
            return false;
        }
        return true;
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.HIGH;
    }

    @Override
    public boolean supportsOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByNullOrdering() {
        return true;
    }

    @Override
    public boolean supportsSearchedCaseExpressions() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    @Override
    public boolean supportsIntersect() {
        return true;
    }

    @Override
    public boolean supportsExcept() {
        return true;
    }

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }

    @Override
    public boolean supportsGroupByRollup() {
        return true;
    }

    @Override
    public boolean supportsArrayType() {
        return true;
    }

    @Override
    public boolean supportsCommonTableExpressions() {
        return true;
    }

    @Override
    public boolean supportsBatchedUpdates() {
        return false;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return true;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaOrdered() {
        return true;
    }

    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }
}