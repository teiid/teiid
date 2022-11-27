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
package org.teiid.translator.jdbc.vertica;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.AggregateFunction;
import org.teiid.language.LanguageObject;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;

@Translator(name="vertica", description="A translator for read/write HP Vertica Analytic Database Server")
public class VerticaExecutionFactory extends JDBCExecutionFactory{

    public static final String VERTICA = "vertica"; //$NON-NLS-1$

    public static final String BIT_LENGTH = "BIT_LENGTH"; //$NON-NLS-1$
    public static final String BITCOUNT = "BITCOUNT"; //$NON-NLS-1$
    public static final String BITSTRING_TO_BINARY = "BITSTRING_TO_BINARY"; //$NON-NLS-1$
    public static final String BTRIM = "BTRIM"; //$NON-NLS-1$
    public static final String CHR = "CHR"; //$NON-NLS-1$
    public static final String GREATEST = "GREATEST"; //$NON-NLS-1$
    public static final String GREATESTB = "GREATESTB"; //$NON-NLS-1$
    public static final String HEX_TO_BINARY = "HEX_TO_BINARY"; //$NON-NLS-1$
    public static final String HEX_TO_INTEGER = "HEX_TO_INTEGER"; //$NON-NLS-1$
    public static final String INITCAP = "INITCAP"; //$NON-NLS-1$
    public static final String INSERT = "INSERT"; //$NON-NLS-1$
    public static final String ISUTF8 = "ISUTF8"; //$NON-NLS-1$
    public static final String LOWER = "LOWER"; //$NON-NLS-1$
    public static final String MD5 = "MD5"; //$NON-NLS-1$
    public static final String SPACE = "SPACE"; //$NON-NLS-1$
    public static final String TO_HEX = "TO_HEX"; //$NON-NLS-1$
    public static final String UPPER = "UPPER"; //$NON-NLS-1$

    public static final String CBRT = "CBRT"; //$NON-NLS-1$
    public static final String LN = "LN"; //$NON-NLS-1$
    public static final String PI = "PI"; //$NON-NLS-1$
    public static final String RANDOM = "RANDOM"; //$NON-NLS-1$
    public static final String TRUNC = "TRUNC"; //$NON-NLS-1$

    public static final String ADD_MONTHS = "ADD_MONTHS"; //$NON-NLS-1$
    public static final String AGE_IN_MONTHS = "AGE_IN_MONTHS"; //$NON-NLS-1$
    public static final String AGE_IN_YEARS = "AGE_IN_YEARS"; //$NON-NLS-1$
    public static final String CURRENT_DATE = "CURRENT_DATE"; //$NON-NLS-1$
    public static final String CURRENT_TIME = "CURRENT_TIME"; //$NON-NLS-1$
    public static final String WEEK_ISO = "WEEK_ISO"; //$NON-NLS-1$
    public static final String DATE_NAME = "DATE"; //$NON-NLS-1$
    public static final String DATEDIFF = "DATEDIFF"; //$NON-NLS-1$
    public static final String DAY = "DAY"; //$NON-NLS-1$
    public static final String GETDATE = "GETDATE"; //$NON-NLS-1$
    public static final String GETUTCDATE = "GETUTCDATE"; //$NON-NLS-1$
    public static final String ISFINITE = "ISFINITE"; //$NON-NLS-1$
    public static final String LOCALTIME = "LOCALTIME"; //$NON-NLS-1$
    public static final String LOCALTIMESTAMP = "LOCALTIMESTAMP"; //$NON-NLS-1$
    public static final String MONTHS_BETWEEN = "MONTHS_BETWEEN"; //$NON-NLS-1$
    public static final String OVERLAPS = "OVERLAPS"; //$NON-NLS-1$
    public static final String TIMESTAMPDIFF = "TIMESTAMPDIFF"; //$NON-NLS-1$

    @Override
    public void start() throws TranslatorException {
        super.start();

        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier(CHR));
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier(LOWER));
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier(UPPER));

        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier(CURRENT_DATE));
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier(CURRENT_TIME));
        registerFunctionModifier(SourceSystemFunctions.WEEK, new AliasModifier(WEEK_ISO));

        addPushDownFunction(VERTICA, BIT_LENGTH, INTEGER, STRING);
        addPushDownFunction(VERTICA, BITCOUNT, BYTE, BYTE);
        addPushDownFunction(VERTICA, BITSTRING_TO_BINARY, BYTE, STRING);
        addPushDownFunction(VERTICA, BTRIM, STRING, STRING, STRING);
        addPushDownFunction(VERTICA, GREATEST, OBJECT, OBJECT);
        addPushDownFunction(VERTICA, GREATESTB, BYTE, OBJECT);
        addPushDownFunction(VERTICA, HEX_TO_BINARY, BYTE, STRING);
        addPushDownFunction(VERTICA, HEX_TO_INTEGER, INTEGER, STRING);
        addPushDownFunction(VERTICA, INITCAP, STRING, STRING);
        addPushDownFunction(VERTICA, INSERT, STRING, STRING, INTEGER, INTEGER, STRING);
        addPushDownFunction(VERTICA, ISUTF8, BOOLEAN, STRING);
        addPushDownFunction(VERTICA, MD5, STRING, STRING);
        addPushDownFunction(VERTICA, SPACE, STRING, INTEGER);
        addPushDownFunction(VERTICA, TO_HEX, STRING, INTEGER);

        addPushDownFunction(VERTICA, CBRT, DOUBLE, DOUBLE);
        addPushDownFunction(VERTICA, LN, DOUBLE, DOUBLE);
        addPushDownFunction(VERTICA, PI, DOUBLE);
        addPushDownFunction(VERTICA, RANDOM, FLOAT);
        addPushDownFunction(VERTICA, TRUNC, DOUBLE, DOUBLE);

        addPushDownFunction(VERTICA, ADD_MONTHS, DATE, DATE, INTEGER);
        addPushDownFunction(VERTICA, AGE_IN_MONTHS, INTEGER, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(VERTICA, AGE_IN_YEARS, INTEGER, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(VERTICA, DATE_NAME, DATE, OBJECT);
        addPushDownFunction(VERTICA, DATEDIFF, INTEGER, STRING, DATE, DATE);
        addPushDownFunction(VERTICA, DAY, INTEGER, OBJECT);
        addPushDownFunction(VERTICA, GETDATE, TIMESTAMP);
        addPushDownFunction(VERTICA, GETUTCDATE, TIMESTAMP);
        addPushDownFunction(VERTICA, ISFINITE, BOOLEAN, TIMESTAMP);
        addPushDownFunction(VERTICA, LOCALTIME, TIME);
        addPushDownFunction(VERTICA, LOCALTIMESTAMP, TIMESTAMP);
        addPushDownFunction(VERTICA, MONTHS_BETWEEN, INTEGER, DATE, DATE);
        addPushDownFunction(VERTICA, OVERLAPS, BOOLEAN, DATE, DATE);
        addPushDownFunction(VERTICA, TIMESTAMPDIFF, INTEGER, TIMESTAMP, TIMESTAMP);

    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.CHAR);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.UCASE);

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
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TAN);

        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
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

        return supportedFunctions;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsIntersect() {
        return true;
    }

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof AggregateFunction) {
            AggregateFunction agg = (AggregateFunction)obj;
            if (agg.getParameters().size() == 1
                    && (agg.getName().equalsIgnoreCase(NonReserved.MIN) || agg.getName().equalsIgnoreCase(NonReserved.MAX))
                    && TypeFacility.RUNTIME_TYPES.BOOLEAN.equals(agg.getParameters().get(0).getType())) {
                return Arrays.asList("CAST(", agg.getName(), "(CAST(", agg.getParameters().get(0), " AS tinyint)) AS boolean)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    }
        }
        return super.translate(obj, context);
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.UNKNOWN; //varies by type
    }

}
