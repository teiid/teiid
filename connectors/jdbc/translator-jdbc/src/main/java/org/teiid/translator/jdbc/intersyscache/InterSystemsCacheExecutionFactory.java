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
package org.teiid.translator.jdbc.intersyscache;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Function;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;

@Translator(name="intersystems-cache", description="A translator for Intersystems Cache Database")
public class InterSystemsCacheExecutionFactory extends JDBCExecutionFactory {

    private static final String INTER_CACHE = "intersystems-cache"; //$NON-NLS-1$
    protected ConvertModifier convert = new ConvertModifier();

    @Override
    public void start() throws TranslatorException {
        super.start();
        convert.addTypeMapping("tinyint", FunctionModifier.BYTE); //$NON-NLS-1$
        convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
        convert.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
        convert.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
        convert.addTypeMapping("decimal(38,19)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convert.addTypeMapping("decimal(19,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convert.addTypeMapping("character", FunctionModifier.CHAR); //$NON-NLS-1$
        convert.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
        convert.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        convert.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
        convert.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convert.addNumericBooleanConversions();
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);

        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("nvl")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.ACOS, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.ASIN, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.ATAN, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.COS, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.COT, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.EXP, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.HOUR, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.LOG,new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.LOG10, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.LEFT, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.MONTH, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.MOD, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.NOW, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.PI, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.SIN, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.SECOND, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.SQRT,new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TAN, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TRUNCATE, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.WEEK, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DIVIDE_OP, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                if (function.getType() == TypeFacility.RUNTIME_TYPES.INTEGER || function.getType() == TypeFacility.RUNTIME_TYPES.LONG) {
                    Function result = ConvertModifier.createConvertFunction(getLanguageFactory(), function, TypeFacility.getDataTypeName(function.getType()));
                    function.setType(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL);
                    return Arrays.asList(result);
                }
                return null;
            }
        });

        addPushDownFunction(INTER_CACHE, "CHARACTER_LENGTH", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "CHAR_LENGTH", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "CHARINDEX", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "CHARINDEX", INTEGER, STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "INSTR", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "INSTR", INTEGER, STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "IS_NUMERIC", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "REPLICATE", STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "REVERSE", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "STUFF", STRING, STRING, STRING, INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(INTER_CACHE, "TRIM", STRING, STRING); //$NON-NLS-1$
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.CHAR);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.COT);
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.DAYNAME);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.MONTHNAME);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);
        supportedFunctions.add(SourceSystemFunctions.NOW);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.QUARTER);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TAN);
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPADD);
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPDIFF);
        supportedFunctions.add(SourceSystemFunctions.TRUNCATE);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.XMLCONCAT);
        supportedFunctions.add(SourceSystemFunctions.WEEK);

        return supportedFunctions;
    }

    @Override
    public String translateLiteralDate(Date dateValue) {
        return "to_date('" + formatDateValue(dateValue) + "', 'yyyy-mm-dd')"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "to_date('" + formatDateValue(timeValue) + "', 'hh:mi:ss')"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "to_timestamp('" + formatDateValue(timestampValue) + "', 'yyyy-mm-dd hh:mi:ss.fffffffff')"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.LAST;
    }

    @Override
    public boolean supportsInlineViews() {
        return true;
    }

}
