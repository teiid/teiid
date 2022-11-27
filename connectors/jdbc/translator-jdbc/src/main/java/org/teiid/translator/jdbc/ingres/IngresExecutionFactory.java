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
package org.teiid.translator.jdbc.ingres;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Limit;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
@Translator(name="ingres", description="A translator for Ingres Databases")
public class IngresExecutionFactory extends JDBCExecutionFactory {

    private static final String INGRES = "ingres"; //$NON-NLS-1$
    protected ConvertModifier convert = new ConvertModifier();

    @Override
    public void start() throws TranslatorException {
        super.start();
        convert.addTypeMapping("tinyint", FunctionModifier.BOOLEAN, FunctionModifier.BYTE); //$NON-NLS-1$
        convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
        convert.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
        convert.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
        convert.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
        convert.addTypeMapping("float", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convert.addTypeMapping("decimal(38,19)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convert.addTypeMapping("decimal(15,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convert.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        convert.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
        convert.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convert.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
        convert.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
        convert.addTypeMapping("blob", FunctionModifier.BLOB); //$NON-NLS-1$
        convert.addTypeMapping("clob", FunctionModifier.CLOB); //$NON-NLS-1$
        convert.addNumericBooleanConversions();
        convert.setWideningNumericImplicit(true);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);

        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("bit_and")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("bit_not")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("bit_or")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("bit_xor")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("current_time")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("current_date")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lowercase")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("uppercase")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$

        addPushDownFunction(INGRES, "bit_add", INTEGER, INTEGER, INTEGER); //$NON-NLS-1$
        addPushDownFunction(INGRES, "bit_length", INTEGER, INTEGER); //$NON-NLS-1$
        addPushDownFunction(INGRES, "character_length", STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(INGRES, "charextract", CHAR, STRING, INTEGER); //$NON-NLS-1$

            // uses ingres date??
            //supportedFunctions.add("date_trunc");
            //supportedFunctions.add("dow");
            //supportedFunctions.add("extract");
        addPushDownFunction(INGRES, "gmt_timestamp", STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(INGRES, "hash", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(INGRES, "hex", STRING, STRING); //$NON-NLS-1$

            // do not have byte[] type
            //supportedFunctions.add("intextract");

        addPushDownFunction(INGRES, "ln", DOUBLE, DOUBLE); //$NON-NLS-1$

            // see lowercase
            // supportedFunctions.add("lower");
            // supportedFunctions.add("upper");

        addPushDownFunction(INGRES, "octet_length", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(INGRES, "randomf", FLOAT); //$NON-NLS-1$
        addPushDownFunction(INGRES, "session_user", STRING); //$NON-NLS-1$
        addPushDownFunction(INGRES, "size", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(INGRES, "squeeze", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(INGRES, "soundex", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(INGRES, "unhex", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(INGRES, "usercode", STRING); //$NON-NLS-1$
        addPushDownFunction(INGRES, "username", STRING); //$NON-NLS-1$
            // ignore
            // supportedFunctions.add("uuid_create");
            // supportedFunctions.add("uuid_compare");
            // supportedFunctions.add("uuid_from_char");
            // supportedFunctions.add("uuid_to_char");

    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        supportedFunctions.add(SourceSystemFunctions.BITNOT);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        supportedFunctions.add(SourceSystemFunctions.UCASE);

        return supportedFunctions;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        return Arrays.asList("FETCH FIRST ", limit.getRowLimit(), " ROWS ONLY"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralDate(Date dateValue) {
        return "DATE '" + formatDateValue(dateValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME '" + formatDateValue(timeValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "TIMESTAMP '" + formatDateValue(timestampValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.LAST;
    }

    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    protected boolean supportsBooleanExpressions() {
        return false;
    }

}
