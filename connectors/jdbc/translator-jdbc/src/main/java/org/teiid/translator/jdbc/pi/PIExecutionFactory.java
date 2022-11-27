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

package org.teiid.translator.jdbc.pi;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.SQLConversionVisitor;

@Translator(name="osisoft-pi", description="A translator for OsiSoft PI database")
public class PIExecutionFactory extends JDBCExecutionFactory {
    public static String PI = "pi"; //$NON-NLS-1$
    protected ConvertModifier convert = new ConvertModifier();

    public PIExecutionFactory() {
        setUseBindVariables(false);
        setSupportsFullOuterJoins(false);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();

        convert.addTypeMapping("Int8", FunctionModifier.BYTE); //$NON-NLS-1$
        convert.addTypeMapping("Int16", FunctionModifier.SHORT); //$NON-NLS-1$
        convert.addTypeMapping("Int32", FunctionModifier.INTEGER); //$NON-NLS-1$
        convert.addTypeMapping("Int64", FunctionModifier.LONG); //$NON-NLS-1$
        convert.addTypeMapping("Single", FunctionModifier.FLOAT); //$NON-NLS-1$
        convert.addTypeMapping("Double", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convert.addTypeMapping("Boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convert.addTypeMapping("String", FunctionModifier.STRING); //$NON-NLS-1$
        convert.addTypeMapping("DateTime", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convert.addTypeMapping("Time", FunctionModifier.TIME); //$NON-NLS-1$
        convert.addTypeMapping("Variant", FunctionModifier.OBJECT); //$NON-NLS-1$

        convert.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(format(",function.getParameters().get(0), ", 'hh:mm:ss.fff') as Time)"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        convert.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.FLOAT, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(cast(",function.getParameters().get(0), " as int8) as single)"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        convert.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.DOUBLE, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(cast(",function.getParameters().get(0), " as int8) as double)"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });

        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);
        registerFunctionModifier(SourceSystemFunctions.MOD, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(", function.getParameters().get(0), " as int64)", "%", //$NON-NLS-1$ //$NON-NLS-2$
                        "cast(",function.getParameters().get(1), " as int64)");
            }
        }); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("DAY")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                if (function.getParameters().size() <= 2) {
                    return Arrays.asList("INSTR(",function.getParameters().get(1), ",", function.getParameters().get(0), ")");
                }
                return Arrays.asList("INSTR(", function.getParameters().get(1), ",", function.getParameters().get(0),
                        ",", function.getParameters().get(2), ")");
            }
        });
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("LOWER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UPPER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("SUBSTR")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LENGTH, new AliasModifier("LEN")); //$NON-NLS-1$

        addPushDownFunction(PI, "COSH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(PI, "TANH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(PI, "SINH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(PI, "FORMAT", STRING, FLOAT, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "FORMAT", STRING, INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "ParentName", STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(PI, "List", STRING, STRING) //$NON-NLS-1$
            .setVarArgs(true);
        addPushDownFunction(PI, "DIGCODE", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "DIGSTRING", STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(PI, "PE", STRING, OBJECT); //$NON-NLS-1$

        addPushDownFunction(PI, "ParentName", STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(PI, "VarType", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "UOMID", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "UOMName", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "UOMAbbreviation", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "UOMClassName", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "UOMCanonicallD", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PI, "UOMConvert", DOUBLE, DOUBLE, STRING, STRING); //$NON-NLS-1$
        FunctionMethod f = addPushDownFunction(PI, "interval", TIMESTAMP, STRING); //$NON-NLS-1$
        f.setProperty(SQLConversionVisitor.TEIID_NATIVE_QUERY, "$1"); //$NON-NLS-1$
    }

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }

    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
        return true;
    }

    @Override
    public boolean supportsInsertWithQueryExpression() {
        return false;
    }

    @Override
    public boolean supportsBatchedUpdates() {
        return false;
    }

    @Override
    public boolean supportsBulkUpdate() {
        return false;
    }

    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        return Arrays.asList("TOP ", limit.getRowLimit()); //$NON-NLS-1$
    }

    @Override
    public boolean useSelectLimit() {
        return true;
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);
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
    public String translateLiteralDate(Date dateValue) {
        return "'" + formatDateValue(dateValue) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "'" + formatDateValue(timeValue) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "'" + formatDateValue(timestampValue) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public void getMetadata(MetadataFactory metadataFactory, Connection conn) throws TranslatorException {
        if (metadataFactory.getModelProperties().get("importer.ImportKeys") == null) {
            metadataFactory.getModelProperties().put("importer.ImportKeys", "false");
        }
        super.getMetadata(metadataFactory, conn);
    }

    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new PIMetadataProcessor();
    }

    public boolean useAsInGroupAlias(){
        return true;
    }

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof DerivedColumn) {
            DerivedColumn derived = (DerivedColumn)obj;
            if (derived.isProjected() && derived.getExpression() instanceof ColumnReference) {
                ColumnReference elem = (ColumnReference)derived.getExpression();
                if (elem.getMetadataObject() != null) {
                    String nativeType = elem.getMetadataObject().getNativeType();
                    if (nativeType != null && TypeFacility.RUNTIME_TYPES.STRING.equals(elem.getType())
                            && PIMetadataProcessor.guidPattern.matcher(nativeType).find()) {
                        return Arrays.asList("cast(", elem, " as String)"); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                }
            }
        }
        return super.translate(obj, context);
    }

    @Override
    public SQLConversionVisitor getSQLConversionVisitor() {
        return new PISQLConversionVisitor(this);
    }

    /**
     *
     * @return true if the source supports lateral join
     */
    public boolean supportsLateralJoin() {
        return true;
    }

    /**
     *
     * @return true if the source supports lateral join conditions
     */
    public boolean supportsLateralJoinCondition() {
        return false;
    }

    @Override
    public boolean supportsOnlyLateralJoinProcedure() {
        return true;
    }

    /**
     *
     * @return
     */
    public boolean supportsProcedureTable() {
        return true;
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException {
        return results.getObject(columnIndex);
    }

    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex, Class<?> expectedType)
            throws SQLException {
        Object result = results.getObject(parameterIndex);
        if (result == null) {
            return null;
        }
        return super.retrieveValue(results, parameterIndex, expectedType);
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        if (fromType != TypeFacility.RUNTIME_CODES.OBJECT && !super.supportsConvert(fromType, toType)) {
            return false;
        }
        if (convert.hasTypeMapping(toType)) {
            return true;
        }
        return false;
    }
}
