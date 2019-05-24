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
package org.teiid.translator.phoenix;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DATE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.OBJECT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.TIME;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.TIMESTAMP;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.DerivedColumn;
import org.teiid.language.DerivedTable;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.Select;
import org.teiid.language.SetQuery;
import org.teiid.language.SubqueryComparison;
import org.teiid.language.SubqueryIn;
import org.teiid.language.TableReference;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.SubqueryComparison.Quantifier;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;
import org.teiid.translator.jdbc.JDBCUpdateExecution;
import org.teiid.util.Version;

@Translator(name="phoenix", description="A translator for Phoenix/HBase")
public class PhoenixExecutionFactory extends JDBCExecutionFactory{

    public static String PHOENIX = "phoenix"; //$NON-NLS-1$
    public static final Version V_4_8 = Version.getVersion("4.8"); //$NON-NLS-1$

    @Override
    public void start() throws TranslatorException {

        super.start();

        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("SUBSTR")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UPPER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("LOWER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new AliasModifier("INSTR")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new AliasModifier("TO_TIMESTAMP")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("CURRENT_TIME")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("LN")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG10, new AliasModifier("LOG")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.PARSEBIGDECIMAL, new AliasModifier("TO_NUMBER")); //$NON-NLS-1$

        addPushDownFunction(PHOENIX, "REVERSE", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "REGEXP_SUBSTR", STRING, STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "REGEXP_REPLACE", STRING, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "REGEXP_SPLIT", OBJECT, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "TO_DATE", DATE, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "TO_TIME", TIME, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "TIMEZONE_OFFSET", INTEGER, STRING, DATE); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "TIMEZONE_OFFSET", INTEGER, STRING, TIME); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "TIMEZONE_OFFSET", INTEGER, STRING, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "CONVERT_TZ", DATE, DATE, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(PHOENIX, "CONVERT_TZ", TIME, TIME, STRING, STRING); //$NON-NLS-1$
    }

    @Override
    public List<String> getSupportedFunctions() {

        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.LCASE);

        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.NOW);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.SECOND);

        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.PARSEBIGDECIMAL);

        return supportedFunctions;
    }

    @Override
    public void initCapabilities(Connection connection) throws TranslatorException {

        super.initCapabilities(connection);

        // https://phoenix.apache.org/joins.html
        if(getVersion().compareTo(V_4_8) >= 0) {
            setSupportsInnerJoins(true);
            setSupportsOuterJoins(true);
            setSupportsFullOuterJoins(true);
        }
    }

    @Override
    public JDBCUpdateExecution createUpdateExecution(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            Connection conn) throws TranslatorException {
        return new PhoenixUpdateExecution(command, executionContext, metadata, conn, this);
    }

    public PhoenixSQLConversionVisitor getSQLConversionVisitor() {
        return new PhoenixSQLConversionVisitor(this);
    }

    @Override
    public void bindValue(PreparedStatement pstmt, Object param, Class<?> paramType, int i) throws SQLException {

        int type = TypeFacility.getSQLTypeFromRuntimeType(paramType);

        if (param == null) {
            pstmt.setNull(i, type);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.STRING)) {
            pstmt.setString(i, String.valueOf(param));
            return;
        }

        if (paramType.equals(TypeFacility.RUNTIME_TYPES.VARBINARY)) {
            byte[] bytes ;
            if(param instanceof BinaryType){
                bytes = ((BinaryType)param).getBytesDirect();
            } else {
                bytes = (byte[]) param;
            }
            pstmt.setBytes(i, bytes);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.CHAR)) {
            pstmt.setString(i, String.valueOf(param));
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN)) {
            pstmt.setBoolean(i, (Boolean)param);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.BYTE)) {
            pstmt.setByte(i, (Byte)param);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.SHORT)) {
            pstmt.setShort(i, (Short)param);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.INTEGER)) {
            pstmt.setInt(i, (Integer)param);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.LONG)) {
            pstmt.setLong(i, (Long)param);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.FLOAT)) {
            pstmt.setFloat(i, (Float)param);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.DOUBLE)) {
            pstmt.setDouble(i, (Double)param);
            return;
        }

        if(paramType.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
            pstmt.setBigDecimal(i, (BigDecimal)param);
            return;
        }

        if (paramType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
            pstmt.setDate(i,(java.sql.Date)param, getDatabaseCalendar());
            return;
        }
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            pstmt.setTime(i,(java.sql.Time)param, getDatabaseCalendar());
            return;
        }
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            pstmt.setTimestamp(i,(java.sql.Timestamp)param, getDatabaseCalendar());
            return;
        }

        if (useStreamsForLobs()) {
            // Phonix current not support Blob, Clob, XML
        }

        pstmt.setObject(i, param, type);
    }

    @Override
    public boolean supportsInsertWithQueryExpression() {
        return true;
    }

    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "true"; //$NON-NLS-1$
        }
        return "false"; //$NON-NLS-1$
    }

    /**
     * Adding a specific workaround for just Pheonix and BigDecimal.
     */
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof SubqueryIn) {
            SubqueryIn in = (SubqueryIn)obj;
            return Arrays.asList(new SubqueryComparison(in.getLeftExpression(),
                    in.isNegated()?Operator.NE:Operator.EQ,
                            in.isNegated()?Quantifier.ALL:Quantifier.SOME, in.getSubquery()));
        }
        if (!(obj instanceof Literal)) {
            return super.translate(obj, context);
        }
        Literal l = (Literal)obj;
        if (l.isBindEligible() || l.getType() != TypeFacility.RUNTIME_TYPES.BIG_DECIMAL) {
            return super.translate(obj, context);
        }
        BigDecimal bd = ((BigDecimal)l.getValue());
        if (bd.scale() == 0) {
            l.setValue(bd.setScale(1));
        }
        return null;
    }

    /**
     * It doesn't appear that the time component is needed, but for consistency with their
     * documentation, we'll add it.
     */
    @Override
    public String translateLiteralDate(java.sql.Date dateValue) {
        return "DATE '" + formatDateValue(new Timestamp(dateValue.getTime())) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * A date component is required, so create a new Timestamp instead
     */
    @Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME '" + formatDateValue(new Timestamp(timeValue.getTime())) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "TIMESTAMP '" + formatDateValue(timestampValue) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * The Phoenix driver has issues using a calendar object.
     * it throws an npe on a null value and also has https://issues.apache.org/jira/browse/PHOENIX-869
     */
    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
            Class<?> expectedType) throws SQLException {
        Integer code = DataTypeManager.getTypeCode(expectedType);
        if(code != null) {
            switch (code) {
                case DataTypeManager.DefaultTypeCodes.TIME: {
                    return results.getTime(columnIndex);
                }
                case DataTypeManager.DefaultTypeCodes.DATE: {
                    return results.getDate(columnIndex);
                }
                case DataTypeManager.DefaultTypeCodes.TIMESTAMP: {
                    return results.getTimestamp(columnIndex);
                }
            }
        }
        return super.retrieveValue(results, columnIndex, expectedType);
    }

    @Override
    protected JDBCMetadataProcessor createMetadataProcessor() {
        JDBCMetadataProcessor processor = new JDBCMetadataProcessor() {
            @Override
            protected boolean getIndexInfoForTable(String catalogName,
                    String schemaName, String tableName, boolean uniqueOnly,
                    boolean approximateIndexes, String tableType) {
                //unique returns an empty result set that is not reusable
                return !uniqueOnly;
            }
        };
        //same issue with foreign keys
        processor.setImportForeignKeys(false);
        return processor;
    }

    @Override
    public Character getRequiredLikeEscape() {
        return '\\';
    }

    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
        if (command instanceof SetQuery) {
            SetQuery set = (SetQuery)command;
            if (!set.isAll()) {
                //distinct is not supported, convert to an inline view and add distinct
                Select s = new Select();
                s.setDistinct(true);
                s.setDerivedColumns(new ArrayList<DerivedColumn>());
                s.setOrderBy(set.getOrderBy());
                for (DerivedColumn dc : set.getProjectedQuery().getDerivedColumns()) {
                    Assertion.assertTrue(dc.getAlias() != null); //it's expected that the columns will be aliases
                    ColumnReference cr = new ColumnReference(null, dc.getAlias(), null, dc.getExpression().getType());
                    s.getDerivedColumns().add(new DerivedColumn(null, cr));
                }
                set.setOrderBy(null);
                s.setLimit(set.getLimit());
                set.setLimit(null);
                set.setAll(true);
                s.setFrom(Arrays.asList((TableReference)new DerivedTable(set, "x"))); //$NON-NLS-1$
                return Arrays.asList(s);
            }
        }
        return super.translateCommand(command, context);
    }

    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {

        if(limit.getRowOffset() > 0) {
            return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        return super.translateLimit(limit, context);
    }


    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsScalarSubqueryProjection() {
        return false; //not supported in the select clause
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public boolean supportsRowOffset() {
        return getVersion().compareTo(V_4_8) >= 0;
    }
}
