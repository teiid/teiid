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

/*
 */
package org.teiid.translator.jdbc.sqlserver;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.StringUtil;
import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.OrderBy;
import org.teiid.language.Parameter;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.SetQuery;
import org.teiid.language.SortSpecification;
import org.teiid.language.WindowFunction;
import org.teiid.language.WindowSpecification;
import org.teiid.language.With;
import org.teiid.language.WithItem;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
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
import org.teiid.translator.jdbc.JDBCMetadataProcessor;
import org.teiid.translator.jdbc.TemplateFunctionModifier;
import org.teiid.translator.jdbc.sybase.SybaseExecutionFactory;
import org.teiid.util.Version;

/**
 * Updated to assume the use of the DataDirect, 2005 driver, or later.
 */
@Translator(name="sqlserver", description="A translator for Microsoft SQL Server Database")
public class SQLServerExecutionFactory extends SybaseExecutionFactory {

    final class SQLServerMetadataProcessor
            extends JDBCMetadataProcessor {
        Pattern PROCEDURE_PATTERN = Pattern.compile("(.*);\\d*"); //$NON-NLS-1$

        @Override
        protected Column addColumn(ResultSet columns, Table table,
                MetadataFactory metadataFactory, int rsColumns)
                throws SQLException {
            Column c = super.addColumn(columns, table, metadataFactory, rsColumns);
            //The ms jdbc driver does not correctly report the auto incremented column
            if (!c.isAutoIncremented() && c.getNativeType() != null && StringUtil.endsWithIgnoreCase(c.getNativeType(), " identity")) { //$NON-NLS-1$
                c.setAutoIncremented(true);
            }
            return c;
        }

        @Override
        protected ResultSet executeSequenceQuery(Connection conn)
                throws SQLException {
            if (getVersion().compareTo(ELEVEN_0) < 0) {
                return null;
            }
            String query = "select db_name() as sequence_catalog, SCHEMA_NAME(schema_id) as sequence_schema, name as sequence_name from sys.sequences" //$NON-NLS-1$
                    + "where db_name() like ? and SCHEMA_NAME(schema_id) like ? and name like ?"; //$NON-NLS-1$
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, getCatalog()==null?"%":getCatalog()); //$NON-NLS-1$
            ps.setString(2, getSchemaPattern()==null?"%":getSchemaPattern()); //$NON-NLS-1$
            ps.setString(3, getSequenceNamePattern()==null?"%":getSequenceNamePattern()); //$NON-NLS-1$
            return ps.executeQuery();
        }

        @Override
        protected String modifyProcedureNameInSource(String nameInSource) {
            Matcher m = PROCEDURE_PATTERN.matcher(nameInSource);
            if (m.matches()) {
                return m.group(1);
            }
            return nameInSource;
        }

        @Override
        protected String getRuntimeType(int type, String typeName,
                int precision) {
            //TEIID-5949 - without this we default to object for datatimeoffet
            //conversion to timestamp looses the offset
            if ("datetimeoffset".equalsIgnoreCase(typeName)) { //$NON-NLS-1$
                return TypeFacility.RUNTIME_NAMES.STRING;
            }
            return super.getRuntimeType(type, typeName, precision);
        }
    }

    private final class UpperLowerFunctionModifier
            extends AliasModifier {
        private UpperLowerFunctionModifier(String alias) {
            super(alias);
        }

        @Override
        public List<?> translate(Function function) {
            if (function.getParameters().get(0) instanceof ColumnReference) {
                ColumnReference cr = (ColumnReference)function.getParameters().get(0);
                if (cr.getMetadataObject() != null) {
                    String nativeType = cr.getMetadataObject().getNativeType();
                    if (nativeType != null
                            && StringUtil.indexOfIgnoreCase(nativeType, "char") == -1) { //$NON-NLS-1$
                        Function cast = ConvertModifier.createConvertFunction(getLanguageFactory(), cr,
                                        (StringUtil.startsWithIgnoreCase(nativeType.trim(), "n")?"n":"")+"varchar(max)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        cast.setName("cast"); //$NON-NLS-1$
                        function.getParameters().set(0, cast);
                    }
                }
            }
            return super.translate(function);
        }
    }

    public static final String V_2000 = "2000"; //$NON-NLS-1$
    public static final String V_2005 = "2005"; //$NON-NLS-1$
    public static final String V_2008 = "2008"; //$NON-NLS-1$
    public static final String V_2012 = "2012"; //$NON-NLS-1$

    public static final Version SEVEN_0 = Version.getVersion("7.0"); //$NON-NLS-1$
    public static final Version NINE_0 = Version.getVersion("9.0"); //$NON-NLS-1$
    public static final Version TEN_0 = Version.getVersion("10.0"); //$NON-NLS-1$
    public static final Version ELEVEN_0 = Version.getVersion("11.0"); //$NON-NLS-1$

    static final class UniqueIdentifier {}

    //TEIID-31 remove mod modifier for SQL Server 2008
    public SQLServerExecutionFactory() {
        setSupportsFullOuterJoins(true);
        setMaxInCriteriaSize(JDBCExecutionFactory.DEFAULT_MAX_IN_CRITERIA);
        setMaxDependentInPredicates(2);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        registerFunctionModifier(SourceSystemFunctions.WEEK, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("DATEPART(ISO_WEEK, ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new AliasModifier("CHARINDEX")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.MD5, new TemplateFunctionModifier("HASHBYTES('MD5', ", 0, ")")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.SHA1, new TemplateFunctionModifier("HASHBYTES('SHA1', ", 0, ")")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.SHA2_256, new TemplateFunctionModifier("HASHBYTES('SHA2_256', ", 0, ")")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.SHA2_512, new TemplateFunctionModifier("HASHBYTES('SHA2_512', ", 0, ")")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new UpperLowerFunctionModifier("upper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new UpperLowerFunctionModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ASCII, new AliasModifier("unicode")); //$NON-NLS-1$
    }

    @Override
    public void initCapabilities(Connection connection)
            throws TranslatorException {
        super.initCapabilities(connection);
        if (getVersion().compareTo(TEN_0) >= 0) {
            //date support
            convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
            formatMap.put("yyyy-MM-dd", "DATE"); //$NON-NLS-1$ //$NON-NLS-2$
            convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE, new FunctionModifier() {
                @Override
                public List<?> translate(Function function) {
                    List<Object> result = new ArrayList<Object>();
                    result.add("cast("); //$NON-NLS-1$
                    result.add(function.getParameters().get(0));
                    result.add(" AS DATE)"); //$NON-NLS-1$
                    return result;
                }
            });
            //timestamp/datetime2
            convertModifier.addTypeMapping("datetime2", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
            registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new SybaseFormatFunctionModifier("CONVERT(DATETIME2, ", formatMap)); //$NON-NLS-1$
        }
    }

    @Override
    protected void populateDateFormats() {
        formatMap.put("MM/dd/yy", 1); //$NON-NLS-1$
        formatMap.put("yy.MM.dd", 2); //$NON-NLS-1$
        formatMap.put("dd/MM/yy", 3); //$NON-NLS-1$
        formatMap.put("dd.MM.yy", 4); //$NON-NLS-1$
        formatMap.put("dd-MM-yy", 5); //$NON-NLS-1$
        formatMap.put("dd MMM yy", 6); //$NON-NLS-1$
        formatMap.put("MMM dd, yy", 7); //$NON-NLS-1$
        formatMap.put("MM-dd-yy", 10); //$NON-NLS-1$
        formatMap.put("yy/MM/dd", 11); //$NON-NLS-1$
        formatMap.put("yyMMdd", 12); //$NON-NLS-1$
        for (Map.Entry<String, Object> entry : new HashSet<Map.Entry<String, Object>>(formatMap.entrySet())) {
            formatMap.put(entry.getKey().replace("yy", "yyyy"), (Integer)entry.getValue() + 100); //$NON-NLS-1$ //$NON-NLS-2$
        }

        formatMap.put("MMM d yyyy hh:mma", 100); //$NON-NLS-1$
        formatMap.put("HH:mm:ss", 8); //$NON-NLS-1$
        formatMap.put("MMM d yyyy hh:mm:ss:SSSa", 109); //$NON-NLS-1$
        formatMap.put("dd MMM yyyy HH:mm:ss:SSS", 113); //$NON-NLS-1$
        formatMap.put("kk:MM:ss:SSS", 14); //$NON-NLS-1$
        formatMap.put("yyyy-MM-dd HH:mm:ss", 120); //$NON-NLS-1$
        formatMap.put("yyyy-MM-dd HH:mm:ss.SSS", 121); //$NON-NLS-1$
        formatMap.put("yyyy-MM-dd'T'HH:mm:ss.SSS", 126); //$NON-NLS-1$
        //formatMap.put("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", 127); //$NON-NLS-1$
    }

    @Override
    protected List<Object> convertDateToString(Function function) {
        if (getVersion().compareTo(TEN_0) >= 0) {
            return Arrays.asList("convert(varchar, ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return Arrays.asList("replace(convert(varchar, ", function.getParameters().get(0), ", 102), '.', '-')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    protected List<?> convertTimestampToString(Function function) {
        return Arrays.asList("convert(varchar, ", function.getParameters().get(0), ", 21)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (getVersion().compareTo(SEVEN_0) > 0) {
            if (obj instanceof SortSpecification) {
                SortSpecification ss = (SortSpecification)obj;
                if (ss.getExpression() instanceof ColumnReference && isUniqueIdentifier(((ColumnReference)ss.getExpression()))) {
                    ((ColumnReference)ss.getExpression()).setType(UniqueIdentifier.class); //prevent the cast
                }
            }
            if (obj instanceof DerivedColumn) {
                DerivedColumn dc = (DerivedColumn)obj;
                if (dc.getExpression() instanceof ColumnReference && isUniqueIdentifier(((ColumnReference)dc.getExpression()))) {
                    ((ColumnReference)dc.getExpression()).setType(UniqueIdentifier.class); //prevent the cast
                }
            } else if (obj instanceof Comparison) {
                Comparison comp = (Comparison)obj;
                if (comp.getOperator() == Comparison.Operator.EQ
                        && comp.getLeftExpression() instanceof ColumnReference && isUniqueIdentifier((ColumnReference)comp.getLeftExpression())
                        && (comp.getRightExpression() instanceof Literal || comp.getRightExpression() instanceof Parameter)) {
                    ((ColumnReference)comp.getLeftExpression()).setType(UniqueIdentifier.class); //prevent the cast
                }
            } else if (obj instanceof AggregateFunction) {
                AggregateFunction f = (AggregateFunction)obj;
                for (Expression p : f.getParameters()) {
                    if (p instanceof ColumnReference && isUniqueIdentifier(((ColumnReference)p))) {
                        ((ColumnReference)p).setType(UniqueIdentifier.class); //prevent the cast
                    }
                }
            }
        }
        if (obj instanceof ColumnReference) {
            ColumnReference elem = (ColumnReference)obj;
            if (isUniqueIdentifier(elem)) {
                return Arrays.asList("cast(", elem, " as char(36))"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } else if (obj instanceof AggregateFunction) {
            AggregateFunction af = (AggregateFunction)obj;
            if (af.getName().equals(AggregateFunction.STDDEV_POP)) {
                af.setName("STDDEVP"); //$NON-NLS-1$
            } else if (af.getName().equals(AggregateFunction.STDDEV_SAMP)) {
                af.setName("STDDEV"); //$NON-NLS-1$
            } else if (af.getName().equals(AggregateFunction.VAR_POP)) {
                af.setName("VARP"); //$NON-NLS-1$
            } else if (af.getName().equals(AggregateFunction.VAR_SAMP)) {
                af.setName("VAR"); //$NON-NLS-1$
            }
        } else if (obj instanceof WithItem) {
            WithItem withItem = (WithItem)obj;
            //unlike normal unions, recursive cte require additional type handling
            if (withItem.isRecusive()) {
                List<DerivedColumn> derivedColumns = withItem.getSubquery().getProjectedQuery().getDerivedColumns();
                List<DerivedColumn> derivedColumnsRecurse = ((SetQuery)withItem.getSubquery()).getRightQuery().getProjectedQuery().getDerivedColumns();
                for (int i = 0; i < derivedColumns.size(); i++) {
                    String nativeType = null;
                    boolean castLeft = true;
                    boolean castRight = true;
                    DerivedColumn dc = derivedColumns.get(i);
                    if (dc.getExpression() instanceof ColumnReference) {
                        Column c = ((ColumnReference)dc.getExpression()).getMetadataObject();
                        if (c != null && c.getNativeType() != null) {
                            nativeType = c.getNativeType();
                            castLeft = false;
                        }
                    }
                    DerivedColumn dcR = derivedColumnsRecurse.get(i);
                    if (dcR.getExpression() instanceof ColumnReference) {
                        Column c = ((ColumnReference)dcR.getExpression()).getMetadataObject();
                        if (c != null) {
                            if (nativeType == null) {
                                if (c.getNativeType() != null) {
                                    nativeType = c.getNativeType();
                                    castRight = false;
                                }
                            } else {
                                if (nativeType.equals(c.getNativeType())) {
                                    continue; //it matches
                                }
                                //we won't gracefully handle this case, we'll just assume the first type
                            }
                        }
                    }
                    if (castLeft) {
                        addCast(nativeType, dc);
                    }
                    if (castRight) {
                        addCast(nativeType, dcR);
                    }
                }
            }
        }
        return super.translate(obj, context);
    }

    private boolean isUniqueIdentifier(ColumnReference elem) {
        return TypeFacility.RUNTIME_TYPES.STRING.equals(elem.getType()) && elem.getMetadataObject() != null && "uniqueidentifier".equalsIgnoreCase(elem.getMetadataObject().getNativeType()); //$NON-NLS-1$
    }

    private void addCast(String nativeType, DerivedColumn dc) {
        if (nativeType != null) {
            Function cast = ConvertModifier.createConvertFunction(getLanguageFactory(), dc.getExpression(), nativeType);
            cast.setName("cast"); //$NON-NLS-1$
            dc.setExpression(cast);
        } else {
            dc.setExpression(ConvertModifier.createConvertFunction(getLanguageFactory(), dc.getExpression(), TypeFacility.getDataTypeName(dc.getExpression().getType())));
        }
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getDefaultSupportedFunctions());
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        if (getVersion().compareTo(TEN_0) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.COALESCE);
        }
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("PI"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        //supportedFunctons.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        //rand is evaulated deterministically by SQL Server
        //if (getVersion().compareTo(TEN_0) >= 0) {
            //supportedFunctions.add("RAND"); //$NON-NLS-1$
        //}
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SPACE"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        //supportedFunctons.add("TRANSLATE"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        //supportedFunctons.add("CURDATE"); //$NON-NLS-1$
        //supportedFunctons.add("CURTIME"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        //supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL");      //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.FORMATTIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);

        if (getVersion().compareTo(TEN_0) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.SHA2_256);
            supportedFunctions.add(SourceSystemFunctions.SHA2_512);
        }

        if (getVersion().compareTo(NINE_0) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.MD5);
            supportedFunctions.add(SourceSystemFunctions.SHA1);
        }

        return supportedFunctions;
    }

    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
        return true;
    }
    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsRowOffset() {
        return getVersion().compareTo(TEN_0) >= 0;
    }

    @Override
    public boolean supportsIntersect() {
        return true;
    }
    @Override
    public boolean supportsExcept() {
        return true;
    };

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean booleanNullable() {
        return true;
    }

    /**
     * Overridden to allow for year based versions
     */
    @Override
    public void setDatabaseVersion(String version) {
        if (version != null) {
            if (version.equals(V_2000)) {
                setDatabaseVersion(SEVEN_0);
                return;
            } else if (version.equals(V_2005)) {
                setDatabaseVersion(NINE_0);
                return;
            } else if (version.equals(V_2008)) {
                setDatabaseVersion(TEN_0);
                return;
            } else if (version.equals(V_2012)) {
                setDatabaseVersion(ELEVEN_0);
                return;
            }
        }
        super.setDatabaseVersion(version);
    }

    @Override
    public String translateLiteralDate(Date dateValue) {
        if (getVersion().compareTo(TEN_0) >= 0) {
            return super.translateLiteralDate(dateValue);
        }
        return super.translateLiteralTimestamp(new Timestamp(dateValue.getTime()));
    }

    @Override
    public boolean hasTimeType() {
        return getVersion().compareTo(TEN_0) >= 0;
    }

    /**
     * The SQL Server driver maps the time escape to a timestamp/datetime, so
     * use a cast of the string literal instead.
     */
    @Override
    public String translateLiteralTime(Time timeValue) {
        return "cast('" +  formatDateValue(timeValue) + "' as time)"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public boolean supportsCommonTableExpressions() {
        return true;
    }

    @Override
    public boolean supportsSubqueryCommonTableExpressions() {
        return false;
    }

    @Override
    public boolean supportsRecursiveCommonTableExpressions() {
        return getVersion().compareTo(TEN_0) >= 0;
    }

    @Override
    protected boolean supportsCrossJoin() {
        return true;
    }

    @Override
    public boolean supportsElementaryOlapOperations() {
        return true;
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
    public boolean supportsFormatLiteral(String literal,
            org.teiid.translator.ExecutionFactory.Format format) {
        if (format == Format.NUMBER) {
            return false; //TODO: add support
        }
        return formatMap.containsKey(literal);
    }

    @Override
    public boolean supportsOnlyFormatLiterals() {
        return true;
    }

    @Override
    protected boolean setFetchSize() {
        return true;
    }

    @Override
    @Deprecated
    protected JDBCMetadataProcessor createMetadataProcessor() {
        return (JDBCMetadataProcessor)getMetadataProcessor();
    }

    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new SQLServerMetadataProcessor();
    }


    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public boolean useStreamsForLobs() {
        return true;
    }

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }

    @Override
    public String getHibernateDialectClassName() {
        if (getVersion().compareTo(NINE_0) >= 0) {
            if (getVersion().compareTo(TEN_0) >= 0) {
                return "org.hibernate.dialect.SQLServer2008Dialect"; //$NON-NLS-1$
            }
            return "org.hibernate.dialect.SQLServer2005Dialect"; //$NON-NLS-1$
        }
        return "org.hibernate.dialect.SQLServerDialect"; //$NON-NLS-1$
    }

    @Override
    public boolean supportsGroupByRollup() {
        return getVersion().compareTo(NINE_0) >= 0;
    }

    @Override
    public boolean useWithRollup() {
        return getVersion().compareTo(TEN_0) < 0;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        if (fromType == TypeFacility.RUNTIME_CODES.OBJECT && this.convertModifier.hasTypeMapping(toType)) {
            return true;
        }
        return super.supportsConvert(fromType, toType);
    }

    @Override
    public boolean supportsLiteralOnlyWithGrouping() {
        return true;
    }

    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
        if (command instanceof Insert) {
            Insert insert = (Insert)command;
            if (insert.getValueSource() instanceof QueryExpression) {
                QueryExpression qe = (QueryExpression)insert.getValueSource();
                if (qe.getWith() != null) {
                    With with = qe.getWith();
                    qe.setWith(null);
                    return Arrays.asList(with, insert);
                }
            }
        }

        //handle offset support
        boolean useRowNumber = false;
        if (getVersion().compareTo(ELEVEN_0) >= 0 || !(command instanceof QueryExpression)) {
            if (getVersion().compareTo(ELEVEN_0) >= 0 && command instanceof QueryExpression) {
                QueryExpression queryCommand = (QueryExpression)command;
                if (queryCommand.getLimit() != null) {
                    if (queryCommand.getOrderBy() == null) {
                        //an order by is required
                        //we could use top if offset is 0, but that would require contextual knowledge in useSelectLimit
                        if (((queryCommand instanceof Select && ((Select)queryCommand).isDistinct())
                                || (queryCommand instanceof SetQuery && !((SetQuery)queryCommand).isAll()))) {
                            //can't use the @@version with distinct
                            useRowNumber = true;
                        } else {
                            List<Object> parts = new ArrayList<Object>();
                            Limit limit = queryCommand.getLimit();
                            queryCommand.setLimit(null);
                            parts.add(queryCommand);
                            parts.add(" ORDER BY @@version "); //$NON-NLS-1$
                            parts.add(limit);
                            return parts;
                        }
                    } else {
                        List<Object> parts = new ArrayList<Object>();
                        Limit limit = queryCommand.getLimit();
                        queryCommand.setLimit(null);
                        parts.add(queryCommand);
                        parts.add(" "); //$NON-NLS-1$
                        parts.add(limit);
                        return parts;
                    }
                }
            }
            if (!useRowNumber) {
                return super.translateCommand(command, context);
            }
        }
        QueryExpression queryCommand = (QueryExpression)command;
        if (!useRowNumber && (queryCommand.getLimit() == null || queryCommand.getLimit().getRowOffset() == 0)) {
            return super.translateCommand(command, context);
        }
        Limit limit = queryCommand.getLimit();
        queryCommand.setLimit(null);

        List<Object> parts = new ArrayList<Object>();

        if (queryCommand.getWith() != null) {
            With with = queryCommand.getWith();
            queryCommand.setWith(null);
            parts.add(with);
        }

        OrderBy orderBy = queryCommand.getOrderBy();
        queryCommand.setOrderBy(null);

        parts.add("SELECT "); //$NON-NLS-1$
        /*
         * if all of the columns are aliased, assume that names matter - it actually only seems to matter for
         * the first query of a set op when there is a order by.  Rather than adding logic to traverse up,
         * we just use the projected names
         */
        boolean allAliased = true;
        for (DerivedColumn selectSymbol : queryCommand.getProjectedQuery().getDerivedColumns()) {
            if (selectSymbol.getAlias() == null) {
                allAliased = false;
                break;
            }
        }
        if (allAliased) {
            String[] columnNames = queryCommand.getColumnNames();
            for (int i = 0; i < columnNames.length; i++) {
                if (i > 0) {
                    parts.add(", "); //$NON-NLS-1$
                }
                parts.add(columnNames[i]);
            }
        } else {
            parts.add("*"); //$NON-NLS-1$
        }
        boolean addedToSelect = false;
        if (orderBy != null && queryCommand instanceof Select) {
            Select select = (Select)queryCommand;
            if (!select.isDistinct() && select.getGroupBy() == null) {
                //the order by may be unrelated, so it needs to be with the select
                WindowFunction expression = new WindowFunction();
                expression.setFunction(new AggregateFunction("ROW_NUMBER", //$NON-NLS-1$
                        false, Collections.EMPTY_LIST, TypeFacility.RUNTIME_TYPES.INTEGER));
                WindowSpecification windowSpecification = new WindowSpecification();
                windowSpecification.setOrderBy(orderBy);
                expression.setWindowSpecification(windowSpecification);
                select.getDerivedColumns().add(new DerivedColumn("ROWNUM_", expression)); //$NON-NLS-1$
                parts.add(" FROM ("); //$NON-NLS-1$
                parts.add(select);
                addedToSelect = true;
            }
        }
        if (!addedToSelect) {
            //the order by can be done above the view
            parts.add(" FROM (SELECT v.*, ROW_NUMBER() OVER ("); //$NON-NLS-1$
            if (orderBy != null) {
                parts.add(orderBy);
            } else {
                //use an order by a "constant"
                parts.add("ORDER BY @@version"); //$NON-NLS-1$
            }
            parts.add(") ROWNUM_ FROM ("); //$NON-NLS-1$
            parts.add(queryCommand);
            parts.add(") v"); //$NON-NLS-1$
        }
        parts.add(") v WHERE ROWNUM_ "); //$NON-NLS-1$
        if (limit.getRowLimit() != Integer.MAX_VALUE) {
            parts.add("<= "); //$NON-NLS-1$
            parts.add((long)limit.getRowLimit() + limit.getRowOffset());
            parts.add(" AND ROWNUM_ "); //$NON-NLS-1$
        }
        parts.add("> "); //$NON-NLS-1$
        parts.add(limit.getRowOffset());
        if (orderBy != null) {
            parts.add(" ORDER BY ROWNUM_"); //$NON-NLS-1$
        }
        return parts;
    }

    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        if (getVersion().compareTo(ELEVEN_0) >= 0) {
            return Arrays.asList("OFFSET ", limit.getRowOffset(), " ROWS FETCH NEXT ", limit.getRowLimit(), " ROWS ONLY"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        return super.translateLimit(limit, context);
    }

    @Override
    public boolean useSelectLimit() {
        return getVersion().compareTo(ELEVEN_0) < 0;
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        if (getVersion().compareTo(TEN_0) < 0) {
            return super.translateLiteralTimestamp(timestampValue);
        }
        return "{ts '" + formatDateValue(timestampValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public boolean supportsWindowFunctionNtile() {
        return getVersion().compareTo(TEN_0) >= 0;
    }

    @Override
    public boolean supportsWindowFunctionPercentRank() {
        return getVersion().compareTo(ELEVEN_0) >= 0;
    }

    @Override
    public boolean supportsWindowFunctionCumeDist() {
        return getVersion().compareTo(ELEVEN_0) >= 0;
    }

    @Override
    public boolean supportsWindowFunctionNthValue() {
        return false;
    }

    @Override
    public boolean supportsWindowFrameClause() {
        return getVersion().compareTo(TEN_0) >= 0;
    }

}
