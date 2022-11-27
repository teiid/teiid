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

package org.teiid.translator.jdbc.oracle;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BIG_DECIMAL;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.OBJECT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.TIMESTAMP;

import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.GeometryInputSource;
import org.teiid.core.types.BinaryType;
import org.teiid.core.util.StringUtil;
import org.teiid.language.AggregateFunction;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Array;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Function;
import org.teiid.language.In;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Like;
import org.teiid.language.Like.MatchMode;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.Parameter;
import org.teiid.language.QueryExpression;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.Select;
import org.teiid.language.SetQuery;
import org.teiid.language.SetQuery.Operation;
import org.teiid.language.TableReference;
import org.teiid.language.With;
import org.teiid.language.WithItem;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.AggregateAttributes;
import org.teiid.metadata.Column;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.TypeFacility.RUNTIME_CODES;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;
import org.teiid.translator.jdbc.JDBCPlugin;
import org.teiid.translator.jdbc.LocateFunctionModifier;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TemplateFunctionModifier;
import org.teiid.util.Version;


@Translator(name="oracle", description="A translator for Oracle 9i Database or later")
public class OracleExecutionFactory extends JDBCExecutionFactory {

    private static final String TRUNC = "TRUNC"; //$NON-NLS-1$
    private static final String LISTAGG = "LISTAGG"; //$NON-NLS-1$
    private static final String TO_NCHAR = "TO_NCHAR"; //$NON-NLS-1$
    public static final Version NINE_0 = Version.getVersion("9.0"); //$NON-NLS-1$
    public static final Version NINE_2 = Version.getVersion("9.2"); //$NON-NLS-1$
    public static final Version ELEVEN_2_0_4 = Version.getVersion("11.2.0.4"); //$NON-NLS-1$
    public static final Version ELEVEN_2 = Version.getVersion("11.2"); //$NON-NLS-1$
    public static final Version TWELVE = Version.getVersion("12"); //$NON-NLS-1$

    private static final String TIME_FORMAT = "HH24:MI:SS"; //$NON-NLS-1$
    private static final String DATE_FORMAT = "YYYY-MM-DD"; //$NON-NLS-1$
    private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT; //$NON-NLS-1$
    private static final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".FF";  //$NON-NLS-1$

    public final static String HINT_PREFIX = "/*+"; //$NON-NLS-1$
    public static final String HINT_SUFFIX = "*/";  //$NON-NLS-1$
    public final static String DUAL = "DUAL"; //$NON-NLS-1$
    public final static String ROWNUM = "ROWNUM"; //$NON-NLS-1$
    public final static String SEQUENCE = ":SEQUENCE="; //$NON-NLS-1$
    /*
     * Spatial Functions
     */
    public static final String RELATE = "sdo_relate"; //$NON-NLS-1$
    public static final String NEAREST_NEIGHBOR = "sdo_nn"; //$NON-NLS-1$
    public static final String FILTER = "sdo_filter"; //$NON-NLS-1$
    public static final String WITHIN_DISTANCE = "sdo_within_distance"; //$NON-NLS-1$
    public static final String NEAREST_NEIGHBOR_DISTANCE = "sdo_nn_distance"; //$NON-NLS-1$
    public static final String ORACLE_SDO = "Oracle-SDO"; //$NON-NLS-1$
    public static final String ORACLE = "Oracle"; //$NON-NLS-1$

    private static final Set<String> STRING_BOOLEAN_FUNCTIONS = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    static {
        STRING_BOOLEAN_FUNCTIONS.addAll(
                Arrays.asList(SourceSystemFunctions.ST_DISJOINT, SourceSystemFunctions.ST_CONTAINS,
                        SourceSystemFunctions.ST_CROSSES, SourceSystemFunctions.ST_INTERSECTS,
                        SourceSystemFunctions.ST_OVERLAPS, SourceSystemFunctions.ST_TOUCHES,
                        SourceSystemFunctions.ST_EQUALS));
    }

    private final class DateAwareExtract extends ExtractFunctionModifier {
        @Override
        public List<?> translate(Function function) {
            Expression ex = function.getParameters().get(0);
            if (ex.getType() == TypeFacility.RUNTIME_TYPES.DATE || ex.getType() == TypeFacility.RUNTIME_TYPES.TIME
                    || (ex instanceof ColumnReference && "date".equalsIgnoreCase(((ColumnReference)ex).getMetadataObject().getNativeType())) //$NON-NLS-1$
                    || (!(ex instanceof ColumnReference) && !(ex instanceof Literal) && !(ex instanceof Function))) {
                ex = ConvertModifier.createConvertFunction(getLanguageFactory(), function.getParameters().get(0), TypeFacility.RUNTIME_NAMES.TIMESTAMP);
                function.getParameters().set(0, ex);
            }
            return super.translate(function);
        }
    }

    /*
     * Handling for cursor return values
     */
    static final class RefCursorType {}
    static int CURSOR_TYPE = -10;
    static final String REF_CURSOR = "REF CURSOR"; //$NON-NLS-1$

    /*
     * handling for char bindings
     */
    static final class FixedCharType {}
    static int FIXED_CHAR_TYPE = 999;

    /*
     * handling for varchar bindings
     */
    static final class VarcharType {}

    protected Map<Class<?>, Integer> customTypeCodes = new HashMap<>();

    private boolean oracleSuppliedDriver = true;

    private OracleFormatFunctionModifier parseModifier = new OracleFormatFunctionModifier("TO_TIMESTAMP(", true); //$NON-NLS-1$

    private boolean useNBindingType = true;
    private boolean isExtendedAscii = true;

    public OracleExecutionFactory() {
        //older oracle instances seem to have issues with large numbers of bindings
        setUseBindingsForDependentJoin(false);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        customTypeCodes.put(VarcharType.class, Types.VARCHAR);
        customTypeCodes.put(FixedCharType.class, FIXED_CHAR_TYPE);
        customTypeCodes.put(RefCursorType.class, CURSOR_TYPE);
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("nvl")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG10, new Log10FunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.HOUR, new DateAwareExtract());
        registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new DateAwareExtract());
        registerFunctionModifier(SourceSystemFunctions.SECOND, new DateAwareExtract());
        registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ExtractFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Month"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Day"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.WEEK, new DayWeekQuarterFunctionModifier("IW"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new DayWeekQuarterFunctionModifier("Q"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new TemplateFunctionModifier("(trunc(",0,") - trunc(",0,",'IW') + 1)"));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new DayWeekQuarterFunctionModifier("DDD"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory(), "INSTR", true)); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new ConcatFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.CONCAT2, new AliasModifier("||")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.COT, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                function.setName(SourceSystemFunctions.TAN);
                return Arrays.asList(getLanguageFactory().createFunction(SourceSystemFunctions.DIVIDE_OP, new Expression[] {new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER), function}, TypeFacility.RUNTIME_TYPES.DOUBLE));
            }
        });

        //spatial functions
        registerFunctionModifier(OracleExecutionFactory.RELATE, new OracleSpatialFunctionModifier());
        registerFunctionModifier(OracleExecutionFactory.NEAREST_NEIGHBOR, new OracleSpatialFunctionModifier());
        registerFunctionModifier(OracleExecutionFactory.FILTER, new OracleSpatialFunctionModifier());
        registerFunctionModifier(OracleExecutionFactory.WITHIN_DISTANCE, new OracleSpatialFunctionModifier());

        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, parseModifier);
        registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new OracleFormatFunctionModifier("TO_CHAR(", false)); //$NON-NLS-1$

        //add in type conversion
        ConvertModifier convertModifier = new ConvertModifier();
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.CHAR, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                if (isNonAscii(function.getParameters().get(0))) {
                    ((Literal)function.getParameters().get(1)).setValue("nchar(1)"); //$NON-NLS-1$
                } else {
                    ((Literal)function.getParameters().get(1)).setValue("char(1)"); //$NON-NLS-1$
                }
                return null;
            }
        });
        convertModifier.addTypeMapping("date", FunctionModifier.DATE, FunctionModifier.TIME); //$NON-NLS-1$
        convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("case when ", function.getParameters().get(0), " is null then null else to_date('1970-01-01 ' || to_char(",function.getParameters().get(0),", 'HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') end"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("trunc(cast(",function.getParameters().get(0)," AS date))"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", DATE_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", TIME_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                //if column and type is date, just use date format
                Expression ex = function.getParameters().get(0);
                String format = TIMESTAMP_FORMAT;
                if (ex instanceof ColumnReference && "date".equalsIgnoreCase(((ColumnReference)ex).getMetadataObject().getNativeType())) { //$NON-NLS-1$
                    format = DATETIME_FORMAT;
                } else if (!(ex instanceof Literal) && !(ex instanceof Function)) {
                    //this isn't needed in every case, but it's simpler than inspecting the expression more
                    ex = ConvertModifier.createConvertFunction(getLanguageFactory(), function.getParameters().get(0), TypeFacility.RUNTIME_NAMES.TIMESTAMP);
                }
                return Arrays.asList("to_char(", ex, ", '", format, "')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("to_date", DATE_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("to_date", TIME_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("to_timestamp", TIMESTAMP_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.CLOB, FunctionModifier.STRING, new TemplateFunctionModifier("DBMS_LOB.substr(", 0, ", 4000)")); //$NON-NLS-1$ //$NON-NLS-2$
        convertModifier.addTypeConversion(new FunctionModifier() {

            ConvertModifier.FormatModifier toChar = new ConvertModifier.FormatModifier("to_char"); //$NON-NLS-1$
            ConvertModifier.FormatModifier toNChar = new ConvertModifier.FormatModifier(TO_NCHAR);

            @Override
            public List<?> translate(Function function) {
                if (isNonAscii(function.getParameters().get(0))) {
                    return toNChar.translate(function);
                }
                return toChar.translate(function);
            }
        }, FunctionModifier.STRING);

        //NOTE: numeric handling in Oracle is split only between integral vs. floating/decimal types
        convertModifier.addTypeConversion(new ConvertModifier.FormatModifier("to_number"), //$NON-NLS-1$
                FunctionModifier.FLOAT, FunctionModifier.DOUBLE, FunctionModifier.BIGDECIMAL);
        convertModifier.addTypeConversion(new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                if (Number.class.isAssignableFrom(function.getParameters().get(0).getType())) {
                    return Arrays.asList("trunc(", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
                }
                return Arrays.asList("trunc(to_number(", function.getParameters().get(0), "))"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        },
        FunctionModifier.BYTE, FunctionModifier.SHORT, FunctionModifier.INTEGER, FunctionModifier.LONG,    FunctionModifier.BIGINTEGER);
        convertModifier.addNumericBooleanConversions();
        convertModifier.setWideningNumericImplicit(true);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);

        addPushDownFunction(ORACLE, TRUNC, TIMESTAMP, TIMESTAMP, STRING);
        addPushDownFunction(ORACLE, TRUNC, TIMESTAMP, TIMESTAMP);
        addPushDownFunction(ORACLE, TRUNC, BIG_DECIMAL, BIG_DECIMAL, BIG_DECIMAL);
        addPushDownFunction(ORACLE, TRUNC, BIG_DECIMAL, BIG_DECIMAL);

        addPushDownFunction(ORACLE_SDO, RELATE, STRING, STRING, STRING, STRING);
        addPushDownFunction(ORACLE_SDO, RELATE, STRING, OBJECT, OBJECT, STRING);
        addPushDownFunction(ORACLE_SDO, RELATE, STRING, STRING, OBJECT, STRING);
        addPushDownFunction(ORACLE_SDO, RELATE, STRING, OBJECT, STRING, STRING);
        addPushDownFunction(ORACLE_SDO, NEAREST_NEIGHBOR, STRING, STRING, OBJECT, STRING, INTEGER);
        addPushDownFunction(ORACLE_SDO, NEAREST_NEIGHBOR, STRING, OBJECT, OBJECT, STRING, INTEGER);
        addPushDownFunction(ORACLE_SDO, NEAREST_NEIGHBOR, STRING, OBJECT, STRING, STRING, INTEGER);
        addPushDownFunction(ORACLE_SDO, NEAREST_NEIGHBOR_DISTANCE, INTEGER, INTEGER);
        addPushDownFunction(ORACLE_SDO, WITHIN_DISTANCE, STRING, OBJECT, OBJECT, STRING);
        addPushDownFunction(ORACLE_SDO, WITHIN_DISTANCE, STRING, STRING, OBJECT, STRING);
        addPushDownFunction(ORACLE_SDO, WITHIN_DISTANCE, STRING, OBJECT, STRING, STRING);
        addPushDownFunction(ORACLE_SDO, FILTER, STRING, OBJECT, STRING, STRING);
        addPushDownFunction(ORACLE_SDO, FILTER, STRING, OBJECT, OBJECT, STRING);
        addPushDownFunction(ORACLE_SDO, FILTER, STRING, STRING, OBJECT, STRING);

        registerFunctionModifier(SourceSystemFunctions.ST_ASBINARY, new AliasModifier("SDO_UTIL.TO_WKBGEOMETRY")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_ASTEXT, new AliasModifier("SDO_UTIL.TO_WKTGEOMETRY")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_ASGML, new AliasModifier("SDO_UTIL.TO_GMLGEOMETRY")); //$NON-NLS-1$

        // Used instead of SDO_UTIL functions because it allows SRID to be specified.
        // we need to use to_blob and to_clob to disambiguate
        registerFunctionModifier(SourceSystemFunctions.ST_GEOMFROMWKB, new AliasModifier("SDO_GEOMETRY") { //$NON-NLS-1$

            @Override
            public List<?> translate(Function function) {
                Expression ex = function.getParameters().get(0);
                if (ex instanceof Parameter || ex instanceof Literal) {
                    function.getParameters().set(0, new Function("TO_BLOB", Arrays.asList(ex), TypeFacility.RUNTIME_TYPES.BLOB)); //$NON-NLS-1$
                }
                return super.translate(function);
            }
        });
        registerFunctionModifier(SourceSystemFunctions.ST_GEOMFROMTEXT, new AliasModifier("SDO_GEOMETRY") { //$NON-NLS-1$

            @Override
            public List<?> translate(Function function) {
                Expression ex = function.getParameters().get(0);
                if (ex instanceof Parameter || ex instanceof Literal) {
                    function.getParameters().set(0, new Function("TO_CLOB", Arrays.asList(ex), TypeFacility.RUNTIME_TYPES.CLOB)); //$NON-NLS-1$
                }
                return super.translate(function);
            }
        });

        registerFunctionModifier(SourceSystemFunctions.ST_DISTANCE, new TemplateFunctionModifier("SDO_GEOM.SDO_DISTANCE(", 0, ", ", 1, ", 0.005)")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Disjoint mask cannot be used with SDO_RELATE (says docs).
        registerFunctionModifier(SourceSystemFunctions.ST_DISJOINT, new TemplateFunctionModifier("CASE SDO_GEOM.RELATE(", 0, ", 'disjoint', ", 1,", 0.005) WHEN 'DISJOINT' THEN 'TRUE' ELSE 'FALSE' END")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        registerFunctionModifier(SourceSystemFunctions.ST_CONTAINS, new AliasModifier("SDO_CONTAINS")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_INTERSECTS, new AliasModifier("SDO_ANYINTERACT")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_OVERLAPS, new AliasModifier("SDO_OVERLAPBDYINTERSECT")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_CROSSES, new AliasModifier("SDO_OVERLAPBDYDISJOINT")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_TOUCHES, new AliasModifier("SDO_TOUCH")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_EQUALS, new AliasModifier("SDO_EQUALS")); //$NON-NLS-1$
        //registerFunctionModifier(SourceSystemFunctions.ST_WITHIN, new OracleRelateModifier("inside")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_SRID, new TemplateFunctionModifier("nvl(", 0, ".sdo_srid, 0)")); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunctionModifier(SourceSystemFunctions.RAND, new AliasModifier("DBMS_RANDOM.VALUE")); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new TimestampAddModifier());
    }

    @Override
    public void initCapabilities(Connection connection)
            throws TranslatorException {
        super.initCapabilities(connection);
        if (getVersion().compareTo(ELEVEN_2) >= 0) {
            AggregateAttributes aa = new AggregateAttributes();
            aa.setAllowsOrderBy(true);
            addPushDownFunction(ORACLE, LISTAGG, STRING, STRING, STRING).setAggregateAttributes(aa);
            addPushDownFunction(ORACLE, LISTAGG, STRING, STRING).setAggregateAttributes(aa);
        }
        if (connection != null && isOracleSuppliedDriver()) {
            try {
                if (connection.getMetaData().getDriverMajorVersion() <= 11) {
                    useNBindingType = false;
                }
                try (Statement s = connection.createStatement();) {
                    ResultSet rs = s.executeQuery("select value from nls_database_parameters where parameter='NLS_CHARACTERSET'"); //$NON-NLS-1$
                    if (rs.next()) {
                        String encoding = rs.getString(1);
                        if ("US7ASCII".equalsIgnoreCase(encoding)) { //$NON-NLS-1$
                            isExtendedAscii = false;
                        }
                    }
                }
            } catch (SQLException e) {

            }
        }
    }

    public void handleInsertSequences(Insert insert) throws TranslatorException {
        /*
         * If a missing auto_increment column is modeled with name in source indicating that an Oracle Sequence
         * then pull the Sequence name out of the name in source of the column.
         */
        if (!(insert.getValueSource() instanceof ExpressionValueSource)) {
            return;
        }
        ExpressionValueSource values = (ExpressionValueSource)insert.getValueSource();
        if (insert.getTable().getMetadataObject() == null) {
            return;
        }
        List<Column> allElements = insert.getTable().getMetadataObject().getColumns();
        if (allElements.size() == values.getValues().size()) {
            return;
        }

        int index = 0;
        List<ColumnReference> elements = insert.getColumns();

        for (Column element : allElements) {
            if (!element.isAutoIncremented()) {
                continue;
            }
            String name = element.getNameInSource();
            int seqIndex = name.indexOf(SEQUENCE);
            if (seqIndex == -1) {
                continue;
            }
            boolean found = false;
            while (index < elements.size()) {
                if (element.equals(elements.get(index).getMetadataObject())) {
                    found = true;
                    break;
                }
                index++;
            }
            if (found) {
                continue;
            }

            String sequence = name.substring(seqIndex + SEQUENCE.length());

            int delimiterIndex = sequence.indexOf(Tokens.DOT);
            if (delimiterIndex == -1) {
                 throw new TranslatorException(JDBCPlugin.Event.TEIID11017, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11017, SEQUENCE, name));
            }
            String sequenceGroupName = sequence.substring(0, delimiterIndex);
            String sequenceElementName = sequence.substring(delimiterIndex + 1);

            NamedTable sequenceGroup = this.getLanguageFactory().createNamedTable(sequenceGroupName, null, null);
            ColumnReference sequenceElement = this.getLanguageFactory().createColumnReference(sequenceElementName, sequenceGroup, null, element.getJavaType());
            insert.getColumns().add(index, this.getLanguageFactory().createColumnReference(element.getName(), insert.getTable(), element, element.getJavaType()));
            values.getValues().add(index, sequenceElement);
        }
    }

    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
        if (command instanceof Insert) {
            Insert insert = (Insert)command;
            try {
                handleInsertSequences(insert);
                correctInsertTypes(insert);
            } catch (TranslatorException e) {
                throw new RuntimeException(e);
            }
        }

        if (!(command instanceof QueryExpression)) {
            return null;
        }
        QueryExpression queryCommand = (QueryExpression)command;
        if (queryCommand.getLimit() == null) {
            return null;
        }
        Limit limit = queryCommand.getLimit();
        queryCommand.setLimit(null);

        if (command instanceof Select) {
            Select select = (Select)command;

            TableReference tr = select.getFrom().get(0);
            if (tr instanceof NamedTable && isDual((NamedTable)tr)) {
                if (limit.getRowOffset() > 0 || limit.getRowLimit() == 0) {
                    //no data
                    select.setWhere(new Comparison(new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER), new Literal(0, TypeFacility.RUNTIME_TYPES.INTEGER), Operator.EQ));
                    return null;
                }
                return null; //dual does not allow a limit
            }
        }

        List<Object> parts = new ArrayList<Object>();

        if (queryCommand.getWith() != null) {
            With with = queryCommand.getWith();
            queryCommand.setWith(null);
            parts.add(with);
        }

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
        if (limit.getRowOffset() > 0) {
            parts.add(" FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM ("); //$NON-NLS-1$
        } else {
            parts.add(" FROM ("); //$NON-NLS-1$
        }
        parts.add(queryCommand);
        if (limit.getRowOffset() > 0) {
            if (limit.getRowLimit() != Integer.MAX_VALUE) {
                parts.add(") VIEW_FOR_LIMIT WHERE ROWNUM <= "); //$NON-NLS-1$
                parts.add((long)limit.getRowLimit() + limit.getRowOffset());
            } else {
                parts.add(") VIEW_FOR_LIMIT"); //$NON-NLS-1$
            }
            parts.add(") WHERE ROWNUM_ > "); //$NON-NLS-1$
            parts.add(limit.getRowOffset());
        } else {
            parts.add(") WHERE ROWNUM <= "); //$NON-NLS-1$
            parts.add(limit.getRowLimit());
        }
        return parts;
    }

    private void correctInsertTypes(Insert insert) {
        List<ColumnReference> columns = insert.getColumns();
        if (!(insert.getValueSource() instanceof ExpressionValueSource)) {
            return;
        }
        ExpressionValueSource evs = (ExpressionValueSource)insert.getValueSource();
        List<Expression> expressions = evs.getValues();
        for (int i = 0; i < columns.size(); i++) {
            ColumnReference cr = columns.get(i);
            if (cr.getMetadataObject() == null) {
                continue;
            }
            if (!isCharacterType(cr.getType())) {
                continue;
            }
            String nativeType = cr.getMetadataObject().getNativeType();
            if (nativeType == null || StringUtil.startsWithIgnoreCase(nativeType, "N")) { //$NON-NLS-1$
                continue;
            }
            Expression ex = expressions.get(i);
            if (ex instanceof Literal) {
                Literal l = (Literal)ex;
                if (!l.isBindEligible()) {
                    continue;
                }
                l.setType(VarcharType.class);
                Object value = l.getValue();
                if (value != null && isNonAscii(value.toString())) {
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Inserting a string with non-ascii characters into a varchar column, replacement characters will be used."); //$NON-NLS-1$
                }
            }
            if (ex instanceof Parameter) {
                ((Parameter)ex).setType(VarcharType.class);
            }
        }
    }

    private boolean isDual(NamedTable table) {
        String groupName = null;
        AbstractMetadataRecord groupID = table.getMetadataObject();
        if(groupID != null) {
            groupName = SQLStringVisitor.getRecordName(groupID);
        } else {
            groupName = table.getName();
        }
        return DUAL.equalsIgnoreCase(groupName);
    }

    @Override
    public boolean useAsInGroupAlias(){
        return false;
    }

    @Override
    public String getSetOperationString(Operation operation) {
        if (operation == Operation.EXCEPT) {
            return "MINUS"; //$NON-NLS-1$
        }
        return super.getSetOperationString(operation);
    }

    @Override
    public String getSourceComment(ExecutionContext context, Command command) {
        String comment = super.getSourceComment(context, command);

        boolean usingPayloadComment = false;
        if (context != null) {
            // Check for db hints
            Object payload = context.getCommandPayload();
            if (payload instanceof String) {
                String payloadString = (String)payload;
                if (payloadString.startsWith(HINT_PREFIX)) {
                    int i = payloadString.indexOf(HINT_SUFFIX);
                    if (i > 0 && payloadString.substring(i + 2).trim().length() == 0) {
                        comment += payloadString + " "; //$NON-NLS-1$
                        usingPayloadComment = true;
                    } else {
                        String msg = JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11003, "Execution Payload", payloadString); //$NON-NLS-1$
                        context.addWarning(new TranslatorException(msg));
                        LogManager.logWarning(LogConstants.CTX_CONNECTOR, msg);
                    }
                }
            }
        }

        if (!usingPayloadComment && context != null) {
            String hint = context.getSourceHint();
            if (context.getGeneralHint() != null) {
                if (hint != null) {
                    hint += (" " + context.getGeneralHint()); //$NON-NLS-1$
                } else {
                    hint = context.getGeneralHint();
                }
            }
            if (hint != null) {
                //append a source hint
                if (!hint.contains(HINT_PREFIX)) {
                    comment += HINT_PREFIX + ' ' + hint + ' ' + HINT_SUFFIX + ' ';
                } else {
                    String msg = JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11003, "Source Hint", hint); //$NON-NLS-1$
                    context.addWarning(new TranslatorException(msg));
                    LogManager.logWarning(LogConstants.CTX_CONNECTOR, msg);
                }
            }
        }

        if (command instanceof Select) {
            //
            // This simple algorithm determines the hint which will be added to the
            // query.
            // Right now, we look through all functions passed in the query
            // (returned as a collection)
            // Then we check if any of those functions are sdo_relate
            // If so, the ORDERED hint is added, if not, it isn't
            Collection<Function> col = CollectorVisitor.collectObjects(Function.class, command);
            for (Function func : col) {
                if (func.getName().equalsIgnoreCase(OracleExecutionFactory.RELATE)) {
                    return comment + "/*+ ORDERED */ "; //$NON-NLS-1$
                }
            }
        }
        return comment;
    }

    /**
     * Don't fully qualify elements if table = DUAL or element = ROWNUM or special stuff is packed into name in source value.
     *
     * @since 5.0
     */
    @Override
    public String replaceElementName(String group, String element) {

        // Check if the element was modeled as using a Sequence
        int useIndex = element.indexOf(SEQUENCE);
        if (useIndex >= 0) {
            String name = element.substring(0, useIndex);
            if (group != null) {
                return group + Tokens.DOT + name;
            }
            return name;
        }

        // Check if the group name should be discarded
        if((group != null && DUAL.equalsIgnoreCase(group)) || element.equalsIgnoreCase(ROWNUM)) {
            // Strip group if group or element are pseudo-columns
            return element;
        }

        return null;
    }

    @Override
    public boolean hasTimeType() {
        return false;
    }

    @Override
    public void bindValue(PreparedStatement stmt, Object param, Class<?> paramType, int i) throws SQLException {
        Integer code = customTypeCodes.get(paramType);
        if (code != null) {
            stmt.setObject(i, param, code);
            return;
        }
        super.bindValue(stmt, param, paramType, i);
    }

    @Override
    public boolean useStreamsForLobs() {
        return true;
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.HIGH;
    }

    @Override
    public boolean supportsOrderByNullOrdering() {
        return true;
    }

    @Override
    public SQLConversionVisitor getSQLConversionVisitor() {
        return new SQLConversionVisitor(this) {

            @Override
            public void visit(Select select) {
                if (select.getFrom() == null || select.getFrom().isEmpty()) {
                    select.setFrom(Arrays.asList((TableReference)new NamedTable(DUAL, null, null)));
                }
                super.visit(select);
            }

            @Override
            public void visit(Comparison obj) {
                if (isFixedChar(obj.getLeftExpression())) {
                    if (obj.getRightExpression() instanceof Literal) {
                        Literal l = (Literal)obj.getRightExpression();
                        l.setType(FixedCharType.class);
                    } else if (obj.getRightExpression() instanceof Parameter) {
                        Parameter p = (Parameter)obj.getRightExpression();
                        p.setType(FixedCharType.class);
                    }
                }
                if (obj.getLeftExpression().getType() == TypeFacility.RUNTIME_TYPES.BOOLEAN
                        && (obj.getLeftExpression() instanceof Function)
                        && obj.getRightExpression() instanceof Literal) {
                    Function f = (Function)obj.getLeftExpression();
                    if (STRING_BOOLEAN_FUNCTIONS.contains(f.getName())) {
                        Boolean b = (Boolean)((Literal)obj.getRightExpression()).getValue();
                        obj.setRightExpression(new Literal(b!=null?(b?"TRUE":"FALSE"):null, TypeFacility.RUNTIME_TYPES.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                }
                super.visit(obj);
            }

            @Override
            protected void appendRightComparison(Comparison obj) {
                if (obj.getRightExpression() instanceof Array) {
                    //oracle needs rhs arrays nested in extra parens
                    buffer.append(SQLConstants.Tokens.LPAREN);
                    super.appendRightComparison(obj);
                    buffer.append(SQLConstants.Tokens.RPAREN);
                } else {
                    super.appendRightComparison(obj);
                }
            }

            private boolean isFixedChar(Expression obj) {
                if (!isOracleSuppliedDriver() || !(obj instanceof ColumnReference)) {
                    return false;
                }
                ColumnReference cr = (ColumnReference)obj;
                return (cr.getType() == TypeFacility.RUNTIME_TYPES.STRING || cr.getType() == TypeFacility.RUNTIME_TYPES.CHAR)
                        && cr.getMetadataObject() != null
                        && ("CHAR".equalsIgnoreCase(cr.getMetadataObject().getNativeType()) //$NON-NLS-1$
                                || "NCHAR".equalsIgnoreCase(cr.getMetadataObject().getNativeType())); //$NON-NLS-1$
            }

            @Override
            public void visit(In obj) {
                if (isFixedChar(obj.getLeftExpression())) {
                    for (Expression exp : obj.getRightExpressions()) {
                        if (exp instanceof Literal) {
                            Literal l = (Literal)exp;
                            l.setType(FixedCharType.class);
                        } else if (exp instanceof Parameter) {
                            Parameter p = (Parameter)exp;
                            p.setType(FixedCharType.class);
                        }
                    }
                }
                super.visit(obj);
            }

            @Override
            public void visit(NamedTable table) {
                stripDualAlias(table);
                super.visit(table);
            }

            private void stripDualAlias(NamedTable table) {
                if (table.getCorrelationName() != null) {
                    if (isDual(table)) {
                        table.setCorrelationName(null);
                    }
                }
            }

            @Override
            public void visit(ColumnReference obj) {
                if (obj.getTable() != null) {
                    stripDualAlias(obj.getTable());
                }
                super.visit(obj);
            }

            @Override
            public void visit(Call call) {
                if (oracleSuppliedDriver && call.getResultSetColumnTypes().length > 0 && call.getMetadataObject() != null) {
                    if (call.getReturnType() == null && call.getMetadataObject().getProperty(SQLConversionVisitor.TEIID_NATIVE_QUERY, false) == null) {
                        //assume stored function handling
                        if (!setOutCursorType(call)) {
                            call.setReturnType(RefCursorType.class);
                        }
                    } else {
                        //TODO we only will allow a single out cursor
                        if (call.getMetadataObject() != null) {
                            ProcedureParameter param = call.getReturnParameter();
                            if (param != null && REF_CURSOR.equalsIgnoreCase(param.getNativeType())) {
                                call.setReturnType(RefCursorType.class);
                            }
                        }
                        setOutCursorType(call);
                    }
                }
                super.visit(call);
            }

            private boolean setOutCursorType(Call call) {
                boolean set = false;
                for (Argument arg : call.getArguments()) {
                    if (arg.getDirection() == Direction.OUT) {
                        ProcedureParameter param = arg.getMetadataObject();
                        if (param != null && REF_CURSOR.equalsIgnoreCase(param.getNativeType())) {
                            arg.setType(RefCursorType.class);
                            set = true;
                        }
                    }
                }
                return set;
            }

            @Override
            public void visit(Like obj) {
                if (obj.getMode() == MatchMode.REGEX) {
                    if (obj.isNegated()) {
                        buffer.append("NOT("); //$NON-NLS-1$
                    }
                    buffer.append("REGEXP_LIKE(");  //$NON-NLS-1$
                    append(obj.getLeftExpression());
                    buffer.append(", ");  //$NON-NLS-1$
                    append(obj.getRightExpression());
                    buffer.append(")");  //$NON-NLS-1$
                    if (obj.isNegated()) {
                        buffer.append(")");  //$NON-NLS-1$
                    }
                } else {
                    super.visit(obj);
                }
            }

            @Override
            public void visit(WithItem obj) {
                if (obj.getColumns() != null) {
                    List<ColumnReference> cols = obj.getColumns();
                    if(!obj.isRecusive()) {
                        //oracle 10 does not support recursion nor a column list
                        obj.setColumns(null);
                        Select select = obj.getSubquery().getProjectedQuery();
                        List<DerivedColumn> selectClause = select.getDerivedColumns();
                        for (int i = 0; i < cols.size(); i++) {
                            selectClause.get(i).setAlias(cols.get(i).getName());
                        }
                    }
                }
                super.visit(obj);
            }

            @Override
            public void visit(SearchedCase obj) {
                boolean i18n = false;
                if (OracleExecutionFactory.this.isNonAscii(obj.getElseExpression())) {
                    i18n = true;
                }
                for (SearchedWhenClause clause : obj.getCases()) {
                    if (OracleExecutionFactory.this.isNonAscii(clause.getResult())) {
                        i18n = true;
                    }
                }
                if (i18n) {
                    if (obj.getElseExpression() != null && !OracleExecutionFactory.this.isNonAscii(obj.getElseExpression())) {
                        obj.setElseExpression(toNChar(obj.getElseExpression()));
                    }
                    for (SearchedWhenClause clause : obj.getCases()) {
                        if (!OracleExecutionFactory.this.isNonAscii(clause.getResult())) {
                            clause.setResult(toNChar(clause.getResult()));
                        }
                    }
                }
                super.visit(obj);
            }

            private Function toNChar(Expression ex) {
                return new Function(TO_NCHAR, Arrays.asList(ex), TypeFacility.RUNTIME_TYPES.STRING);
            }

            @Override
            public void visit(SetQuery obj) {
                for (int i = 0; i < obj.getColumnNames().length; i++) {
                    DerivedColumn leftDerivedColumn = obj.getLeftQuery().getProjectedQuery().getDerivedColumns().get(i);
                    boolean left_i18n = OracleExecutionFactory.this.isNonAscii(leftDerivedColumn.getExpression());
                    DerivedColumn rightDerivedColumn = obj.getRightQuery().getProjectedQuery().getDerivedColumns().get(i);
                    boolean right_i18n = OracleExecutionFactory.this.isNonAscii(rightDerivedColumn.getExpression());
                    if (left_i18n ^ right_i18n) {
                        if (!left_i18n) {
                            leftDerivedColumn.setExpression(toNChar(leftDerivedColumn.getExpression()));
                        } else if (!right_i18n) {
                            rightDerivedColumn.setExpression(toNChar(rightDerivedColumn.getExpression()));
                        }
                    }
                }
                super.visit(obj);
            }

        };
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.COT);
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        supportedFunctions.add("CEILING"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.CONCAT2);
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        //supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("DAY"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.FORMATTIMESTAMP);
        //TEIID-4656
        //supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL");      //$NON-NLS-1$
        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(RELATE);
        supportedFunctions.add(NEAREST_NEIGHBOR);
        supportedFunctions.add(NEAREST_NEIGHBOR_DISTANCE);
        supportedFunctions.add(WITHIN_DISTANCE);
        supportedFunctions.add(FILTER);
        supportedFunctions.add(SourceSystemFunctions.ST_ASBINARY);
        supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMWKB);
        supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMTEXT);
        supportedFunctions.add(SourceSystemFunctions.ST_ASTEXT);
        supportedFunctions.add(SourceSystemFunctions.ST_ASGML);
        supportedFunctions.add(SourceSystemFunctions.ST_CONTAINS);
        supportedFunctions.add(SourceSystemFunctions.ST_CROSSES);
        supportedFunctions.add(SourceSystemFunctions.ST_DISJOINT);
        supportedFunctions.add(SourceSystemFunctions.ST_DISTANCE);
        supportedFunctions.add(SourceSystemFunctions.ST_INTERSECTS);
        supportedFunctions.add(SourceSystemFunctions.ST_OVERLAPS);
        supportedFunctions.add(SourceSystemFunctions.ST_TOUCHES);
        supportedFunctions.add(SourceSystemFunctions.ST_SRID);
        supportedFunctions.add(SourceSystemFunctions.ST_EQUALS);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPADD);
        return supportedFunctions;
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        if (timestampValue.getNanos() == 0) {
            String val = formatDateValue(timestampValue);
            val = val.substring(0, val.length() - 2);
            return "to_date('" + val + "', '" + DATETIME_FORMAT + "')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        return super.translateLiteralTimestamp(timestampValue);
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
        return true;
    }

    @Override
    public boolean supportsExcept() {
        return true;
    }

    @Override
    public boolean supportsIntersect() {
        return true;
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
    public boolean supportsLikeRegex() {
        return true;
    }

    public void setOracleSuppliedDriver(boolean oracleNative) {
        this.oracleSuppliedDriver = oracleNative;
    }

    @TranslatorProperty(display="Oracle Supplied Driver", description="True if the driver is an Oracle supplied driver",advanced=true)
    public boolean isOracleSuppliedDriver() {
        return oracleSuppliedDriver;
    }

    @Override
    protected void registerSpecificTypeOfOutParameter(
            CallableStatement statement, Class<?> runtimeType, int index)
            throws SQLException {
        if (oracleSuppliedDriver) {
            if (runtimeType == RefCursorType.class) {
                statement.registerOutParameter(index, CURSOR_TYPE);
                return;
            } else if (runtimeType == TypeFacility.RUNTIME_TYPES.OBJECT) {
                //TODO: this is not currently handled and oracle will throw an exception.
                //we need additional logic to handle sub types (possibly using the nativeType)
            }
        }
        super.registerSpecificTypeOfOutParameter(statement, runtimeType, index);
    }

    @Override
    public ResultSet executeStoredProcedure(CallableStatement statement,
            List<Argument> preparedValues, Class<?> returnType) throws SQLException {
        ResultSet rs = super.executeStoredProcedure(statement, preparedValues, returnType);
        if (!oracleSuppliedDriver || rs != null) {
            return rs;
        }
        if (returnType == RefCursorType.class) {
            return (ResultSet)statement.getObject(1);
        }
        for (int i = 0; i < preparedValues.size(); i++) {
            Argument arg = preparedValues.get(i);
            if (arg.getType() == RefCursorType.class) {
                return (ResultSet)statement.getObject(i + (returnType == null?1:2));
            }
        }
        return null;
    }

    @Override
    public boolean supportsOnlyFormatLiterals() {
        return true;
    }

    @Override
    public boolean supportsFormatLiteral(String literal,
            org.teiid.translator.ExecutionFactory.Format format) {
        if (format == Format.NUMBER) {
            return false;
        }
        return parseModifier.supportsLiteral(literal);
    }

    @Override
    public boolean supportsArrayType() {
        return true;
    }

    @Override
    @Deprecated
    protected JDBCMetadataProcessor createMetadataProcessor() {
        return (JDBCMetadataProcessor)getMetadataProcessor();
    }

    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new OracleMetadataProcessor();
    }

    @Override
    public boolean supportsCommonTableExpressions() {
        return getVersion().compareTo(NINE_2) >= 0;
    }

    @Override
    public boolean supportsRecursiveCommonTableExpressions() {
        return getVersion().compareTo(ELEVEN_2_0_4) >= 0;
    }

    @Override
    protected boolean supportsGeneratedKeys(ExecutionContext context,
            Command command) {
        if (command instanceof Insert) {
            Insert insert = (Insert)command;
            if (insert.getParameterValues() != null) {
                return false; //bulk inserts result in an exception if keys are flaged for return
            }
        }
        return super.supportsGeneratedKeys(context, command);
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }

    @Override
    public String createTempTable(String string, List<ColumnReference> cols,
            ExecutionContext context, Connection connection) throws SQLException {
        SQLException e1 = null;
        for (int i = 0; i < 5; i++) {
            try {
                return super.createTempTable(string, cols, context, connection);
            } catch (SQLException e) {
                if (e.getErrorCode() == 955) {
                    e1 = e;
                    continue;
                }
                throw e;
            }
        }
        throw e1;
    }

    /**
     * uses a random table name strategy with a
     * retry in the {@link #createTempTable(String, List, ExecutionContext, Connection)} method
     */
    @Override
    public String getTemporaryTableName(String prefix) {
        return prefix + (int)(Math.random() * 10000000);
    }

    @Override
    public String getCreateTemporaryTablePostfix(boolean inTransaction) {
        if (!inTransaction) {
            return "ON COMMIT PRESERVE ROWS"; //$NON-NLS-1$
        }
        return super.getCreateTemporaryTablePostfix(inTransaction) + "; END;"; //$NON-NLS-1$
    }

    @Override
    public String getCreateTemporaryTableString(boolean inTransaction) {
        if (!inTransaction) {
            return super.getCreateTemporaryTableString(inTransaction);
        }
        return "DECLARE PRAGMA AUTONOMOUS_TRANSACTION; BEGIN EXECUTE IMMEDIATE '" + super.getCreateTemporaryTableString(inTransaction); //$NON-NLS-1$
    }

    @Override
    public String getHibernateDialectClassName() {
        if (getVersion().getMajorVersion() >= 10) {
            return "org.hibernate.dialect.Oracle10gDialect"; //$NON-NLS-1$
        }
        return "org.hibernate.dialect.Oracle9iDialect"; //$NON-NLS-1$
    }

    @Override
    public boolean supportsGroupByRollup() {
        return true;
    }

    @Override
    public Expression translateGeometrySelect(Expression expr) {
        return new Function(SourceSystemFunctions.ST_ASGML, Arrays.asList(expr), TypeFacility.RUNTIME_TYPES.CLOB);
    }

    @Override
    public Object retrieveGeometryValue(ResultSet results, int paramIndex) throws SQLException {
        final Clob clob = results.getClob(paramIndex);
        if (clob != null) {
            return new GeometryInputSource() {

                @Override
                public Reader getGml() throws SQLException {
                    return clob.getCharacterStream();
                }

            };
        }
        return null;
    }

    @Override
    public void intializeConnectionAfterCancel(Connection c) throws SQLException {
        //Oracle JDBC has a timing bug with cancel during result set iteration
        //that can cause the next statement on the connection to throw an exception
        //doing an isvalid check seems to allow the connection to be safely reused
        c.isValid(1);
    }

    @Override
    public boolean supportsCorrelatedSubqueryLimit() {
        return false;
    }

    @Override
    public boolean useColumnNamesForGeneratedKeys() {
        return true;
    }

    @Override
    public String translateLiteralBinaryType(BinaryType obj) {
        return "HEXTORAW('" + obj + "')"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public boolean supportsSubqueryInOn() {
        return false; //even oracle 12 still has issues if a with clause is also in the source query
    }

    public boolean supportsConvert(int fromType, int toType) {
        //allow conversion from clob to string
        if (fromType == RUNTIME_CODES.OBJECT || fromType == RUNTIME_CODES.XML || fromType == RUNTIME_CODES.BLOB || toType == RUNTIME_CODES.CLOB || toType == RUNTIME_CODES.XML || toType == RUNTIME_CODES.BLOB) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean supportsBooleanExpressions() {
        return false;
    }

    @Override
    public boolean supportsSelectExpressionArrayType() {
        return false;
    }

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof AggregateFunction) {
            AggregateFunction af = (AggregateFunction)obj;
            if (af.getName().equalsIgnoreCase(LISTAGG) || af.getName().equalsIgnoreCase("STRING_AGG")) { //$NON-NLS-1$
                af.setName(LISTAGG);
                OrderBy order = af.getOrderBy();
                af.setOrderBy(null);
                return Arrays.asList(af, " WITHIN GROUP (", order != null?order:"ORDER BY 1", ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }
        return super.translate(obj, context);
    }

    @Override
    public boolean useUnicodePrefix() {
        return true;
    }

    @Override
    protected boolean isNonAsciiFunction(Function f) {
        return f.getName().equalsIgnoreCase(TO_NCHAR)
                     || (f.getType() == TypeFacility.RUNTIME_TYPES.CHAR && f.getName().equalsIgnoreCase(SourceSystemFunctions.CONVERT));
    }

    @Override
    public boolean isExtendedAscii() {
        return isExtendedAscii;
    }

    @Override
    public boolean useNBindingType() {
        return useNBindingType;
    }

    @Override
    public boolean supportsOnlyTimestampAddLiteral() {
        return true;
    }

    @Override
    public boolean supportsWindowFunctionNthValue() {
        return getVersion().compareTo(ELEVEN_2) >= 0;
    }
}
