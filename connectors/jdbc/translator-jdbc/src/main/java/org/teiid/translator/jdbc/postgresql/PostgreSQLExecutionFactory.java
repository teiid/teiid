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

package org.teiid.translator.jdbc.postgresql;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BOOLEAN;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.GeometryInputSource;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.JsonType;
import org.teiid.language.AggregateFunction;
import org.teiid.language.Array;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Like;
import org.teiid.language.Like.MatchMode;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.ModFunctionModifier;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.oracle.LeftOrRightFunctionModifier;
import org.teiid.translator.jdbc.oracle.MonthOrDayNameFunctionModifier;
import org.teiid.translator.jdbc.oracle.OracleFormatFunctionModifier;
import org.teiid.util.Version;



/**
 * Translator class for PostgreSQL.  Updated to expect a 8.0+ jdbc client
 * @since 4.3
 */
@Translator(name="postgresql", description="A translator for postgreSQL Database")
public class PostgreSQLExecutionFactory extends JDBCExecutionFactory {

    public static String POSTGRESQL = "postgresql"; //$NON-NLS-1$

    static final String UUID_TYPE = "uuid"; //$NON-NLS-1$
    private static final String INTEGER_TYPE = "integer"; //$NON-NLS-1$

    private static final class NonIntegralNumberToBoolean extends
            FunctionModifier {
        @Override
        public List<?> translate(Function function) {
            return Arrays.asList(function.getParameters().get(0), " <> 0"); //$NON-NLS-1$
        }
    }

    private final class PostgreSQLFormatFunctionModifier extends
            OracleFormatFunctionModifier {
        private PostgreSQLFormatFunctionModifier(String prefix, boolean parse) {
            super(prefix, parse);
        }

        protected Object convertToken(String group) {
            Object result = PostgreSQLExecutionFactory.this.convertToken(group);
            if (result == null) {
                return super.convertToken(group);
            }
            return result;
        }
    }

    public static final Version EIGHT_0 = Version.getVersion("8.0"); //$NON-NLS-1$
    public static final Version EIGHT_1 = Version.getVersion("8.1"); //$NON-NLS-1$
    public static final Version EIGHT_2 = Version.getVersion("8.2"); //$NON-NLS-1$
    public static final Version EIGHT_3 = Version.getVersion("8.3"); //$NON-NLS-1$
    public static final Version EIGHT_4 = Version.getVersion("8.4"); //$NON-NLS-1$
    public static final Version NINE_0 = Version.getVersion("9.0"); //$NON-NLS-1$
    public static final Version NINE_3 = Version.getVersion("9.3"); //$NON-NLS-1$
    public static final Version NINE_4 = Version.getVersion("9.4"); //$NON-NLS-1$
    protected OracleFormatFunctionModifier parseModifier = new PostgreSQLFormatFunctionModifier("TO_TIMESTAMP(", true); //$NON-NLS-1$

    //postgis versions
    public static final Version ONE_3 = Version.getVersion("1.3"); //$NON-NLS-1$
    public static final Version ONE_4 = Version.getVersion("1.4"); //$NON-NLS-1$
    public static final Version ONE_5 = Version.getVersion("1.5"); //$NON-NLS-1$
    public static final Version TWO_0 = Version.getVersion("2.0"); //$NON-NLS-1$

    private Version postGisVersion = Version.DEFAULT_VERSION;
    private boolean projSupported = false;

    protected ConvertModifier convertModifier;

    public PostgreSQLExecutionFactory() {
        setMaxDependentInPredicates(1);
        setMaxInCriteriaSize(Short.MAX_VALUE - 50); //set a value that is safely smaller than the max in case there are other parameters
    }

    /**
     * Convert to a new parsing token or return null if not possible
     * @param group
     * @return
     */
    public Object convertToken(String group) {
        switch (group.charAt(0)) {
        case 'Z':
            return "TZ"; //$NON-NLS-1$
        case 'S':
            if (group.length() > 3) {
                return "US"; //$NON-NLS-1$
            }
            return "MS"; //$NON-NLS-1$
        }
        return null;
    }

    public void start() throws TranslatorException {
        //TODO: all of the functions (except for convert) can be handled through just the escape syntax
        super.start();

        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG10, new AliasModifier("log")); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("&")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("~")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("|")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("#")); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("||")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                List<Object> parts = new ArrayList<Object>();
                parts.add("substring("); //$NON-NLS-1$
                parts.add(function.getParameters().get(0));
                parts.add(" from "); //$NON-NLS-1$
                Expression index = function.getParameters().get(1);
                if (!(index instanceof Literal)) {
                    parts.add("case sign("); //$NON-NLS-1$
                    parts.add(index);
                    parts.add(") when -1 then length("); //$NON-NLS-1$
                    parts.add(function.getParameters().get(0));
                    parts.add(") + 1 + "); //$NON-NLS-1$
                    parts.add(index);
                    parts.add(" when 0 then 1 else "); //$NON-NLS-1$
                    parts.add(index);
                    parts.add(" end"); //$NON-NLS-1$
                } else {
                    parts.add(index);
                }
                if (function.getParameters().size() > 2) {
                    parts.add(" for "); //$NON-NLS-1$
                    parts.add(function.getParameters().get(2));
                }
                parts.add(")"); //$NON-NLS-1$
                return parts;
            }
        });
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Day"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.HOUR, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Month"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.SECOND, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.WEEK, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractFunctionModifier(INTEGER_TYPE));
        registerFunctionModifier(SourceSystemFunctions.EPOCH, new ExtractFunctionModifier(null));
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, parseModifier);
        registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new PostgreSQLFormatFunctionModifier("TO_CHAR(", false)); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier("%", getLanguageFactory(), Arrays.asList(TypeFacility.RUNTIME_TYPES.BIG_INTEGER, TypeFacility.RUNTIME_TYPES.BIG_DECIMAL))); //$NON-NLS-1$

        //specific to 8.2 client or later
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.RAND, new AliasModifier("random")); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.ARRAY_GET, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                //TODO: doesn't work for all array expressions
                return Arrays.asList(function.getParameters().get(0), '[', function.getParameters().get(1), ']');
            }
        });
        registerFunctionModifier(SourceSystemFunctions.ARRAY_LENGTH, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                if (function.getParameters().size() == 1) {
                    function.getParameters().add(new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER));
                }
                return null;
            }
        });
        registerFunctionModifier(SourceSystemFunctions.ROUND, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                if (function.getParameters().size() > 1) {
                    Expression ex = function.getParameters().get(0);
                    if (ex.getType() == TypeFacility.RUNTIME_TYPES.DOUBLE || ex.getType() == TypeFacility.RUNTIME_TYPES.FLOAT) {
                        if (function.getParameters().get(1) instanceof Literal && Integer.valueOf(0).equals(((Literal)function.getParameters().get(1)).getValue())) {
                            function.getParameters().remove(1);
                        } else {
                            function.getParameters().set(0, new Function(SourceSystemFunctions.CONVERT, Arrays.asList(ex, new Literal("bigdecimal", TypeFacility.RUNTIME_TYPES.STRING)), TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)); //$NON-NLS-1$
                        }
                    }
                }
                return null;
            }
        });

        convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convertModifier.addTypeMapping("smallint", FunctionModifier.BYTE, FunctionModifier.SHORT); //$NON-NLS-1$
        convertModifier.addTypeMapping(INTEGER_TYPE, FunctionModifier.INTEGER);
        convertModifier.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("float8", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("numeric(38)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
        convertModifier.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
        convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        convertModifier.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
        convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convertModifier.addTypeMapping("geography", FunctionModifier.GEOGRAPHY); //$NON-NLS-1$
        convertModifier.addTypeMapping("geometry", FunctionModifier.GEOMETRY); //$NON-NLS-1$
        convertModifier.addTypeMapping("json", FunctionModifier.JSON); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.BIGDECIMAL, FunctionModifier.BOOLEAN, new NonIntegralNumberToBoolean());
        convertModifier.addConvert(FunctionModifier.FLOAT, FunctionModifier.BOOLEAN, new NonIntegralNumberToBoolean());
        convertModifier.addConvert(FunctionModifier.BIGDECIMAL, FunctionModifier.BOOLEAN, new NonIntegralNumberToBoolean());
        convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.TIMESTAMP, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList(function.getParameters().get(0), " + TIMESTAMP '1970-01-01'"); //$NON-NLS-1$
            }
        });
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(date_trunc('second', ", function.getParameters().get(0), ") AS time)"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD")); //$NON-NLS-1$ //$NON-NLS-2$
        convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "HH24:MI:SS")); //$NON-NLS-1$ //$NON-NLS-2$
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD HH24:MI:SS.US")); //$NON-NLS-1$ //$NON-NLS-2$
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                Expression stringValue = function.getParameters().get(0);
                return Arrays.asList("CASE WHEN ", stringValue, " THEN 'true' WHEN not(", stringValue, ") THEN 'false' END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        convertModifier.addSourceConversion(new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                ((Literal)function.getParameters().get(1)).setValue(INTEGER_TYPE);
                return null;
            }
        }, FunctionModifier.BOOLEAN);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);

        //standard function form of several predicates
        addPushDownFunction(POSTGRESQL, "ilike", BOOLEAN, STRING, STRING).setProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, "($1 ilike $2)"); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(POSTGRESQL, "rlike", BOOLEAN, STRING, STRING).setProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, "($1 ~ $2)"); //$NON-NLS-1$ //$NON-NLS-2$
        addPushDownFunction(POSTGRESQL, "iregexp", BOOLEAN, STRING, STRING).setProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, "($1 ~* $2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public void initCapabilities(Connection connection)
            throws TranslatorException {
        super.initCapabilities(connection);
        if (getVersion().compareTo(NINE_0) <= 0) {
            registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        }
        if (this.postGisVersion.compareTo(Version.DEFAULT_VERSION) != 0 || connection == null) {
            return;
        }
        Statement s = null;
        ResultSet rs = null;
        try {
            s = connection.createStatement();
            rs = s.executeQuery("SELECT 1 FROM pg_catalog.pg_proc where lower(proname) = 'postgis_full_version'"); //$NON-NLS-1$
            if (!rs.next()) {
                //no such function breakout
                return;
            }
            rs.close();
            rs = s.executeQuery("SELECT PostGIS_Full_Version()"); //$NON-NLS-1$
            rs.next();
            String versionInfo = rs.getString(1);
            if (versionInfo != null) {
                if (versionInfo.contains("PROJ=")) { //$NON-NLS-1$
                    projSupported = true;
                }
                int index = versionInfo.indexOf("POSTGIS="); //$NON-NLS-1$
                if (index > -1) {
                    String version = versionInfo.substring(index+9, versionInfo.indexOf('"', index+9));
                    this.setPostGisVersion(version);
                }
            }
        } catch (SQLException e) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not determine PostGIS version"); //$NON-NLS-1$
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {

            }
            try {
                if (s != null) {
                    s.close();
                }
            } catch (SQLException e) {

            }
        }
    }


    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "TRUE"; //$NON-NLS-1$
        }
        return "FALSE"; //$NON-NLS-1$
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
    public int getTimestampNanoPrecision() {
        return 6;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        if (limit.getRowOffset() > 0) {
            return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return null;
    }

    /**
     * Postgres doesn't provide min/max(boolean), so this conversion writes a min(BooleanValue) as
     * bool_and(BooleanValue)
     * @see org.teiid.language.visitor.LanguageObjectVisitor#visit(org.teiid.language.AggregateFunction)
     * @since 4.3
     */
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof AggregateFunction) {
            AggregateFunction agg = (AggregateFunction)obj;
            if (agg.getParameters().size() == 1 && TypeFacility.RUNTIME_TYPES.BOOLEAN.equals(agg.getParameters().get(0).getType())) {
                if (agg.getName().equalsIgnoreCase(NonReserved.MIN)) {
                    agg.setName("bool_and"); //$NON-NLS-1$
                } else if (agg.getName().equalsIgnoreCase(NonReserved.MAX)) {
                    agg.setName("bool_or"); //$NON-NLS-1$
                }
            }
        } else if (obj instanceof Like) {
            Like like = (Like)obj;
            if (like.getMode() == MatchMode.REGEX) {
                return Arrays.asList(like.getLeftExpression(), like.isNegated()?" !~ ":" ~ ", like.getRightExpression()); //$NON-NLS-1$ //$NON-NLS-2$
            } else if (like.getEscapeCharacter() == null) {
                return addDefaultEscape(like);
            }
        }
        return super.translate(obj, context);
    }

    /**
     * Add a default escape
     * @param like
     * @return
     */
    public static List<Object> addDefaultEscape(Like like) {
        return Arrays.asList(like.getLeftExpression(),
                like.isNegated()?" NOT ":" ", like.getMode()==MatchMode.LIKE?"LIKE ":"SIMILAR TO ", like.getRightExpression(), " ESCAPE ''"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.HIGH;
    }

    @Override
    public boolean supportsOrderByNullOrdering() {
        return getVersion().compareTo(EIGHT_4) >= 0;
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
        supportedFunctions.add("BITAND"); //$NON-NLS-1$
        supportedFunctions.add("BITNOT"); //$NON-NLS-1$
        supportedFunctions.add("BITOR"); //$NON-NLS-1$
        supportedFunctions.add("BITXOR"); //$NON-NLS-1$
        supportedFunctions.add("CEILING"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATBIGDECIMAL"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATBIGINTEGER"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATDOUBLE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATFLOAT"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATINTEGER"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATLONG"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("PI"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("ROUND"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$

        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        if (getVersion().compareTo(NINE_0) > 0) {
            supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        }
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$

        // These are executed within the server and never pushed down
//        supportedFunctions.add("CURDATE"); //$NON-NLS-1$
//        supportedFunctions.add("CURTIME"); //$NON-NLS-1$
//        supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.EPOCH);
        if (this.getVersion().compareTo(EIGHT_2) >= 0) {
            supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$

            //only year and day match our expectations
            //supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        }
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$

        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL"); //$NON-NLS-1$

        // Additional functions
//        // Math
//        supportedFunctions.add("%"); //$NON-NLS-1$
//        supportedFunctions.add("^"); //$NON-NLS-1$
//        supportedFunctions.add("|/"); //$NON-NLS-1$
//        supportedFunctions.add("||/"); //$NON-NLS-1$
//        supportedFunctions.add("!"); //$NON-NLS-1$
//        supportedFunctions.add("!!"); //$NON-NLS-1$
//        supportedFunctions.add("@"); //$NON-NLS-1$
//          // Bit manipulation
//        supportedFunctions.add("&"); //$NON-NLS-1$
//        supportedFunctions.add("|"); //$NON-NLS-1$
//        supportedFunctions.add("#"); //$NON-NLS-1$
//        supportedFunctions.add("~"); //$NON-NLS-1$
//        supportedFunctions.add("<<"); //$NON-NLS-1$
//        supportedFunctions.add(">>"); //$NON-NLS-1$
//
//        supportedFunctions.add("CBRT"); //$NON-NLS-1$
//        supportedFunctions.add("CEIL"); //$NON-NLS-1$
//        supportedFunctions.add("LN"); //$NON-NLS-1$
//        supportedFunctions.add("MOD"); //$NON-NLS-1$
//        supportedFunctions.add("RANDOM"); //$NON-NLS-1$
//        supportedFunctions.add("SETSEED"); //$NON-NLS-1$
//        supportedFunctions.add("TRUNC"); //$NON-NLS-1$
//        supportedFunctions.add("WIDTH_BUCKET"); //$NON-NLS-1$
//
//        // String
//        supportedFunctions.add("BIT_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("BTRIM"); //$NON-NLS-1$
//        supportedFunctions.add("CHAR_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("CHARACTER_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("DECODE"); //$NON-NLS-1$
//        supportedFunctions.add("ENCODE"); //$NON-NLS-1$
//        supportedFunctions.add("MD5"); //$NON-NLS-1$
//        supportedFunctions.add("OCTET_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("PG_CLIENT_ENCODING"); //$NON-NLS-1$
//        supportedFunctions.add("QUOTE_IDENT"); //$NON-NLS-1$
//        supportedFunctions.add("QUOTE_LITERAL"); //$NON-NLS-1$
//        supportedFunctions.add("SPLIT_PART"); //$NON-NLS-1$
//        supportedFunctions.add("STRPOS"); //$NON-NLS-1$
//        supportedFunctions.add("SUBSTR"); //$NON-NLS-1$
//        supportedFunctions.add("TO_ASCII"); //$NON-NLS-1$
//        supportedFunctions.add("TO_HEX"); //$NON-NLS-1$
//        supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
//
//        // Bit operations
//        supportedFunctions.add("GET_BIT"); //$NON-NLS-1$
//        supportedFunctions.add("GET_BYTE"); //$NON-NLS-1$
//        supportedFunctions.add("SET_BIT"); //$NON-NLS-1$
//        supportedFunctions.add("SET_BYTE"); //$NON-NLS-1$
//
//        // Formatting
//        supportedFunctions.add("TO_CHAR"); //$NON-NLS-1$
//        supportedFunctions.add("TO_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("TO_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("TO_NUMBER"); //$NON-NLS-1$
//
//        // Date / Time
//        supportedFunctions.add("AGE"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_DATE"); //$NON-NLS-1$            // no ()
//        supportedFunctions.add("CURRENT_TIME"); //$NON-NLS-1$            // no ()
//        supportedFunctions.add("CURRENT_TIMESTAMP"); //$NON-NLS-1$       // no ()
//        supportedFunctions.add("DATE_PART"); //$NON-NLS-1$
//        supportedFunctions.add("DATE_TRUNC"); //$NON-NLS-1$
//        supportedFunctions.add("ISFINITE"); //$NON-NLS-1$
//        supportedFunctions.add("JUSTIFY_HOURS"); //$NON-NLS-1$
//        supportedFunctions.add("JUSTIFY_DAYS"); //$NON-NLS-1$
//        supportedFunctions.add("LOCALTIME"); //$NON-NLS-1$               // no ()
//        supportedFunctions.add("LOCALTIMESTAMP"); //$NON-NLS-1$          // no ()
//        supportedFunctions.add("TIMEOFDAY"); //$NON-NLS-1$
//
//        // Conditional
          supportedFunctions.add("COALESCE"); //$NON-NLS-1$
//        supportedFunctions.add("NULLIF"); //$NON-NLS-1$
//        supportedFunctions.add("GREATEST"); //$NON-NLS-1$
//        supportedFunctions.add("LEAST"); //$NON-NLS-1$
//
//        // Network Addresses
////        supportedFunctions.add("BROADCAST"); //$NON-NLS-1$
////        supportedFunctions.add("HOST"); //$NON-NLS-1$
////        supportedFunctions.add("MASKLEN"); //$NON-NLS-1$
////        supportedFunctions.add("SET_MASKLEN"); //$NON-NLS-1$
////        supportedFunctions.add("NETMASK"); //$NON-NLS-1$
////        supportedFunctions.add("HOSTMASK"); //$NON-NLS-1$
////        supportedFunctions.add("NETWORK"); //$NON-NLS-1$
////        supportedFunctions.add("TEXT"); //$NON-NLS-1$
////        supportedFunctions.add("ABBREV"); //$NON-NLS-1$
////        supportedFunctions.add("FAMILY"); //$NON-NLS-1$
////        supportedFunctions.add("TRUNC"); //$NON-NLS-1$
//
//        // Set generator
//        supportedFunctions.add("GENERATE_SERIES"); //$NON-NLS-1$
//
//        // Information
//        supportedFunctions.add("CURRENT_DATABASE"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_SCHEMA"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_SCHEMAS"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_USER"); //$NON-NLS-1$           // no ()
//        supportedFunctions.add("INET_CLIENT_ADDR"); //$NON-NLS-1$
//        supportedFunctions.add("INET_CLIENT_PORT"); //$NON-NLS-1$
//        supportedFunctions.add("INET_SERVER_ADDR"); //$NON-NLS-1$
//        supportedFunctions.add("INET_SERVER_PORT"); //$NON-NLS-1$
//        supportedFunctions.add("SESSION_USER"); //$NON-NLS-1$           // no ()
//        supportedFunctions.add("USER"); //$NON-NLS-1$                   // no ()
//        supportedFunctions.add("VERSION"); //$NON-NLS-1$
//
        supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        supportedFunctions.add(SourceSystemFunctions.ARRAY_LENGTH);
        supportedFunctions.add(SourceSystemFunctions.FORMATTIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);

        if (this.postGisVersion.compareTo(ONE_3) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.ST_ASBINARY);
            supportedFunctions.add(SourceSystemFunctions.ST_ASTEXT);
            supportedFunctions.add(SourceSystemFunctions.ST_CONTAINS);
            supportedFunctions.add(SourceSystemFunctions.ST_CROSSES);
            supportedFunctions.add(SourceSystemFunctions.ST_DISJOINT);
            supportedFunctions.add(SourceSystemFunctions.ST_DISTANCE);
            supportedFunctions.add(SourceSystemFunctions.ST_EQUALS);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMTEXT);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMWKB);
            supportedFunctions.add(SourceSystemFunctions.ST_INTERSECTS);
            supportedFunctions.add(SourceSystemFunctions.ST_OVERLAPS);
            supportedFunctions.add(SourceSystemFunctions.ST_SETSRID);
            supportedFunctions.add(SourceSystemFunctions.ST_SRID);
            supportedFunctions.add(SourceSystemFunctions.ST_TOUCHES);
            supportedFunctions.add(SourceSystemFunctions.ST_HASARC);
            supportedFunctions.add(SourceSystemFunctions.ST_SIMPLIFY);
            supportedFunctions.add(SourceSystemFunctions.ST_FORCE_2D);
            supportedFunctions.add(SourceSystemFunctions.ST_ENVELOPE);
            supportedFunctions.add(SourceSystemFunctions.ST_WITHIN);
            supportedFunctions.add(SourceSystemFunctions.ST_DWITHIN);
            supportedFunctions.add(SourceSystemFunctions.ST_EXTENT);
            supportedFunctions.add(SourceSystemFunctions.DOUBLE_AMP_OP);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMEWKT);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMEWKB);
            supportedFunctions.add(SourceSystemFunctions.ST_ASEWKB);
            supportedFunctions.add(SourceSystemFunctions.ST_ASEWKT);
            supportedFunctions.add(SourceSystemFunctions.ST_AREA);
            supportedFunctions.add(SourceSystemFunctions.ST_BOUNDARY);
            supportedFunctions.add(SourceSystemFunctions.ST_BUFFER);
            supportedFunctions.add(SourceSystemFunctions.ST_CENTROID);
            supportedFunctions.add(SourceSystemFunctions.ST_COORDDIM);
            supportedFunctions.add(SourceSystemFunctions.ST_CONVEXHULL);
            supportedFunctions.add(SourceSystemFunctions.ST_DIFFERENCE);
            supportedFunctions.add(SourceSystemFunctions.ST_DIMENSION);
            supportedFunctions.add(SourceSystemFunctions.ST_ENDPOINT);
            supportedFunctions.add(SourceSystemFunctions.ST_EXTERIORRING);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMETRYN);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMETRYTYPE);
            supportedFunctions.add(SourceSystemFunctions.ST_INTERSECTION);
            supportedFunctions.add(SourceSystemFunctions.ST_INTERIORRINGN);
            supportedFunctions.add(SourceSystemFunctions.ST_ISCLOSED);
            supportedFunctions.add(SourceSystemFunctions.ST_ISEMPTY);
            supportedFunctions.add(SourceSystemFunctions.ST_ISRING);
            supportedFunctions.add(SourceSystemFunctions.ST_ISSIMPLE);
            supportedFunctions.add(SourceSystemFunctions.ST_ISVALID);
            supportedFunctions.add(SourceSystemFunctions.ST_LENGTH);
            supportedFunctions.add(SourceSystemFunctions.ST_NUMGEOMETRIES);
            supportedFunctions.add(SourceSystemFunctions.ST_NUMINTERIORRINGS);
            supportedFunctions.add(SourceSystemFunctions.ST_NUMPOINTS);
            supportedFunctions.add(SourceSystemFunctions.ST_ORDERINGEQUALS);
            supportedFunctions.add(SourceSystemFunctions.ST_PERIMETER);
            supportedFunctions.add(SourceSystemFunctions.ST_POINT);
            supportedFunctions.add(SourceSystemFunctions.ST_POINTN);
            supportedFunctions.add(SourceSystemFunctions.ST_POINTONSURFACE);
            supportedFunctions.add(SourceSystemFunctions.ST_POLYGON);
            supportedFunctions.add(SourceSystemFunctions.ST_RELATE);
            supportedFunctions.add(SourceSystemFunctions.ST_STARTPOINT);
            supportedFunctions.add(SourceSystemFunctions.ST_SYMDIFFERENCE);
            supportedFunctions.add(SourceSystemFunctions.ST_UNION);
            supportedFunctions.add(SourceSystemFunctions.ST_X);
            supportedFunctions.add(SourceSystemFunctions.ST_Y);
            supportedFunctions.add(SourceSystemFunctions.ST_SNAPTOGRID);
            supportedFunctions.add(SourceSystemFunctions.ST_SIMPLIFYPRESERVETOPOLOGY);
        }
        if (this.postGisVersion.compareTo(ONE_4) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.ST_ASGEOJSON);
            supportedFunctions.add(SourceSystemFunctions.ST_ASGML);
            supportedFunctions.add(SourceSystemFunctions.ST_CURVETOLINE);
            supportedFunctions.add(SourceSystemFunctions.ST_Z);
        }
        if (this.postGisVersion.compareTo(ONE_5) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMGML);
            supportedFunctions.add(SourceSystemFunctions.ST_MAKEENVELOPE);
        }
        if (this.postGisVersion.compareTo(TWO_0) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMGEOJSON);
        }
        if (this.projSupported) {
            supportedFunctions.add(SourceSystemFunctions.ST_TRANSFORM);
            supportedFunctions.add(SourceSystemFunctions.ST_ASKML);
        }
        supportedFunctions.add("pg_catalog.encode"); //$NON-NLS-1$
        return supportedFunctions;
    }

    /**
     * This is true only after Postgre version 7.1
     * However, since version 7 was released in 2000 we'll assume a post 7 instance.
     */
    public boolean supportsInlineViews() {
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
        return getVersion().compareTo(EIGHT_2) >= 0;
    }

    @Override
    public boolean supportsCommonTableExpressions() {
        return getVersion().compareTo(EIGHT_4) >= 0;
    }

    @Override
    public boolean supportsRecursiveCommonTableExpressions() {
        return supportsCommonTableExpressions();
    }

    @Override
    public boolean supportsArrayAgg() {
        return false;
        //return getVersion().compareTo(EIGHT_4) >= 0;
    }

    @Override
    public boolean supportsElementaryOlapOperations() {
        return getVersion().compareTo(EIGHT_4) >= 0;
    }

    @Override
    public boolean supportsAdvancedOlapOperations() {
        return getVersion().compareTo(NINE_4) >= 0;
    }

    @Override
    public boolean supportsWindowDistinctAggregates() {
        return false;
    }

    @Override
    public boolean supportsSimilarTo() {
        return true;
    }

    @Override
    public boolean supportsLikeRegex() {
        return true;
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
    public boolean supportsSelectExpressionArrayType() {
        return false;
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public boolean supportsStringAgg() {
        return getVersion().compareTo(NINE_0) >= 0;
    }

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }

    @Override
    public String getHibernateDialectClassName() {
        if (getVersion().compareTo(EIGHT_2) >= 0) {
            return "org.hibernate.dialect.PostgreSQL82Dialect"; //$NON-NLS-1$
        }
        return "org.hibernate.dialect.PostgreSQL81Dialect"; //$NON-NLS-1$
    }

    @Override
    public String getCreateTemporaryTablePostfix(boolean inTransaction) {
        if (!inTransaction) {
            return "ON COMMIT PRESERVE ROWS"; //$NON-NLS-1$
        }
        return super.getCreateTemporaryTablePostfix(inTransaction);
    }

    /**
     * pg needs to collect stats for effective planning
     */
    @Override
    public void loadedTemporaryTable(String tableName,
            ExecutionContext context, Connection connection) throws SQLException {
        Statement s = connection.createStatement();
        try {
            s.execute("ANALYZE " + tableName); //$NON-NLS-1$
        } finally {
            try {
                s.close();
            } catch (SQLException e) {

            }
        }
    }

    @Override
    public SQLConversionVisitor getSQLConversionVisitor() {
        return new PostgreSQLConversionVisitor(this);
    }

    public void setPostGisVersion(String postGisVersion) {
        this.postGisVersion = Version.getVersion(postGisVersion);
    }

    @TranslatorProperty(display="PostGIS Version", description="The version of the PostGIS extension.",advanced=true)
    public String getPostGisVersion() {
        return postGisVersion.toString();
    }

    @TranslatorProperty(display="Proj support enabled", description="If PostGIS Proj support is enabled for ST_TRANSFORM",advanced=true)
    public boolean isProjSupported() {
        return projSupported;
    }

    public void setProjSupported(boolean projSupported) {
        this.projSupported = projSupported;
    }

    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new PostgreSQLMetadataProcessor();
    }

    @Override
    public Expression translateGeometrySelect(Expression expr) {
        return new Function(SourceSystemFunctions.ST_ASEWKB, Arrays.asList(expr), TypeFacility.RUNTIME_TYPES.VARBINARY);
    }

    @Override
    public Expression translateGeographySelect(Expression expr) {
        return new Function(SourceSystemFunctions.ST_ASEWKB, Arrays.asList(
                new Function("CAST", //$NON-NLS-1$
                        Arrays.asList(expr, new Literal("geometry", TypeFacility.RUNTIME_TYPES.STRING)), //$NON-NLS-1$
                        TypeFacility.RUNTIME_TYPES.GEOMETRY)),
                TypeFacility.RUNTIME_TYPES.VARBINARY);
    }

    @Override
    public Object retrieveGeometryValue(ResultSet results, int paramIndex) throws SQLException {
        //geometry strategy includes the srid
        final byte[] bytes = results.getBytes(paramIndex);
        if (bytes != null) {
            return new GeometryInputSource() {
                @Override
                public InputStream getEwkb() throws Exception {
                    return new ByteArrayInputStream(bytes);
                }
            };
        }
        return null;
    }

    @Override
    public Object retrieveGeographyValue(ResultSet results, int paramIndex)
            throws SQLException {
        return retrieveGeometryValue(results, paramIndex);
        /*//geography for now only includes the wkb
        //to include the srid
        final byte[] bytes = results.getBytes(paramIndex);
        if (bytes != null) {
            return new GeographyType(bytes);
        }
        return null;*/
    }

    @Override
    public boolean useStreamsForLobs() {
        return true; //lob bindings require a transaction
    }

    @Override
    public String translateLiteralBinaryType(BinaryType obj) {
        return "E'\\\\x" + obj + '\''; //$NON-NLS-1$
    }

    @Override
    public boolean supportsLateralJoin() {
        return getVersion().compareTo(NINE_3) >= 0;
    }

    @Override
    public void bindValue(PreparedStatement stmt, Object param,
            Class<?> paramType, int i) throws SQLException {
        if (param == null && (paramType == TypeFacility.RUNTIME_TYPES.BLOB
                || paramType == TypeFacility.RUNTIME_TYPES.GEOMETRY
                || paramType == TypeFacility.RUNTIME_TYPES.GEOGRAPHY)) {
            //the blob sql type causes a failure with nulls
            paramType = TypeFacility.RUNTIME_TYPES.VARBINARY;
        } else if (param instanceof Array) {
            //pg allows for the direct binding of certain arrays
            Array array = (Array)param;
            Connection c = stmt.getConnection();
            int code = ConvertModifier.getCode(array.getBaseType());
            String nativeType = convertModifier.getSimpleTypeMapping(code);
            int index = nativeType.indexOf('(');
            if (index > 0) {
                nativeType = nativeType.substring(0, index);
            }
            Object[] values = new Object[array.getExpressions().size()];
            for (int j = 0; j < values.length; j++) {
                Expression ex = array.getExpressions().get(j);
                values[j] = ((Literal)ex).getValue();
            }
            java.sql.Array value = c.createArrayOf(nativeType, values);
            stmt.setArray(i, value);
            return;
        }
        super.bindValue(stmt, param, paramType, i);
    }

    @Override
    public boolean supportsIsDistinctCriteria() {
        return true;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGeographyType() {
        return this.postGisVersion.compareTo(ONE_5) >= 0;
    }

    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex,
            Class<?> expectedType) throws SQLException {
        if (expectedType == TypeFacility.RUNTIME_TYPES.JSON) {
            return new JsonType(new ClobImpl(results.getString(parameterIndex)));
        }
        return super.retrieveValue(results, parameterIndex, expectedType);
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
            Class<?> expectedType) throws SQLException {
        if (expectedType == TypeFacility.RUNTIME_TYPES.JSON) {
            return new JsonType(new ClobImpl(results.getString(columnIndex)));
        }
        try {
            return super.retrieveValue(results, columnIndex, expectedType);
        } catch (SQLException e) {
            if (expectedType == TypeFacility.RUNTIME_TYPES.TIMESTAMP) {
                //redshift cannot convert timestamptz to timestamp at the driver level
                String value = (String) super.retrieveValue(results, columnIndex, TypeFacility.RUNTIME_TYPES.STRING);
                if (value != null && value.length() > 10) {
                    return convertTimestampTZ(value);
                }
            }
            throw e;
        }
    }

    static Object convertTimestampTZ(String value) {
        char[] chars = value.toCharArray();
        chars[10] = 'T'; //convert to the iso format
        ZonedDateTime dt = ZonedDateTime.parse(CharBuffer.wrap(chars), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        Timestamp ts = new Timestamp(dt.toEpochSecond()*1000);
        ts.setNanos(dt.getNano());
        return ts;
    }

    @Override
    public int getMaxProjectedColumns() {
        //in practice this could be lower depending on the block size, so it may need to be settable
        return 1600;
    }

    @Override
    public boolean preserveNullTyping() {
        return true;
    }

}
