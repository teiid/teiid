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
package org.teiid.translator.jdbc.sybase;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.core.types.BinaryType;
import org.teiid.core.util.StringUtil;
import org.teiid.language.Command;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.ModFunctionModifier;
import org.teiid.translator.jdbc.ParseFormatFunctionModifier;
import org.teiid.translator.jdbc.oracle.ConcatFunctionModifier;
import org.teiid.util.Version;


@Translator(name="sybase", description="A translator for Sybase Database")
public class SybaseExecutionFactory extends BaseSybaseExecutionFactory {

    public static final class SybaseFormatFunctionModifier extends
            ParseFormatFunctionModifier {
        private Map<String, Object> formatMap;

        public SybaseFormatFunctionModifier(String prefix, Map<String, Object> formatMap) {
            super(prefix);
            this.formatMap = formatMap;
        }

        @Override
        protected void translateFormat(List<Object> result,
                Expression expression, String value) {
            Object format = translateFormat(value);
            if (format instanceof String) {
                result.add("convert("); //$NON-NLS-1$
                result.add(format);
                result.add(", "); //$NON-NLS-1$
                result.add(expression);
                result.add(")"); //$NON-NLS-1$
            } else {
                super.translateFormat(result, expression, value);
            }
        }

        @Override
        protected Object translateFormat(String format) {
            return formatMap.get(format);
        }
    }

    public static final Version TWELVE_5_3 = Version.getVersion("12.5.3"); //$NON-NLS-1$
    public static final Version TWELVE_5 = Version.getVersion("12.5"); //$NON-NLS-1$
    public static final Version FIFTEEN_0_2 = Version.getVersion("15.0.2"); //$NON-NLS-1$
    public static final Version FIFTEEN_5 = Version.getVersion("15.5"); //$NON-NLS-1$

    protected Map<String, Object> formatMap = new HashMap<String, Object>();
    protected boolean jtdsDriver;
    protected ConvertModifier convertModifier = new ConvertModifier();

    public SybaseExecutionFactory() {
        setSupportsFullOuterJoins(false);
        setMaxInCriteriaSize(250);
        setMaxDependentInPredicates(7);
        populateDateFormats();
    }

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
        formatMap.put("yyddMM", 13); //$NON-NLS-1$
        formatMap.put("MM/yy/dd", 14); //$NON-NLS-1$
        formatMap.put("dd/yy/MM", 15); //$NON-NLS-1$
        formatMap.put("MMM dd yy HH:mm:ss", 16); //$NON-NLS-1$
        for (Map.Entry<String, Object> entry : new HashSet<Map.Entry<String, Object>>(formatMap.entrySet())) {
            formatMap.put(entry.getKey().replace("yy", "yyyy"), (Integer)entry.getValue() + 100); //$NON-NLS-1$ //$NON-NLS-2$
        }

        formatMap.put("MMM d yyyy hh:mma", 100); //$NON-NLS-1$
        formatMap.put("HH:mm:ss", 8); //$NON-NLS-1$
        formatMap.put("MMM d yyyy hh:mm:ss:SSSa", 109); //$NON-NLS-1$
        formatMap.put("hh:mma", 17); //$NON-NLS-1$
        formatMap.put("HH:mm", 18); //$NON-NLS-1$
        formatMap.put("hh:mm:ss:SSSa", 19); //$NON-NLS-1$
        formatMap.put("HH:mm:ss:SSS", 20); //$NON-NLS-1$
        formatMap.put("yy/MM/dd HH:mm:ss", 21); //$NON-NLS-1$
        formatMap.put("yy/MM/dd hh:mm:ssa", 22); //$NON-NLS-1$
        formatMap.put("yyyy-MM-dd'T'HH:mm:ss", 23); //$NON-NLS-1$
    }

    public void start() throws TranslatorException {
        super.start();
        registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier("%", getLanguageFactory())); //$NON-NLS-1$
        if (nullPlusNonNullIsNull()) {
            registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("+")); //$NON-NLS-1$
        } else {
            registerFunctionModifier(SourceSystemFunctions.CONCAT, new ConcatFunctionModifier(getLanguageFactory()) {
                @Override
                public List<?> translate(Function function) {
                    function.setName("+"); //$NON-NLS-1$
                    return super.translate(function);
                }
            });
        }
        registerFunctionModifier(SourceSystemFunctions.LPAD, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                List<Expression> params = function.getParameters();
                List<Object> result = new ArrayList<Object>();
                if (!nullPlusNonNullIsNull()) {
                    result.add("CASE WHEN "); //$NON-NLS-1$
                    result.add(params.get(0));
                    result.add(" IS NULL"); //$NON-NLS-1$
                    if (params.size()>2) {
                        result.add(" OR "); //$NON-NLS-1$
                        result.add(params.get(2));
                        result.add(" IS NULL"); //$NON-NLS-1$
                    }
                    result.add(" THEN NULL ELSE "); //$NON-NLS-1$
                }

                result.addAll(Arrays.asList("RIGHT(REPLICATE(", //$NON-NLS-1$
                        params.size()>2?params.get(2):new Literal(" ", TypeFacility.RUNTIME_TYPES.STRING), ", ", //$NON-NLS-1$ //$NON-NLS-2$
                                params.get(1), ") + LEFT(", params.get(0), ", ", params.get(1), "), ", params.get(1), ")")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

                if (!nullPlusNonNullIsNull()) {
                    result.add(" END"); //$NON-NLS-1$
                }

                return result;
            }
        });
        registerFunctionModifier(SourceSystemFunctions.RPAD, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                List<Expression> params = function.getParameters();
                List<Object> result = new ArrayList<Object>();
                if (!nullPlusNonNullIsNull()) {
                    result.add("CASE WHEN "); //$NON-NLS-1$
                    result.add(params.get(0));
                    result.add(" IS NULL"); //$NON-NLS-1$
                    if (params.size()>2) {
                        result.add(" OR "); //$NON-NLS-1$
                        result.add(params.get(2));
                        result.add(" IS NULL"); //$NON-NLS-1$
                    }
                    result.add(" THEN NULL ELSE "); //$NON-NLS-1$
                }

                result.addAll(Arrays.asList("LEFT(", params.get(0), " + REPLICATE(", //$NON-NLS-1$ //$NON-NLS-2$
                        params.size()>2?params.get(2):new Literal(" ", TypeFacility.RUNTIME_TYPES.STRING), ", ", //$NON-NLS-1$ //$NON-NLS-2$
                                params.get(1), "), ", params.get(1), ")")); //$NON-NLS-1$ //$NON-NLS-2$

                if (!nullPlusNonNullIsNull()) {
                    result.add(" END"); //$NON-NLS-1$
                }

                return result;
            }
        });
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("isnull")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.REPEAT, new AliasModifier("replicate")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new SubstringFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.HOUR, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.SECOND, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.WEEK, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.LENGTH, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.ATAN2, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier() {

            @Override
            public List<?> translate(Function function) {
                if (!isFracSeconds(function)) {
                    return super.translate(function);
                }
                //convert from billionths to thousandths
                return Arrays.asList("dateadd(millisecond, ", function.getParameters().get(1), "/1000000, ", function.getParameters().get(2), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier() {

            @Override
            public List<?> translate(Function function) {
                if (!isFracSeconds(function)) {
                    return super.translate(function);
                }
                //convert from billionths to thousandths
                return Arrays.asList("datediff(millisecond, ", function.getParameters().get(1), ",", function.getParameters().get(2), ")*1000000"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });

        //add in type conversion
        convertModifier.setBooleanNullable(booleanNullable());
        //boolean isn't treated as bit, since it doesn't support null
        //byte is treated as smallint, since tinyint is unsigned
        convertModifier.addTypeMapping("smallint", FunctionModifier.BYTE, FunctionModifier.SHORT); //$NON-NLS-1$
        convertModifier.addTypeMapping("int", FunctionModifier.INTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("numeric(19,0)", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("double precision", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("numeric(38, 0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("numeric(38, 19)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convertModifier.addTypeConversion(new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                if (isNonAscii(function.getParameters().get(0))) {
                    ((Literal)function.getParameters().get(1)).setValue("nchar(1)"); //$NON-NLS-1$
                } else {
                    ((Literal)function.getParameters().get(1)).setValue("char(1)"); //$NON-NLS-1$
                }
                return null;
            }
        }, FunctionModifier.CHAR);
        convertModifier.addTypeConversion(new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                if (isNonAscii(function.getParameters().get(0))) {
                    ((Literal)function.getParameters().get(1)).setValue("nvarchar(4000)"); //$NON-NLS-1$
                } else {
                    ((Literal)function.getParameters().get(1)).setValue("varchar(4000)"); //$NON-NLS-1$
                }
                return null;
            }
        }, FunctionModifier.STRING);
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                List<Object> result = new ArrayList<Object>();
                result.add("cast("); //$NON-NLS-1$
                result.addAll(convertDateToString(function));
                result.add(" AS datetime)"); //$NON-NLS-1$
                return result;
            }
        });
        convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return convertTimeToString(function);
            }
        });
        convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return convertDateToString(function);
            }
        });
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return convertTimestampToString(function);
            }
        });
        convertModifier.addNumericBooleanConversions();
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new SybaseFormatFunctionModifier("CONVERT(DATETIME, ", formatMap)); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new SybaseFormatFunctionModifier("CONVERT(VARCHAR, ", formatMap)); //$NON-NLS-1$
    }

    private void handleTimeConversions() {
        if (!hasTimeType()) {
            convertModifier.addTypeMapping("datetime", FunctionModifier.DATE, FunctionModifier.TIME, FunctionModifier.TIMESTAMP); //$NON-NLS-1$
            convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
                @Override
                public List<?> translate(Function function) {
                    List<Object> result = new ArrayList<Object>();
                    result.add("cast("); //$NON-NLS-1$
                    boolean needsEnd = false;
                    if (!nullPlusNonNullIsNull() && !ConcatFunctionModifier.isNotNull(function.getParameters().get(0))) {
                        result.add("CASE WHEN "); //$NON-NLS-1$
                        result.add(function.getParameters().get(0));
                        result.add(" IS NOT NULL THEN "); //$NON-NLS-1$
                        needsEnd = true;
                    }
                    result.add("'1970-01-01 ' + "); //$NON-NLS-1$
                    result.addAll(convertTimeToString(function));
                    if (needsEnd) {
                        result.add(" END"); //$NON-NLS-1$
                    }
                    result.add(" AS datetime)"); //$NON-NLS-1$
                    return result;
                }
            });
        } else {
            convertModifier.addTypeMapping("datetime", FunctionModifier.DATE, FunctionModifier.TIMESTAMP); //$NON-NLS-1$
            convertModifier.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
        }
    }

    private List<Object> convertTimeToString(Function function) {
        return Arrays.asList("convert(varchar, ", function.getParameters().get(0), ", 8)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    protected List<Object> convertDateToString(Function function) {
        return Arrays.asList("stuff(stuff(convert(varchar, ", function.getParameters().get(0), ", 102), 5, 1, '-'), 8, 1, '-')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    //TODO: this looses the milliseconds
    protected List<?> convertTimestampToString(Function function) {
        if (getVersion().compareTo(FIFTEEN_5) >= 0) {
            return Arrays.asList("stuff(convert(varchar, ", function.getParameters().get(0), ", 23), 11, 1, ' ')"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        LinkedList<Object> result = new LinkedList<Object>();
        result.addAll(convertDateToString(function));
        result.add("+' '+"); //$NON-NLS-1$
        result.addAll(convertTimeToString(function));
        return result;
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
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
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
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        //supportedFunctions.add("RAND"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SPACE"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
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
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        //not an iso calculation
        //supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL");      //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.FORMATTIMESTAMP);
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
    public int getMaxFromGroups() {
        return 50;
    }

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        return getVersion().compareTo(FIFTEEN_0_2) >= 0;
    }

    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    public boolean booleanNullable() {
        return false;
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "CAST('" + formatDateValue(timestampValue) +"' AS DATETIME)"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralDate(Date dateValue) {
        return "CAST('" + formatDateValue(dateValue) +"' AS DATE)"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    private boolean isFracSeconds(Function function) {
        Expression e = function.getParameters().get(0);
        return (e instanceof Literal && SQLConstants.NonReserved.SQL_TSI_FRAC_SECOND.equalsIgnoreCase((String)((Literal)e).getValue()));
    }

    @Override
    public boolean supportsRowLimit() {
        return (getVersion().getMajorVersion() == 12 && getVersion().compareTo(TWELVE_5_3) >= 0) || getVersion().compareTo(FIFTEEN_0_2) >=0; //$NON-NLS-1$
    }

    @TranslatorProperty(display="JTDS Driver", description="True if the driver is the JTDS driver",advanced=true)
    public boolean isJtdsDriver() {
        return jtdsDriver;
    }

    public void setJtdsDriver(boolean jtdsDriver) {
        this.jtdsDriver = jtdsDriver;
    }

    protected boolean setFetchSize() {
        return isJtdsDriver();
    }

    @Override
    public void setFetchSize(Command command, ExecutionContext context,
            Statement statement, int fetchSize) throws SQLException {
        if (!setFetchSize()) {
            return;
        }
        super.setFetchSize(command, context, statement, fetchSize);
    }

    @Override
    public void initCapabilities(Connection connection)
            throws TranslatorException {
        super.initCapabilities(connection);
        if (!jtdsDriver && connection != null) {
            try {
                jtdsDriver = StringUtil.indexOfIgnoreCase(connection.getMetaData().getDriverName(), "jtds") != -1; //$NON-NLS-1$
            } catch (SQLException e) {
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not automatically determine if the jtds driver is in use"); //$NON-NLS-1$
            }
        }
        handleTimeConversions();
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
    public String getHibernateDialectClassName() {
        if (getVersion().compareTo(FIFTEEN_0_2) >= 0) {
            return "org.hibernate.dialect.SybaseASE15Dialect"; //$NON-NLS-1$
        }
        return "org.hibernate.dialect.Sybase11Dialect"; //$NON-NLS-1$
    }

    @Override
    public boolean supportsGroupByRollup() {
        //TODO: there is support in SQL Anywhere/IQ, but not ASE
        return false;
    }

    @Override
    public boolean useUnicodePrefix() {
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
            return false; //TODO: add support
        }
        return formatMap.containsKey(literal);
    }

    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
        if (!supportsLiteralOnlyWithGrouping() && (command instanceof Select)) {
            Select select = (Select)command;
            if (select.getGroupBy() != null && select.getDerivedColumns().size() == 1) {
                DerivedColumn dc = select.getDerivedColumns().get(0);
                if (dc.getExpression() instanceof Literal) {
                    dc.setExpression(select.getGroupBy().getElements().get(0));
                }
            }
        }
        return super.translateCommand(command, context);
    }

    public boolean supportsLiteralOnlyWithGrouping() {
        return false;
    }

    @Override
    public String translateLiteralBinaryType(BinaryType obj) {
        return "0x" + obj; //$NON-NLS-1$
    }

    @Override
    public boolean preserveNullTyping() {
        return true;
    }

}
