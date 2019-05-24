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

package org.teiid.translator.jdbc.hana;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.SQLConstants.NonReserved;
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
import org.teiid.translator.jdbc.LocateFunctionModifier;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TemplateFunctionModifier;
import org.teiid.util.Version;

@Translator(name = "hana", description = "SAP HANA translator")
public class HanaExecutionFactory extends JDBCExecutionFactory {

    private static final String TINYINT_TYPE = "tinyint"; //$NON-NLS-1$

    public static final Version SPS8 = Version.getVersion("SPS8"); //$NON-NLS-1$

    private static final String TIME_FORMAT = "HH24:MI:SS"; //$NON-NLS-1$
    private static final String DATE_FORMAT = "YYYY-MM-DD"; //$NON-NLS-1$
    private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT; //$NON-NLS-1$
//    private static final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".FF7";  //$NON-NLS-1$

    /*
     * Date/Time Pushdown Functions
     */
    public static final String HANA = "hana"; //$NON-NLS-1$
    public static final String ADD_DAYS = "add_days"; //$NON-NLS-1$
    public static final String ADD_SECONDS = "add_seconds"; //$NON-NLS-1$
    public static final String ADD_WORKDAYS = "add_workdays"; //$NON-NLS-1$
    public static final String ADD_MONTHS = "add_months"; //$NON-NLS-1$
    public static final String ADD_YEARS = "add_years"; //$NON-NLS-1$
    public static final String CURRENT_UTCDATE = "current_utcdate"; //$NON-NLS-1$
    public static final String CURRENT_UTCTIME = "current_utctime"; //$NON-NLS-1$
    public static final String CURRENT_UTCTIMESTAMP = "current_utctimestamp"; //$NON-NLS-1$
    public static final String DAYS_BETWEEN = "days_between"; //$NON-NLS-1$
    public static final String EXTRACT = "extract"; //$NON-NLS-1$
    public static final String ISOWEEK = "isoweek"; //$NON-NLS-1$
    public static final String LAST_DAY = "last_day"; //$NON-NLS-1$
    public static final String LOCALTOUTC = "localtoutc"; //$NON-NLS-1$
    public static final String NANO100_BETWEEN = "nano100_between"; //$NON-NLS-1$
    public static final String NEXT_DAY = "next_day"; //$NON-NLS-1$
    public static final String SECONDS_BETWEEN = "seconds_between"; //$NON-NLS-1$
    public static final String WEEKDAY = "weekday"; //$NON-NLS-1$
    public static final String WORKDAYS_BETWEEN = "weekdays_between"; //$NON-NLS-1$

    /*
     * Numeric Pushdown Functions
     */
    public static final String COSH = "cosh"; //$NON-NLS-1$
    public static final String BITSET = "bitset"; //$NON-NLS-1$
    public static final String BITUNSET = "bitunset"; //$NON-NLS-1$
    public static final String HEXTOBIN = "hextobin"; //$NON-NLS-1$
    public static final String RAND = "rand"; //$NON-NLS-1$
    public static final String SINH = "sinh"; //$NON-NLS-1$
    public static final String TANH = "tanh"; //$NON-NLS-1$
    public static final String UMINUS = "uminus"; //$NON-NLS-1$

    public HanaExecutionFactory() {
    }

    @Override
    public void start() throws TranslatorException {
        super.start();

        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("to_nvarchar")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG, new Log10FunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG10, new Log10FunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory(), "locate", true) { //$NON-NLS-1$

            @Override
            public void modify(Function function) {
                super.modify(function);
                //If a start index was passed in, we convert to a substring on the search string since
                //HANA does not support the start index parameter in LOCATE().
                List<Expression> args = function.getParameters();
                if (args.size() > 2) {
                    List<Expression> substringArgs = new ArrayList<Expression>();
                    substringArgs.add(args.get(0));
                    substringArgs.add(args.get(2));
                    args.set(0, getLanguageFactory().createFunction(SourceSystemFunctions.SUBSTRING, substringArgs, null));
                    args.remove(2);
                }

            }

        });
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("current_date")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("current_time")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.WEEK, new TemplateFunctionModifier("cast(substring(isoweek(",0,"), 7, 2) as integer)")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new TemplateFunctionModifier("(MOD((WEEKDAY(",0,")+1),7)+1)")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new TemplateFunctionModifier("initcap(lower(dayname(", 0, ")))")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new TemplateFunctionModifier("((month(",0,")+2)/3)")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.NOW, new AliasModifier("current_timestamp")); //$NON-NLS-1$

        //spatial functions
        registerFunctionModifier(SourceSystemFunctions.ST_ASEWKT, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_ASBINARY, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_ASGEOJSON, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_ASTEXT, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_CONTAINS, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_CROSSES, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_DISTANCE, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_DISJOINT, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_EQUALS, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_INTERSECTS, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_SRID, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_GEOMFROMTEXT, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_OVERLAPS, new HanaSpatialFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.ST_TOUCHES, new HanaSpatialFunctionModifier());


        //////////////////////////////////////////////////////////
        //TYPE CONVERION MODIFIERS////////////////////////////////
        //////////////////////////////////////////////////////////
        ConvertModifier convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convertModifier.addTypeMapping(TINYINT_TYPE, FunctionModifier.BYTE);
        convertModifier.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
        convertModifier.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("bigint", FunctionModifier.LONG, FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        //convertModifier.addTypeMapping("smalldecimal", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convertModifier.addTypeMapping("float", FunctionModifier.FLOAT); //$NON-NLS-1$
        //convertModifier.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        convertModifier.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
        convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        //convertModifier.addTypeMapping("seconddate", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convertModifier.addTypeMapping("nvarchar", FunctionModifier.STRING); //$NON-NLS-1$
        convertModifier.addTypeMapping("nvarchar(1)", FunctionModifier.CHAR); //$NON-NLS-1$
        //convertModifier.addTypeMapping("alphanum", FunctionModifier.STRING); //$NON-NLS-1$
        convertModifier.addTypeMapping("varbinary", FunctionModifier.VARBINARY); //$NON-NLS-1$
        convertModifier.addTypeMapping("blob", FunctionModifier.BLOB); //$NON-NLS-1$
        convertModifier.addTypeMapping("nclob", FunctionModifier.CLOB); //$NON-NLS-1$
        convertModifier.addTypeMapping("text", FunctionModifier.XML); //$NON-NLS-1$
        //convertModifier.addTypeMapping("shorttext", FunctionModifier.STRING); //$NON-NLS-1$
        convertModifier.addTypeMapping("st_geometry", FunctionModifier.GEOMETRY); //$NON-NLS-1$

        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("to_date", DATE_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("to_time", TIME_FORMAT)); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("to_timestamp", DATETIME_FORMAT));  //$NON-NLS-1$

        convertModifier.setWideningNumericImplicit(true);
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                Expression trueValue = function.getParameters().get(0);
                Expression falseValue = trueValue;
                falseValue = new IsNull(falseValue, true);
                if (!(trueValue instanceof Predicate)) {
                    trueValue = new Comparison(trueValue, new Literal(Boolean.TRUE, TypeFacility.RUNTIME_TYPES.BOOLEAN), Comparison.Operator.EQ);
                }
                return Arrays.asList("CASE WHEN ", trueValue, " THEN 'true' WHEN ", falseValue, " THEN 'false' END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        convertModifier.addSourceConversion(new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                ((Literal)function.getParameters().get(1)).setValue(TINYINT_TYPE);
                return null;
            }
        }, FunctionModifier.BOOLEAN);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);

        /*
         * Date/Time Pushdown functions
         */
        addPushDownFunction(HANA, ADD_DAYS, DATE, DATE, INTEGER);
        addPushDownFunction(HANA, ADD_MONTHS, DATE, DATE, INTEGER);
        addPushDownFunction(HANA, ADD_SECONDS, TIMESTAMP, TIMESTAMP, INTEGER);
        addPushDownFunction(HANA, ADD_WORKDAYS, DATE, DATE, INTEGER);
        addPushDownFunction(HANA, ADD_YEARS, DATE, DATE, INTEGER);
        addPushDownFunction(HANA, CURRENT_UTCDATE, DATE);
        addPushDownFunction(HANA, CURRENT_UTCTIME, TIME);
        addPushDownFunction(HANA, CURRENT_UTCTIMESTAMP, TIMESTAMP);
        addPushDownFunction(HANA, DAYS_BETWEEN, INTEGER, DATE, DATE);
        addPushDownFunction(HANA, ISOWEEK, STRING, DATE);
        addPushDownFunction(HANA, LAST_DAY, DATE, DATE);
        addPushDownFunction(HANA, LOCALTOUTC, TIMESTAMP, TIMESTAMP, STRING);
        addPushDownFunction(HANA, NANO100_BETWEEN, INTEGER, DATE, DATE);
        addPushDownFunction(HANA, NEXT_DAY, DATE, DATE);
        addPushDownFunction(HANA, SECONDS_BETWEEN, INTEGER, DATE, DATE);
        addPushDownFunction(HANA, WEEKDAY, INTEGER, DATE);
        addPushDownFunction(HANA, WORKDAYS_BETWEEN, DATE, INTEGER);

        /*
         * Numeric Pushdown functions
         */
        addPushDownFunction(HANA, COSH, INTEGER, FLOAT);
        addPushDownFunction(HANA, BITSET, STRING, VARBINARY, INTEGER, INTEGER);
        addPushDownFunction(HANA, BITUNSET, STRING, VARBINARY, INTEGER, INTEGER);
        addPushDownFunction(HANA, COSH, FLOAT, FLOAT);
        addPushDownFunction(HANA, HEXTOBIN, BLOB, STRING);
        addPushDownFunction(HANA, RAND, BLOB);
        addPushDownFunction(HANA, SINH, FLOAT, FLOAT);
        addPushDownFunction(HANA, TANH, FLOAT, FLOAT);
        addPushDownFunction(HANA, UMINUS, INTEGER, INTEGER);

    }

    @Override
    public String getHibernateDialectClassName() {
        return "org.hibernate.dialect.HANARowStoreDialect"; //$NON-NLS-1$
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        //////////////////////////////////////////////////////////
        //STRING FUNCTIONS////////////////////////////////////////
        //////////////////////////////////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.ASCII);// taken care with alias function modifier
        supportedFunctions.add(SourceSystemFunctions.CHAR);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.LCASE);//ALIAS 'lower'
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.UCASE); //No Need of ALIAS as both ucase and upper work in HANA
        supportedFunctions.add(SourceSystemFunctions.RTRIM);

        ///////////////////////////////////////////////////////////
        //NUMERIC FUNCTIONS////////////////////////////////////////
        ///////////////////////////////////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.CEILING);  ///ALIAS-ceil
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
        supportedFunctions.add(SourceSystemFunctions.RAND);

        /////////////////////////////////////////////////////////////////////
        //BIT FUNCTIONS//////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITNOT);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);

        /////////////////////////////////////////////////////////////////////
        //DATE FUNCTIONS/////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYNAME);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.MONTHNAME);
        supportedFunctions.add(SourceSystemFunctions.QUARTER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.YEAR);

        /////////////////////////////////////////////////////////////////////
        //SYSTEM FUNCTIONS///////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);

        /////////////////////////////////////////////////////////////////////
        //CONVERSION functions///////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.CONVERT);

        /////////////////////////////////////////////////////////////////////
        //GEO Spatial functions//////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.ST_SRID);
        supportedFunctions.add(SourceSystemFunctions.ST_ASBINARY);
        supportedFunctions.add(SourceSystemFunctions.ST_ASEWKT);
        supportedFunctions.add(SourceSystemFunctions.ST_ASTEXT);
        supportedFunctions.add(SourceSystemFunctions.ST_ASGEOJSON);
        supportedFunctions.add(SourceSystemFunctions.ST_CONTAINS);
        supportedFunctions.add(SourceSystemFunctions.ST_CROSSES);
        supportedFunctions.add(SourceSystemFunctions.ST_DISJOINT);
        supportedFunctions.add(SourceSystemFunctions.ST_DISTANCE);
        supportedFunctions.add(SourceSystemFunctions.ST_EQUALS);
        supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMTEXT);
        supportedFunctions.add(SourceSystemFunctions.ST_INTERSECTS);
        supportedFunctions.add(SourceSystemFunctions.ST_OVERLAPS);
        supportedFunctions.add(SourceSystemFunctions.ST_SRID);
        supportedFunctions.add(SourceSystemFunctions.ST_TOUCHES);


        return supportedFunctions;
    }

    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    public boolean supportsInCriteria() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        if (limit.getRowOffset() > 0) {
            return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return null;
    }

    public void getMetadata(MetadataFactory metadataFactory,
            Connection connection) throws TranslatorException {
        super.getMetadata(metadataFactory, connection);
    }

    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new HanaMetadataProcessor();
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }

    @Override
    public SQLConversionVisitor getSQLConversionVisitor() {
        return new SQLConversionVisitor(this) {

            @Override
            protected void translateSQLType(Class<?> type, Object obj,
                    StringBuilder valuesbuffer) {
                if (type == TypeFacility.RUNTIME_TYPES.VARBINARY) {
                    valuesbuffer.append("to_binary("); //$NON-NLS-1$
                    super.translateSQLType(TypeFacility.RUNTIME_TYPES.STRING, obj, valuesbuffer);
                    valuesbuffer.append(")"); //$NON-NLS-1$
                } else {
                    super.translateSQLType(type, obj, valuesbuffer);
                }
            }

        };
    }

    /**
     * Hana doesn't provide min/max(boolean)
     */
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof AggregateFunction) {
            AggregateFunction agg = (AggregateFunction)obj;
            if (agg.getParameters().size() == 1
                    && (agg.getName().equalsIgnoreCase(NonReserved.MIN) || agg.getName().equalsIgnoreCase(NonReserved.MAX))
                    && TypeFacility.RUNTIME_TYPES.BOOLEAN.equals(agg.getParameters().get(0).getType())) {
                return Arrays.asList("cast(", agg.getName(), "(to_tinyint(", agg.getParameters().get(0), ")) as boolean)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }
        return super.translate(obj, context);
    }

    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if (booleanValue) {
            return "true"; //$NON-NLS-1$
        }
        return "false"; //$NON-NLS-1$
    }

}
