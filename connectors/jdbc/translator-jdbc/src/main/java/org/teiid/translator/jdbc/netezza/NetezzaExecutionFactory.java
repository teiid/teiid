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

package org.teiid.translator.jdbc.netezza;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Like;
import org.teiid.language.Like.MatchMode;
import org.teiid.language.Limit;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility.RUNTIME_NAMES;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.LocateFunctionModifier;
import org.teiid.util.Version;


@Translator(name = "netezza", description = "A translator for Netezza Database")
public class NetezzaExecutionFactory extends JDBCExecutionFactory {

    private static final String TIME_FORMAT = "HH24:MI:SS";  //$NON-NLS-1$
    private static final String DATE_FORMAT = "YYYY-MM-DD";  //$NON-NLS-1$
    private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;  //$NON-NLS-1$
    private static final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".MS";   //$NON-NLS-1$

    private static final Version SEVEN_0 = Version.getVersion("7.0"); //$NON-NLS-1$

    private boolean sqlExtensionsInstalled;

    public NetezzaExecutionFactory() {
        setSupportsFullOuterJoins(true);
        setSupportsOrderBy(true);
        setSupportsOuterJoins(true);
        setSupportsSelectDistinct(true);
        setSupportsInnerJoins(true);
    }

    public void start() throws TranslatorException {
        super.start();

        //STRING FUNCTION MODIFIERS
        ////////////////////////////////////
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr"));   //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE,new AliasModifier("lower"));   //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE,new AliasModifier("upper"));   //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new  LocateFunctionModifier(getLanguageFactory(), "INSTR", true));       //$NON-NLS-1$
           registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("||"));  //$NON-NLS-1$
        ///NUMERIC FUNCTION MODIFIERS
        ////////////////////////////////////
        registerFunctionModifier(SourceSystemFunctions.CEILING,    new AliasModifier("ceil"));   //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.POWER,    new AliasModifier("pow"));   //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG,    new AliasModifier("LN")); //$NON-NLS-1$
        ///BIT FUNCTION MODIFIERS
        ////////////////////////////////////
        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("int4and"));  //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("int4not"));  //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("int4or"));  //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("int4xor"));  //$NON-NLS-1$
        //DATE FUNCTION MODIFIERS
        //////////////////////////////////////////
         registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractFunctionModifier());
         registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new    ExtractModifier("DOY")); //$NON-NLS-1$
         registerFunctionModifier(SourceSystemFunctions.QUARTER, new  ExtractFunctionModifier());
         registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractFunctionModifier());
         registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new  ExtractModifier("DAY")); //$NON-NLS-1$
         registerFunctionModifier(SourceSystemFunctions.WEEK, new ExtractFunctionModifier());
         registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new  ExtractModifier("DOW")); //$NON-NLS-1$
         registerFunctionModifier(SourceSystemFunctions.HOUR, new    ExtractFunctionModifier());
         registerFunctionModifier(SourceSystemFunctions.MINUTE, new  ExtractFunctionModifier());
         registerFunctionModifier(SourceSystemFunctions.SECOND, new ExtractFunctionModifier());
         registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("CURRENT_DATE"));  //$NON-NLS-1$
         registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("CURRENT_TIME"));  //$NON-NLS-1$
       //SYSTEM FUNCTIONS
       ////////////////////////////////////
        registerFunctionModifier(SourceSystemFunctions.IFNULL,new AliasModifier("NVL"));   //$NON-NLS-1$


        // DATA TYPE CONVERSION
        ///////////////////////////////////////////
        ConvertModifier convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR);  //$NON-NLS-1$
        convertModifier.addTypeMapping("byteint", FunctionModifier.BYTE);  //$NON-NLS-1$
        convertModifier.addTypeMapping("smallint", FunctionModifier.SHORT);  //$NON-NLS-1$
        convertModifier.addTypeMapping("bigint", FunctionModifier.LONG);  //$NON-NLS-1$
        convertModifier.addTypeMapping("numeric(38)", FunctionModifier.BIGINTEGER);  //$NON-NLS-1$
        convertModifier.addTypeMapping("numeric(38,18)", FunctionModifier.BIGDECIMAL);  //$NON-NLS-1$
        convertModifier.addTypeMapping("varchar(4000)", FunctionModifier.STRING);  //$NON-NLS-1$
        //convertModifier.addTypeMapping("nvarchar(5)", FunctionModifier.BOOLEAN);

         ///NO BOOLEAN, INTEGER, FLOAT, DATE, TIME, TIMESTAMP, as they are directly available in netezza
        ///NO NULL, CLOB, BLOB, OBJECT, XML


        ///BOOLEAN--BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BIGINTEGER, BIGDECIMAL--AS IT DOESN'T WORK IMPLICITLY IN NETEZZA

        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.INTEGER, new BooleanToNumericConversionModifier());
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.BYTE, new BooleanToNumericConversionModifier());
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.SHORT, new BooleanToNumericConversionModifier());
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.LONG, new BooleanToNumericConversionModifier());
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.FLOAT, new BooleanToNumericConversionModifier());
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.DOUBLE, new BooleanToNumericConversionModifier());
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.BIGINTEGER, new BooleanToNumericConversionModifier());
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.BIGDECIMAL, new BooleanToNumericConversionModifier());
        convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.STRING, new BooleanToStringConversionModifier());
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.BOOLEAN, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                Expression stringValue = function.getParameters().get(0);
                return Arrays.asList("CASE WHEN ", stringValue, " IN ('false', '0') THEN '0' WHEN ", stringValue, " IS NOT NULL THEN '1' END");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        convertModifier.addTypeConversion(new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                Expression stringValue = function.getParameters().get(0);
                return Arrays.asList("CASE WHEN ", stringValue, " = 0 THEN '0' WHEN ", stringValue, " IS NOT NULL THEN '1' END");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }, FunctionModifier.BOOLEAN);




        ////////STRING TO DATATYPE CONVERSION OTHER THAN DATE/TIME
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.INTEGER, new CastModifier("integer"));  //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.FLOAT, new CastModifier("float")); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DOUBLE, new CastModifier("double")); //$NON-NLS-1$
        ///// STRING --> CHAR, BYTE, SHORT, LONG, BIGI, BIGD, BOOLEAN is taken care by Type Mapping
        ///// NO conversion support for NULL, CLOB, BLOB, OBJECT, XML
        ////STRING TO DATE/TIME CONVERSION////
        //////////////////////////////////////
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("to_date", DATE_FORMAT));   //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("to_timestamp", TIME_FORMAT));   //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("to_timestamp", TIMESTAMP_FORMAT));   //$NON-NLS-1$
        //////DATE/TIME INTERNAL CONVERSION/////////
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new CastModifier("TIME"));  //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE,  new CastModifier("DATE"));   //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.TIMESTAMP,  new CastModifier("TIMESTAMP"));  //$NON-NLS-1$
        //convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.TIMESTAMP, new CastModifier("TIMESTAMP")); //TIME --> TIMESTAMP --DOESN't WORK IN NETEZZA-NO FUNCTION SUPPORT

        ////DATE/TIME to STRING CONVERION////
        /////////////////////////////////////
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", TIMESTAMP_FORMAT)); //$NON-NLS-1$
        ///NO NETEZAA FUNCTION for DATE, TIME to STRING


        convertModifier.setWideningNumericImplicit(true);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);

        if (sqlExtensionsInstalled) {
            addPushDownFunction("netezza", "regexp_extract", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_extract", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_extract", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$

            addPushDownFunction("netezza", "regexp_extract_all", RUNTIME_NAMES.STRING+"[]", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            addPushDownFunction("netezza", "regexp_extract_all", RUNTIME_NAMES.STRING+"[]", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            addPushDownFunction("netezza", "regexp_extract_all", RUNTIME_NAMES.STRING+"[]", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            addPushDownFunction("netezza", "regexp_extract_all_sp", RUNTIME_NAMES.STRING+"[]", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            addPushDownFunction("netezza", "regexp_extract_all_sp", RUNTIME_NAMES.STRING+"[]", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            addPushDownFunction("netezza", "regexp_extract_all_sp", RUNTIME_NAMES.STRING+"[]", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            addPushDownFunction("netezza", "regexp_extract_sp", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_extract_sp", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$

            addPushDownFunction("netezza", "regexp_instr", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_instr", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_instr", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$

            addPushDownFunction("netezza", "regexp_like", RUNTIME_NAMES.BOOLEAN, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_like", RUNTIME_NAMES.BOOLEAN, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_like", RUNTIME_NAMES.BOOLEAN, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$

            addPushDownFunction("netezza", "regexp_match_count", RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_match_count", RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_match_count", RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$

            addPushDownFunction("netezza", "regexp_replace", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_replace", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$
            addPushDownFunction("netezza", "regexp_replace", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$

            addPushDownFunction("netezza", "regexp_replace_sp", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING+"[]"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            addPushDownFunction("netezza", "regexp_replace_sp", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING+"[]", RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            addPushDownFunction("netezza", "regexp_replace_sp", RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING, RUNTIME_NAMES.STRING+"[]", RUNTIME_NAMES.INTEGER, RUNTIME_NAMES.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        ////////////////////////////////////////////////////////////
        //STRING FUNCTIONS
        //////////////////////////////////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.ASCII);// taken care with alias function modifier
        supportedFunctions.add(SourceSystemFunctions.CHAR);//ALIAS to use 'chr'
        supportedFunctions.add(SourceSystemFunctions.CONCAT); // ALIAS ||
        supportedFunctions.add(SourceSystemFunctions.INITCAP);
        supportedFunctions.add(SourceSystemFunctions.LCASE);//ALIAS 'lower'
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE); //LOCATE FUNCTIO MODIFIER
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        //supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING); //No Need of ALIAS as both substring and substr work in netezza
        supportedFunctions.add(SourceSystemFunctions.UCASE); //ALIAS UPPER
        // FUNCTION DIFFERENCE = "difference";  ///NO FUNCTION FOUND--DIFFERENCE
        //    FUNCTION INSERT = "insert";
        // supportedFunctions.add(SourceSystemFunctions.LEFT); //is this available or is it simply for LEFT OUTER JOIN?
        // FUNCTION REPLACE = "replace"; // NO REPLACE Function
        // supportedFunctions.add(SourceSystemFunctions.RIGHT);--is this available or is it simply for RIGHT OUTER JOIN?
        // FUNCTION SOUNDEX = "soundex";
        //    FUNCTION TO_BYTES = "to_bytes";
        //    FUNCTION TO_CHARS = "to_chars";
        //////////    ////////////////////////////////////////////////////////////////////
        //NUMERIC FUNCTIONS////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////
        //supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.CEILING);  ///ALIAS-ceil
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.COT);
        supportedFunctions.add(SourceSystemFunctions.DEGREES);
        //supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.POWER);//    ALIAS-POW
        supportedFunctions.add(SourceSystemFunctions.RADIANS);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TAN);
        //        FUNCTION TRANSLATE = "translate";
        //        FUNCTION TRUNCATE = "truncate";
        //        FUNCTION FORMATINTEGER = "formatinteger";
        //        FUNCTION FORMATLONG = "formatlong";
        //        FUNCTION FORMATDOUBLE = "formatdouble";
        //        FUNCTION FORMATFLOAT = "formatfloat";
        //        FUNCTION FORMATBIGINTEGER = "formatbiginteger";
        //        FUNCTION FORMATBIGDECIMAL = "formatbigdecimal";
        //        FUNCTION LOG10 = "log10";
        //        FUNCTION PARSEINTEGER = "parseinteger";
        //        FUNCTION PARSELONG = "parselong";
        //        FUNCTION PARSEDOUBLE = "parsedouble";
        //        FUNCTION PARSEFLOAT = "parsefloat";
        //        FUNCTION PARSEBIGINTEGER = "parsebiginteger";
        //        FUNCTION PARSEBIGDECIMAL = "parsebigdecimal";
        // supportedFunctions.add(SourceSystemFunctions.RAND); --Needs Alias--But, is it required to even have an alias???
        /////////////////////////////////////////////////////////////////////
        //BIT FUNCTIONS//////////////////////////////////////////////////////
        //ALIAS FUNCTION MODIFIER IS APPLIED//////////////////////////////
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITNOT);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);
        // DATE FUNCTIONS
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.QUARTER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        //        FUNCTION DAYNAME = "dayname";
        //        FUNCTION FORMATTIMESTAMP = "formattimestamp";
        //        FUNCTION MODIFYTIMEZONE = "modifytimezone";
        //        FUNCTION MONTHNAME = "monthname";
        //        FUNCTION NOW = "now";
        //        FUNCTION PARSETIMESTAMP = "parsetimestamp";
        //        FUNCTION TIMESTAMPADD = "timestampadd";
        //        FUNCTION TIMESTAMPCREATE = "timestampcreate";
        //        FUNCTION TIMESTAMPDIFF = "timestampdiff";


        //SYSTEM FUNCTIONS
        supportedFunctions.add(SourceSystemFunctions.IFNULL); //ALIAS-NVL
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);


        //CONVERSION functions
        supportedFunctions.add(SourceSystemFunctions.CONVERT);


        return supportedFunctions;
    }

    public static class ExtractModifier extends FunctionModifier {
        private String type;
        public ExtractModifier(String type) {
            this.type = type;
        }
        @Override
        public List<?> translate(Function function) {
            return Arrays.asList("extract(",this.type," from ",function.getParameters().get(0) ,")");                     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    public static class BooleanToNumericConversionModifier extends FunctionModifier {
        @Override
        public List<?> translate(Function function) {
            Expression booleanValue = function.getParameters().get(0);
            if (booleanValue instanceof Function) {
                Function nested = (Function)booleanValue;
                if (nested.getName().equalsIgnoreCase("convert") && Number.class.isAssignableFrom(nested.getParameters().get(0).getType())) {  //$NON-NLS-1$
                    booleanValue = nested.getParameters().get(0);
                }
            }
            return Arrays.asList("(CASE WHEN ", booleanValue, " IN ( '0', 'FALSE') THEN 0 WHEN ", booleanValue, " IS NOT NULL THEN 1 END)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }

    }
    public static class BooleanToStringConversionModifier extends FunctionModifier {
        @Override
        public List<?> translate(Function function) {
            Expression booleanValue = function.getParameters().get(0);
            if (booleanValue instanceof Function) {
                Function nested = (Function)booleanValue;
                if (nested.getName().equalsIgnoreCase("convert") && Number.class.isAssignableFrom(nested.getParameters().get(0).getType())) {  //$NON-NLS-1$
                    booleanValue = nested.getParameters().get(0);
                }
            }
            return Arrays.asList("CASE WHEN ", booleanValue, " = '0' THEN 'false' WHEN ", booleanValue, " IS NOT NULL THEN 'true' END");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }

    }


    public static class CastModifier extends FunctionModifier {
        private String target;
        public CastModifier(String target) {
            this.target = target;
        }
        @Override
        public List<?> translate(Function function) {
            return Arrays.asList("cast(", function.getParameters().get(0), " AS "+this.target+")");    //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }


    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        if (limit.getRowOffset() > 0) {
            return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset());    //$NON-NLS-1$ //$NON-NLS-2$
        }
        return null;
    }

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof Like) {
            Like like = (Like)obj;
            if (like.getMode() == MatchMode.REGEX) {
                if (like.isNegated()) {
                    return Arrays.asList("NOT(REGEXP_LIKE(", like.getLeftExpression(), ", ", like.getRightExpression(), "))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                }
                return Arrays.asList("REGEXP_LIKE(", like.getLeftExpression(), ", ", like.getRightExpression(), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }
        return super.translate(obj, context);
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsIntersect() {
        return true;
    }

    @Override
    public boolean supportsExcept() {
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
    public boolean supportsRowOffset() {
        return true;
    }

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        return true;
    }

    @Override
    public boolean supportsLikeRegex() {
        return sqlExtensionsInstalled;
    }

    @TranslatorProperty(display="SQL Extensions Installed", description="True if SQL Extensions including support fo REGEXP_LIKE are installed",advanced=true)
    public boolean isSqlExtensionsInstalled() {
        return sqlExtensionsInstalled;
    }

    public void setSqlExtensionsInstalled(boolean sqlExtensionsInstalled) {
        this.sqlExtensionsInstalled = sqlExtensionsInstalled;
    }

    @Override
    public boolean supportsCommonTableExpressions() {
        return getVersion().compareTo(SEVEN_0) >= 0;
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
    public boolean supportsMultipleOpenExecutions() {
        //See TEIID-5462
        return false;
    }

}
