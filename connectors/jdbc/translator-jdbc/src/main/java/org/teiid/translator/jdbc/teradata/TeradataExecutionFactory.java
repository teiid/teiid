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

package org.teiid.translator.jdbc.teradata;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.*;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.SQLConversionVisitor;



/**
 * Teradata database Release 12
 */
@Translator(name="teradata", description="A translator for Teradata Database")
public class TeradataExecutionFactory extends JDBCExecutionFactory {

    public static String TERADATA = "teradata"; //$NON-NLS-1$
    protected ConvertModifier convert = new ConvertModifier();

    public TeradataExecutionFactory() {
        setMaxDependentInPredicates(5);
        //teradata documentation does not make it clear that there is a hard limit. this value comes from hibernate
        setMaxInCriteriaSize(1024);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        convert.addTypeMapping("byteint", FunctionModifier.BYTE, FunctionModifier.SHORT, FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convert.addTypeMapping("double precision", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convert.addTypeMapping("numeric(18,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convert.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$

        convert.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new CastModifier("TIME")); //$NON-NLS-1$
        convert.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE,  new CastModifier("DATE")); //$NON-NLS-1$
        convert.addConvert(FunctionModifier.TIME, FunctionModifier.TIMESTAMP, new CastModifier("TIMESTAMP")); //$NON-NLS-1$
        convert.addConvert(FunctionModifier.DATE, FunctionModifier.TIMESTAMP,  new CastModifier("TIMESTAMP")); //$NON-NLS-1$

        convert.addConvert(FunctionModifier.STRING, FunctionModifier.INTEGER, new CastModifier("integer")); //$NON-NLS-1$
        convert.addConvert(FunctionModifier.STRING, FunctionModifier.BIGDECIMAL, new CastModifier("decimal(37,5)"));//$NON-NLS-1$
        convert.addConvert(FunctionModifier.STRING, FunctionModifier.BIGINTEGER, new CastModifier("numeric(18,0)"));//$NON-NLS-1$
        convert.addConvert(FunctionModifier.STRING, FunctionModifier.FLOAT, new CastModifier("float"));//$NON-NLS-1$
        convert.addConvert(FunctionModifier.STRING, FunctionModifier.BOOLEAN, new CastModifier("byteint"));//$NON-NLS-1$
        convert.addConvert(FunctionModifier.STRING, FunctionModifier.LONG, new CastModifier("numeric(18,0)"));//$NON-NLS-1$
        convert.addConvert(FunctionModifier.STRING, FunctionModifier.SHORT, new CastModifier("smallint"));//$NON-NLS-1$
        convert.addConvert(FunctionModifier.STRING, FunctionModifier.DOUBLE, new CastModifier("double precision"));//$NON-NLS-1$
        convert.addConvert(FunctionModifier.STRING, FunctionModifier.BYTE, new CastModifier("byteint")); //$NON-NLS-1$

        convert.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING,  new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(cast(", function.getParameters().get(0), " AS FORMAT 'Y4-MM-DDBHH:MI:SSDS(6)') AS VARCHAR(26))"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        convert.addConvert(FunctionModifier.TIME, FunctionModifier.STRING,   new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(cast(", function.getParameters().get(0), " AS FORMAT 'HH:MI:SS') AS VARCHAR(9))"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        convert.addConvert(FunctionModifier.DATE, FunctionModifier.STRING,  new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("cast(cast(", function.getParameters().get(0), " AS FORMAT 'YYYY-MM-DD') AS VARCHAR(11))"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });

        convert.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
        convert.addNumericBooleanConversions();

        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("LN")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("LOWER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UPPER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LENGTH, new AliasModifier("character_length")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("CURRENT_DATE")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("CURRENT_TIME")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractModifier("YEAR")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractModifier("MONTH")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ExtractModifier("DAY")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.HOUR, new ExtractModifier("HOUR")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new ExtractModifier("MINUTE")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SECOND, new ExtractModifier("SECOND")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateModifier(this.convert));
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory(), this.convert));
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new LeftOrRightFunctionModifier(getLanguageFactory(), this.convert));
        registerFunctionModifier(SourceSystemFunctions.COT, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                function.setName(SourceSystemFunctions.TAN);
                return Arrays.asList(getLanguageFactory().createFunction(SourceSystemFunctions.DIVIDE_OP, new Expression[] {new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER), function}, TypeFacility.RUNTIME_TYPES.DOUBLE));
            }
        });
        registerFunctionModifier(SourceSystemFunctions.LTRIM, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                ArrayList<Object> target = new ArrayList<Object>();
                target.add("TRIM(LEADING FROM ");//$NON-NLS-1$
                target.add(function.getParameters().get(0));
                target.add(")"); //$NON-NLS-1$
                return target;
            }
        });
        registerFunctionModifier(SourceSystemFunctions.RTRIM, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                ArrayList<Object> target = new ArrayList<Object>();
                target.add("TRIM(TRAILING FROM ");//$NON-NLS-1$
                target.add(function.getParameters().get(0));
                target.add(")"); //$NON-NLS-1$
                return target;
            }
        });
        registerFunctionModifier(SourceSystemFunctions.MOD, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList(function.getParameters().get(0), " MOD ", function.getParameters().get(1)); //$NON-NLS-1$
            }
        });

        addPushDownFunction(TERADATA, "COSH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "TANH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "ACOSH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "ASINH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "ATANH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "CHAR2HEXINT", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "INDEX", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "BYTES", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "OCTET_LENGTH", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "HASHAMP", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "HASHBAKAMP", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "HASHBUCKET", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "HASHROW", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "NULLIFZERO", BIG_DECIMAL, BIG_DECIMAL); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "ZEROIFNULL", BIG_DECIMAL, BIG_DECIMAL); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.COT, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                function.setName(SourceSystemFunctions.TAN);
                return Arrays.asList(getLanguageFactory().createFunction(SourceSystemFunctions.DIVIDE_OP, new Expression[] {new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER), function}, TypeFacility.RUNTIME_TYPES.DOUBLE));
            }
        });
        registerFunctionModifier(SourceSystemFunctions.LTRIM, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("TRIM(LEADING FROM ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        registerFunctionModifier(SourceSystemFunctions.RTRIM, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("TRIM(TRAILING FROM ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        registerFunctionModifier(SourceSystemFunctions.MOD, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return Arrays.asList(function.getParameters().get(0), " MOD ", function.getParameters().get(1)); //$NON-NLS-1$
            }
        });
    }

    @Override
    public SQLConversionVisitor getSQLConversionVisitor() {
        return new TeradataSQLConversionVisitor(this);
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
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.COT);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.TAN);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.YEAR);

        return supportedFunctions;
    }

    @Override
    public String translateLiteralDate(Date dateValue) {
        return "DATE '" + formatDateValue(dateValue, false) + '\''; //$NON-NLS-1$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME '" + formatDateValue(timeValue, false) + '\''; //$NON-NLS-1$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "TIMESTAMP '" + formatDateValue(timestampValue, false) + '\''; //$NON-NLS-1$
    }

    // Teradata also supports MINUS & ALL set operators
    // more aggregates available

    @Override
    public boolean supportsScalarSubqueries() {
        return false;
    }

    @Override
    public boolean supportsUnions() {
        return true;
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
    public boolean supportsAggregatesEnhancedNumeric() {
        return true;
    }

    @Override
    public boolean supportsCommonTableExpressions() {
        return false;
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.FIRST;
    }

    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
        if (command instanceof QueryExpression) {
            QueryExpression qe = (QueryExpression)command;
            //teradata prefers positional ordering
            if (qe.getOrderBy() != null) {
                Select select = qe.getProjectedQuery();
                List<DerivedColumn> derivedColumns = select.getDerivedColumns();
                Map<String, Integer> positions = new HashMap<String, Integer>();
                int i = 1;
                for (DerivedColumn derivedColumn : derivedColumns) {
                    String name = derivedColumn.getAlias();
                    if (name == null && derivedColumn.getExpression() instanceof ColumnReference) {
                        ColumnReference cr = (ColumnReference)derivedColumn.getExpression();
                        name = cr.toString();
                    }
                    positions.put(name, i++);
                }
                for (SortSpecification ss : qe.getOrderBy().getSortSpecifications()) {
                    Expression ex = ss.getExpression();
                    if (!(ex instanceof ColumnReference)) {
                        continue;
                    }
                    ColumnReference cr = (ColumnReference)ex;
                    Integer position = positions.get(cr.toString());
                    if (position != null) {
                        ss.setExpression(new Literal(position, TypeFacility.RUNTIME_TYPES.INTEGER));
                    }
                }
            }
        }
        return super.translateCommand(command, context);
    }

    public static class LocateModifier extends FunctionModifier {
        ConvertModifier convertModifier;

        public LocateModifier(ConvertModifier convertModifier) {
            this.convertModifier = convertModifier;
        }

        @Override
        public List<?> translate(Function function) {
            ArrayList<Object> target = new ArrayList<Object>();
            Expression expr1 =  function.getParameters().get(0);
            Expression expr2 =  function.getParameters().get(1);
            if (function.getParameters().size() > 2) {
                Expression expr3 =  function.getParameters().get(2);
                target.add("position("); //$NON-NLS-1$
                target.add(expr1);
                target.add( " in "); //$NON-NLS-1$
                target.add("substr("); //$NON-NLS-1$
                target.add(expr2);
                target.add(","); //$NON-NLS-1$
                target.add(expr3);
                target.add("))"); //$NON-NLS-1$
            }
            else {
                target.add("position("); //$NON-NLS-1$
                target.add(expr1);
                target.add( " in "); //$NON-NLS-1$
                target.add(expr2);
                target.add(")"); //$NON-NLS-1$
            }
            return target;
        }
    }

    public static class ExtractModifier extends FunctionModifier {
        private String type;
        public ExtractModifier(String type) {
            this.type = type;
        }
        @Override
        public List<?> translate(Function function) {
            return Arrays.asList("extract(",this.type," from ",function.getParameters().get(0) ,")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    public static class CastModifier extends FunctionModifier {
        private String target;
        public CastModifier(String target) {
            this.target = target;
        }
        @Override
        public List<?> translate(Function function) {
            return Arrays.asList("cast(", function.getParameters().get(0), " AS "+this.target+")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    public static class LeftOrRightFunctionModifier extends FunctionModifier {
        private LanguageFactory langFactory;
        ConvertModifier convertModifier;

        public LeftOrRightFunctionModifier(LanguageFactory langFactory, ConvertModifier converModifier) {
            this.langFactory = langFactory;
            this.convertModifier = converModifier;
        }

        @Override
        public List<?> translate(Function function) {
            List<Expression> args = function.getParameters();
            ArrayList<Object> target = new ArrayList<Object>();
            if (function.getName().equalsIgnoreCase("left")) { //$NON-NLS-1$
                //substr(string, 1, length)
                target.add("substr("); //$NON-NLS-1$
                target.add(args.get(0));
                target.add(","); //$NON-NLS-1$
                target.add(langFactory.createLiteral(Integer.valueOf(1), TypeFacility.RUNTIME_TYPES.INTEGER));
                target.add(","); //$NON-NLS-1$
                target.add(args.get(1));
                target.add(")"); //$NON-NLS-1$
            } else if (function.getName().equalsIgnoreCase("right")) { //$NON-NLS-1$
                //substr(case_size, character_length(case_size) -4)
                target.add("substr("); //$NON-NLS-1$
                target.add(args.get(0));

                target.add(",(character_length("); //$NON-NLS-1$
                target.add(args.get(0));
                target.add(")-"); //$NON-NLS-1$
                target.add(args.get(1));
                target.add("+1))"); //$NON-NLS-1$ // offset for 1 based index
            }
            return target;
        }
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public String getHibernateDialectClassName() {
        if (getVersion().getMajorVersion() >= 14) {
            return "org.hibernate.dialect.Teradata14Dialect"; //$NON-NLS-1$
        }
        return "org.hibernate.dialect.TeradataDialect"; //$NON-NLS-1$
    }

    @Override
    protected boolean supportsBooleanExpressions() {
        return false;
    }

}
