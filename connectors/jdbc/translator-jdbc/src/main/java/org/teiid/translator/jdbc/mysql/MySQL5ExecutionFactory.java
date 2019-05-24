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

package org.teiid.translator.jdbc.mysql;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.util.Version;

@Translator(name="mysql5", description="A translator for open source MySQL5 Database")
public class MySQL5ExecutionFactory extends MySQLExecutionFactory {

    public static final Version FIVE_6 = Version.getVersion("5.6"); //$NON-NLS-1$

    @Override
    public void start() throws TranslatorException {
        super.start();
        registerFunctionModifier(SourceSystemFunctions.CHAR, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                return Arrays.asList("char(", function.getParameters().get(0), " USING ASCII)"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        });
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                Literal intervalType = (Literal)function.getParameters().get(0);
                String interval = ((String)intervalType.getValue()).toUpperCase();
                if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
                    intervalType.setValue("MICROSECOND"); //$NON-NLS-1$
                    Expression[] args = new Expression[] {function.getParameters().get(1), getLanguageFactory().createLiteral(1000, TypeFacility.RUNTIME_TYPES.INTEGER)};
                    function.getParameters().set(1, getLanguageFactory().createFunction("/", args, TypeFacility.RUNTIME_TYPES.INTEGER)); //$NON-NLS-1$
                }
                return null;
            }
        });

        addPushDownFunction("mysql", "timestampdiff", TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.TIMESTAMP, TypeFacility.RUNTIME_NAMES.TIMESTAMP); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                Literal intervalType = (Literal)function.getParameters().get(0);
                String interval = ((String)intervalType.getValue()).toUpperCase();
                if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
                    intervalType.setValue("MICROSECOND"); //$NON-NLS-1$
                    return Arrays.asList(function, " * 1000"); //$NON-NLS-1$
                }
                return null;
            }
        });

        registerFunctionModifier(SourceSystemFunctions.ST_SRID, new AliasModifier("SRID")); //$NON-NLS-1$
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPADD);
        //mysql rounds down even when crossing a date part
        //supportedFunctions.add(SourceSystemFunctions.TIMESTAMPDIFF);
        if (getVersion().compareTo(FIVE_6) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.ST_INTERSECTS);
            supportedFunctions.add(SourceSystemFunctions.ST_CONTAINS);
            supportedFunctions.add(SourceSystemFunctions.ST_CROSSES);
            supportedFunctions.add(SourceSystemFunctions.ST_DISJOINT);
            supportedFunctions.add(SourceSystemFunctions.ST_DISTANCE);
            supportedFunctions.add(SourceSystemFunctions.ST_OVERLAPS);
            supportedFunctions.add(SourceSystemFunctions.ST_TOUCHES);
            supportedFunctions.add(SourceSystemFunctions.ST_EQUALS);
        }
        supportedFunctions.add(SourceSystemFunctions.ST_SRID);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        return supportedFunctions;
    }

    @Override
    protected boolean usesDatabaseVersion() {
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
    public boolean supportsLikeRegex() {
        return true;
    }

    @Override
    public String getLikeRegexString() {
        return "REGEXP"; //$NON-NLS-1$
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
            Class<?> expectedType) throws SQLException {
        Object result = super.retrieveValue(results, columnIndex, expectedType);
        if (expectedType == TypeFacility.RUNTIME_TYPES.STRING && (result instanceof Blob || result instanceof byte[])) {
            return results.getString(columnIndex);
        }
        return result;
    }

    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex,
            Class<?> expectedType) throws SQLException {
        Object result = super.retrieveValue(results, parameterIndex, expectedType);
        if (expectedType == TypeFacility.RUNTIME_TYPES.STRING && (result instanceof Blob || result instanceof byte[])) {
            return results.getString(parameterIndex);
        }
        return result;
    }

    @Override
    public String getHibernateDialectClassName() {
        return "org.hibernate.dialect.MySQL5Dialect"; //$NON-NLS-1$
    }

    @Override
    public boolean supportsGroupByRollup() {
        return true;
    }

    @Override
    public boolean useWithRollup() {
        return true;
    }

    @Override
    public boolean supportsOrderByWithExtendedGrouping() {
        return false;
    }

    @Override
    public boolean supportsAggregatesDistinct() {
        return true;
    }

    @Override
    public int getTimestampNanoPrecision() {
        //the conversion routines won't error out even if additional
        //digits are included prior to 5.6.4.  After 5.6.4
        //we'll just let mysql do the truncating or rounding
        return 9;
    }

}
