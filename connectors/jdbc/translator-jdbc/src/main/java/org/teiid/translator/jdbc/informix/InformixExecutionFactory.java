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
package org.teiid.translator.jdbc.informix;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.AggregateFunction;
import org.teiid.language.LanguageObject;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;


@Translator(name="informix", description="A translator for Informix Database")
public class InformixExecutionFactory extends JDBCExecutionFactory {

    private ConvertModifier convertModifier;

    @Override
    public void start() throws TranslatorException {
        super.start();

        convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convertModifier.addTypeMapping("smallint", FunctionModifier.BYTE, FunctionModifier.SHORT); //$NON-NLS-1$
        convertModifier.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("int8", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("decimal(32,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convertModifier.addTypeMapping("smallfloat", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("float", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        convertModifier.addTypeMapping("datetime year to fraction(5)", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convertModifier.addTypeMapping("datetime hour to second", FunctionModifier.TIME); //$NON-NLS-1$
        convertModifier.addTypeMapping("varchar(255)", FunctionModifier.STRING); //$NON-NLS-1$
        convertModifier.addTypeMapping("varchar(1)", FunctionModifier.CHAR); //$NON-NLS-1$
        convertModifier.addTypeMapping("byte", FunctionModifier.VARBINARY); //$NON-NLS-1$
        convertModifier.addTypeMapping("blob", FunctionModifier.BLOB); //$NON-NLS-1$
        convertModifier.addTypeMapping("clob", FunctionModifier.CLOB); //$NON-NLS-1$

        convertModifier.setWideningNumericImplicit(true);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
    }

    @Override
    public List getSupportedFunctions() {
        List supportedFunctons = new ArrayList();
        supportedFunctons.addAll(super.getSupportedFunctions());
        supportedFunctons.add(SourceSystemFunctions.CONVERT);
        return supportedFunctons;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        if (!super.supportsConvert(fromType, toType)) {
            return false;
        }
        if (convertModifier.hasTypeMapping(toType)) {
            return true;
        }
        return false;
    }

    /**
     * Informix doesn't provide min/max(boolean)
     */
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof AggregateFunction) {
            AggregateFunction agg = (AggregateFunction)obj;
            if (agg.getParameters().size() == 1
                    && (agg.getName().equalsIgnoreCase(NonReserved.MIN) || agg.getName().equalsIgnoreCase(NonReserved.MAX))
                    && TypeFacility.RUNTIME_TYPES.BOOLEAN.equals(agg.getParameters().get(0).getType())) {
                return Arrays.asList("cast(", agg.getName(), "(cast(", agg.getParameters().get(0), " as char)) as boolean)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }
        return super.translate(obj, context);
    }

    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex,
            Class<?> expectedType) throws SQLException {
        if (expectedType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
            if (isDefaultTimeZone()) {
                return results.getDate(parameterIndex);
            }
            return results.getDate(parameterIndex, getDatabaseCalendar());
        }
        if (expectedType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            Time time = results.getTime(parameterIndex);
            if (time == null || isDefaultTimeZone()) {
                return time;
            }
            return TimestampWithTimezone.createTime(time, TimeZone.getDefault(), getDatabaseCalendar());
        }
        if (expectedType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            if (isDefaultTimeZone()) {
                return results.getTimestamp(parameterIndex);
            }
            return results.getTimestamp(parameterIndex, getDatabaseCalendar());
        }
        return super.retrieveValue(results, parameterIndex, expectedType);
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
            Class<?> expectedType) throws SQLException {
        if (expectedType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
            if (isDefaultTimeZone()) {
                return results.getDate(columnIndex);
            }
            return results.getDate(columnIndex, getDatabaseCalendar());
        }
        if (expectedType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            Time time = results.getTime(columnIndex);
            if (time == null || isDefaultTimeZone()) {
                return time;
            }
            return TimestampWithTimezone.createTime(time, TimeZone.getDefault(), getDatabaseCalendar());
        }
        if (expectedType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            if (isDefaultTimeZone()) {
                return results.getTimestamp(columnIndex);
            }
            return results.getTimestamp(columnIndex, getDatabaseCalendar());
        }
        return super.retrieveValue(results, columnIndex, expectedType);
    }

    @Override
    public void bindValue(PreparedStatement stmt, Object param,
            Class<?> paramType, int i) throws SQLException {
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
            if (isDefaultTimeZone()) {
                stmt.setDate(i,(java.sql.Date)param);
            } else {
                stmt.setDate(i,(java.sql.Date)param, getDatabaseCalendar());
            }
            return;
        }
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            if (isDefaultTimeZone()) {
                stmt.setTime(i,(java.sql.Time)param);
            } else {
                stmt.setTime(i,(java.sql.Time)param, getDatabaseCalendar());
            }
            return;
        }
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            if (isDefaultTimeZone()) {
                stmt.setTimestamp(i,(java.sql.Timestamp)param);
            } else {
                stmt.setTimestamp(i,(java.sql.Timestamp)param, getDatabaseCalendar());
            }
            return;
        }
        super.bindValue(stmt, param, paramType, i);
    }

    @Override
    public Calendar getDatabaseCalendar() {
        //informix will modify the calendar
        return (Calendar) super.getDatabaseCalendar().clone();
    }

}
