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

package org.teiid.translator.jdbc.sybase;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.OrderBy;
import org.teiid.language.SetQuery;
import org.teiid.language.With;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.db2.DB2ExecutionFactory;

public class BaseSybaseExecutionFactory extends JDBCExecutionFactory {

    @Override
    public boolean useAsInGroupAlias() {
        return false;
    }

    @Override
    public boolean hasTimeType() {
        return false;
    }

    @Override
    public int getTimestampNanoPrecision() {
        return 3;
    }

    /**
     * SetQueries don't have a concept of TOP, an inline view is needed.
     */
    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
        if (!(command instanceof SetQuery)) {
            return null;
        }
        SetQuery queryCommand = (SetQuery)command;
        if (queryCommand.getLimit() == null) {
            return null;
        }
        Limit limit = queryCommand.getLimit();
        OrderBy orderBy = queryCommand.getOrderBy();
        queryCommand.setLimit(null);
        queryCommand.setOrderBy(null);
        List<Object> parts = new ArrayList<Object>(6);
        if (queryCommand.getWith() != null) {
            With with = queryCommand.getWith();
            queryCommand.setWith(null);
            parts.add(with);
        }
        parts.add("SELECT "); //$NON-NLS-1$
        parts.addAll(translateLimit(limit, context));
        parts.add(" * FROM ("); //$NON-NLS-1$
        parts.add(queryCommand);
        parts.add(") AS X"); //$NON-NLS-1$
        if (orderBy != null) {
            parts.add(" "); //$NON-NLS-1$
            parts.add(orderBy);
        }
        return parts;
    }

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (!supportsCrossJoin()) {
            DB2ExecutionFactory.convertCrossJoinToInner(obj, getLanguageFactory());
        }
        return super.translate(obj, context);
    }

    protected boolean supportsCrossJoin() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        return Arrays.asList("TOP ", limit.getRowLimit()); //$NON-NLS-1$
    }

    @Override
    public boolean useSelectLimit() {
        return true;
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
            Class<?> expectedType) throws SQLException {
        if (expectedType == TypeFacility.RUNTIME_TYPES.BYTE) {
            expectedType = TypeFacility.RUNTIME_TYPES.SHORT;
        }
        return super.retrieveValue(results, columnIndex, expectedType);
    }

    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex,
            Class<?> expectedType) throws SQLException {
        if (expectedType == TypeFacility.RUNTIME_TYPES.BYTE) {
            expectedType = TypeFacility.RUNTIME_TYPES.SHORT;
        }
        return super.retrieveValue(results, parameterIndex, expectedType);
    }

    @Override
    public void bindValue(PreparedStatement stmt, Object param,
            Class<?> paramType, int i) throws SQLException {
        if (paramType == TypeFacility.RUNTIME_TYPES.BYTE) {
            paramType = TypeFacility.RUNTIME_TYPES.SHORT;
            param = ((Byte)param).shortValue();
        }
        super.bindValue(stmt, param, paramType, i);
    }

    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    public boolean booleanNullable() {
        return false;
    }

    @Override
    public String getTemporaryTableName(String prefix) {
        return "#" + super.getTemporaryTableName(prefix); //$NON-NLS-1$
    }

    @Override
    protected boolean supportsBooleanExpressions() {
        return false;
    }

    @Override
    public boolean supportsAggregatesCountBig() {
        return true;
    }

}
