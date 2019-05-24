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

package org.teiid.jdbc;

import java.sql.CallableStatement;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.teiid.core.types.JDBCSQLTypeInfo;


/**
 * Note: this is currently only accurate for {@link PreparedStatement}s.
 * Only the basic type information will be accurate for {@link CallableStatement}s.
 */
public class ParameterMetaDataImpl extends WrapperImpl implements ParameterMetaData {

    private ResultSetMetaDataImpl metadata;

    public ParameterMetaDataImpl(ResultSetMetaDataImpl metadata) {
        this.metadata = metadata;
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return JDBCSQLTypeInfo.getJavaClassName(getParameterType(param));
    }

    @Override
    public int getParameterCount() throws SQLException {
        return metadata.getColumnCount();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeUnknown;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return metadata.getColumnType(param);
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return metadata.getColumnTypeName(param);
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return metadata.getPrecision(param);
    }

    @Override
    public int getScale(int param) throws SQLException {
        return metadata.getScale(param);
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return metadata.isNullable(param);
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return metadata.isSigned(param);
    }

    public String getParameterName(int param) throws SQLException {
        return metadata.getColumnName(param);
    }

}
