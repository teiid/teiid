/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.common.extensionmodule.spi.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.util.ErrorMessageKeys;

public class JDBCExtensionDescriptor extends ExtensionModuleDescriptor {

    public JDBCExtensionDescriptor(){}

    public JDBCExtensionDescriptor(ResultSet resultSet) throws MetaMatrixComponentException {
        try{
            this.name = resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME);
            this.type = resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE);
            this.position = resultSet.getInt(JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION);
            this.enabled = resultSet.getBoolean(JDBCNames.ExtensionFilesTable.ColumnName.IS_ENABLED);
            this.desc = resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.FILE_DESCRIPTION);
            this.createdBy = resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.CREATED_BY);
            this.creationDate = resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.CREATION_DATE);
            this.lastUpdatedBy = resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY);
            this.lastUpdatedDate = resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.UPDATED);
            this.checksum = resultSet.getLong(JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM);
        } catch (SQLException e){
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0044, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0044));
        }
    }
}

