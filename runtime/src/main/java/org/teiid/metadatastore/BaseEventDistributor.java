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
package org.teiid.metadatastore;

import java.util.List;

import org.teiid.events.EventDistributor;
import org.teiid.events.EventListener;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.Server;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.TableStats;

public class BaseEventDistributor implements EventDistributor {

    @Override
    public void updateMatViewRow(String vdbName, int vdbVersion, String schema, String viewName, List<?> tuple,
            boolean delete) {
    }

    @Override
    public void updateMatViewRow(String vdbName, String vdbVersion, String schema, String viewName, List<?> tuple,
            boolean delete) {
    }

    @Override
    public void dataModification(String vdbName, int vdbVersion, String schema, String... tableNames) {
    }

    @Override
    public void dataModification(String vdbName, String vdbVersion, String schema, String... tableNames) {
    }

    @Override
    public void setColumnStats(String vdbName, int vdbVersion, String schemaName, String tableName, String columnName,
            ColumnStats stats) {
    }

    @Override
    public void setColumnStats(String vdbName, String vdbVersion, String schemaName, String tableName,
            String columnName, ColumnStats stats) {
    }

    @Override
    public void setTableStats(String vdbName, int vdbVersion, String schemaName, String tableName, TableStats stats) {
    }

    @Override
    public void setTableStats(String vdbName, String vdbVersion, String schemaName, String tableName,
            TableStats stats) {
    }

    @Override
    public void setProperty(String vdbName, int vdbVersion, String uuid, String name, String value) {
    }

    @Override
    public void setProperty(String vdbName, String vdbVersion, String uuid, String name, String value) {
    }

    @Override
    public void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion, String schema, String viewName,
            TriggerEvent triggerEvent, String triggerDefinition, Boolean enabled) {
    }

    @Override
    public void setInsteadOfTriggerDefinition(String vdbName, String vdbVersion, String schema, String viewName,
            TriggerEvent triggerEvent, String triggerDefinition, Boolean enabled) {
    }

    @Override
    public void setProcedureDefinition(String vdbName, int vdbVersion, String schema, String procName,
            String definition) {
    }

    @Override
    public void setProcedureDefinition(String vdbName, String vdbVersion, String schema, String procName,
            String definition) {
    }

    @Override
    public void setViewDefinition(String vdbName, int vdbVersion, String schema, String viewName, String definition) {
    }

    @Override
    public void setViewDefinition(String vdbName, String vdbVersion, String schema, String viewName,
            String definition) {
    }

    @Override
    public void register(EventListener listener) {
    }

    @Override
    public void unregister(EventListener listener) {
    }

    @Override
    public void createDatabase(Database database) {
    }

    @Override
    public void dropDatabase(Database database) {
    }

    @Override
    public void createDataWrapper(String dbName, String version, DataWrapper dataWrapper) {
    }

    @Override
    public void dropDataWrapper(String dbName, String version, String dataWrapperName, boolean override) {
    }

    @Override
    public void createServer(String dbName, String version, Server server) {
    }

    @Override
    public void dropServer(String dbName, String version, Server server) {
    }

    @Override
    public void reloadDatabase(Database database) {
    }

}
