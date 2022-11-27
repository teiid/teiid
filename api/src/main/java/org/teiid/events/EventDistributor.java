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

package org.teiid.events;

import java.util.List;

import org.teiid.Replicated;
import org.teiid.client.util.ResultsFuture;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.Table;
import org.teiid.metadata.TableStats;

/**
 * Distributes events across the Teiid cluster
 */
public interface EventDistributor {

    /**
     * Update the given materialized view row.
     * The tuple is expected to be in table order, which has the primary key first.
     * Deletes need to only send the key, not the entire row contents.
     *
     * @param vdbName
     * @param vdbVersion
     * @param schema
     * @param viewName
     * @param tuple
     * @param delete
     */
    @Deprecated
    @Replicated(remoteOnly=true)
    void updateMatViewRow(String vdbName, int vdbVersion, String schema, String viewName, List<?> tuple, boolean delete);

    /**
     * Update the given materialized view row.
     * The tuple is expected to be in table order, which has the primary key first.
     * Deletes need to only send the key, not the entire row contents.
     *
     * @param vdbName
     * @param vdbVersion
     * @param schema
     * @param viewName
     * @param tuple
     * @param delete
     */
    @Replicated(remoteOnly=true)
    void updateMatViewRow(String vdbName, String vdbVersion, String schema, String viewName, List<?> tuple, boolean delete);

    /**
     * Notify that the table data has changed.
     * @param vdbName
     * @param vdbVersion
      * @param schema
     * @param tableNames
     */
    @Deprecated
    @Replicated(remoteOnly=true)
    void dataModification(String vdbName, int vdbVersion, String schema, String... tableNames);

    /**
     * Notify that the table data has changed.
     * @param vdbName
     * @param vdbVersion
      * @param schema
     * @param tableNames
     */
    @Replicated(remoteOnly=true)
    void dataModification(String vdbName, String vdbVersion, String schema, String... tableNames);

    /**
     * Set the column stats
     * @param vdbName
     * @param vdbVersion
     * @param schemaName
     * @param tableName
     * @param columnName
     * @param stats
     */
    @Deprecated
    @Replicated(remoteOnly=true)
    void setColumnStats(String vdbName, int vdbVersion, String schemaName,
            String tableName, String columnName, ColumnStats stats);

    /**
     * Set the column stats
     * @param vdbName
     * @param vdbVersion
     * @param schemaName
     * @param tableName
     * @param columnName
     * @param stats
     */
    @Replicated(remoteOnly=true)
    void setColumnStats(String vdbName, String vdbVersion, String schemaName,
            String tableName, String columnName, ColumnStats stats);

    /**
     * Set the table stats
     * @param vdbName
     * @param vdbVersion
     * @param schemaName
     * @param tableName
     * @param stats
     */
    @Deprecated
    @Replicated(remoteOnly=true)
    void setTableStats(String vdbName, int vdbVersion, String schemaName,
            String tableName, TableStats stats);

    /**
     * Set the table stats
     * @param vdbName
     * @param vdbVersion
     * @param schemaName
     * @param tableName
     * @param stats
     */
    @Replicated(remoteOnly=true)
    void setTableStats(String vdbName, String vdbVersion, String schemaName,
            String tableName, TableStats stats);

    /**
     * Set the given property value
     * @param vdbName
     * @param vdbVersion
     * @param uuid
     * @param name
     * @param value
     */
    @Deprecated
    @Replicated(remoteOnly=true)
    void setProperty(String vdbName, int vdbVersion, String uuid, String name, String value);

    /**
     * Set the given property value
     * @param vdbName
     * @param vdbVersion
     * @param uuid
     * @param name
     * @param value
     */
    @Replicated(remoteOnly=true)
    void setProperty(String vdbName, String vdbVersion, String uuid, String name, String value);

    /**
     * Set the instead of trigger definition.  Only one of either the triggerDefinition or enabled should be specified.
     * @param vdbName
     * @param vdbVersion
     * @param schema
     * @param viewName
     * @param triggerEvent
     * @param triggerDefinition
     * @param enabled
     */
    @Deprecated
    @Replicated(remoteOnly=true)
    void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion, String schema, String viewName, Table.TriggerEvent triggerEvent, String triggerDefinition, Boolean enabled);

    /**
     * Set the instead of trigger definition.  Only one of either the triggerDefinition or enabled should be specified.
     * @param vdbName
     * @param vdbVersion
     * @param schema
     * @param viewName
     * @param triggerEvent
     * @param triggerDefinition
     * @param enabled
     */
    @Replicated(remoteOnly=true)
    void setInsteadOfTriggerDefinition(String vdbName, String vdbVersion, String schema, String viewName, Table.TriggerEvent triggerEvent, String triggerDefinition, Boolean enabled);

    /**
     * Set the procedure definition
     * @param vdbName
     * @param vdbVersion
     * @param schema
     * @param procName
     * @param definition
     */
    @Deprecated
    @Replicated(remoteOnly=true)
    void setProcedureDefinition(String vdbName, int vdbVersion, String schema, String procName, String definition);

    /**
     * Set the procedure definition
     * @param vdbName
     * @param vdbVersion
     * @param schema
     * @param procName
     * @param definition
     */
    @Replicated(remoteOnly=true)
    void setProcedureDefinition(String vdbName, String vdbVersion, String schema, String procName, String definition);

    /**
     * Set the view definition
     * @param vdbName
     * @param vdbVersion
     * @param schema
     * @param viewName
     * @param definition
     */
    @Deprecated
    @Replicated(remoteOnly=true)
    void setViewDefinition(String vdbName, int vdbVersion, String schema, String viewName, String definition);

    /**
     * Set the view definition
     * @param vdbName
     * @param vdbVersion
     * @param schema
     * @param viewName
     * @param definition
     */
    @Replicated(remoteOnly=true)
    void setViewDefinition(String vdbName, String vdbVersion, String schema, String viewName, String definition);

    /**
     * Add EventListener for callback on events
     * @param listener
     */
    void register(EventListener listener);

    /**
     * Remove EventListener
     * @param listener
     */
    void unregister(EventListener listener);

    /**
     * Notify that the table data has changed.
     * <br>For an insert only the newValues are provided.
     * <br>For a delete only the oldValues are provided.
     * <br>For an update both are provided.
     * @return a {@link ResultsFuture} if execution has started, or null if no execution has started
     */
    ResultsFuture<?> dataModification(String vdbName, String vdbVersion, String schema, String tableName, Object[] oldValues, Object[] newValues, String[] columnNames);
}
