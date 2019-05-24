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

package org.teiid.metadata;


import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

/**
 * A hook for externalizing view, procedure, and other metadata.
 *
 * One of the two load methods should be overriden.
 */
public interface MetadataRepository<F,C> {

    /**
     * Loads the schema information for the vdb for the given schemaName. Loads table, procedures, functions, indexes etc.
     * @param factory
     * @param executionFactory may be null if loading a virtual source
     * @param connectionFactory may be null if source is not available
     * @param text the text used to configure the load
     * @throws TranslatorException to indicate a recoverable error, otherwise a RuntimeException
     */
    public default void loadMetadata(MetadataFactory factory, ExecutionFactory<F, C> executionFactory, F connectionFactory, String text) throws TranslatorException {
        loadMetadata(factory, executionFactory, connectionFactory);
    }

    /**
     * Loads the schema information for the vdb for the given schemaName. Loads table, procedures, functions, indexes etc.
     * @param factory
     * @param executionFactory may be null if loading a virtual source
     * @param connectionFactory may be null if source is not available
     * @throws TranslatorException to indicate a recoverable error, otherwise a RuntimeException
     */
    public default void loadMetadata(MetadataFactory factory, ExecutionFactory<F, C> executionFactory, F connectionFactory) throws TranslatorException {

    }

    /**
     * Call back function, when "alter view" definition is called
     * @param vdbName
     * @param vdbVersion
     * @param table
     * @param viewDefinition
     */
    public default void setViewDefinition(String vdbName, String vdbVersion, Table table, String viewDefinition) {}

    /**
     * Call back function, when "alter trigger" is called
     * @param vdbName
     * @param vdbVersion
     * @param table
     * @param triggerOperation
     * @param triggerDefinition
     */
    public default void setInsteadOfTriggerDefinition(String vdbName, String vdbVersion, Table table, Table.TriggerEvent triggerOperation, String triggerDefinition) {}

    /**
     * Callback function, when "alter trigger" is called to enable or disable a trigger
     * @param vdbName
     * @param vdbVersion
     * @param table
     * @param triggerOperation
     * @param enabled
     */
    public default void setInsteadOfTriggerEnabled(String vdbName, String vdbVersion, Table table, Table.TriggerEvent triggerOperation, boolean enabled) {}


    /**
     * Call back function, when "alter procedure" is called to set the procedure definition
     * @param vdbName
     * @param vdbVersion
     * @param procedure
     * @param procedureDefinition
     */
    public default void setProcedureDefinition(String vdbName, String vdbVersion, Procedure procedure, String procedureDefinition) {}

    /**
     * Set the {@link TableStats} for the given table
     * @param vdbName
     * @param vdbVersion
     * @param table
     * @param tableStats
     */
    public default void setTableStats(String vdbName, String vdbVersion, Table table, TableStats tableStats) {}


    /**
     * Set the {@link ColumnStats} for a given column
     * @param vdbName
     * @param vdbVersion
     * @param column
     * @param columnStats
     */
    public default void setColumnStats(String vdbName, String vdbVersion, Column column, ColumnStats columnStats) {}

    /**
     * Set an extension metadata property for a given record.
     * @param vdbName
     * @param vdbVersion
     * @param record
     * @param name
     * @param value
     */
    public default void setProperty(String vdbName, String vdbVersion, AbstractMetadataRecord record, String name, String value) {}

}
