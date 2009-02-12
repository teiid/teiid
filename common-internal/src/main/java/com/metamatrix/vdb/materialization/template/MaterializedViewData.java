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

package com.metamatrix.vdb.materialization.template;

import com.metamatrix.vdb.materialization.DatabaseDialect;


/** 
 * Data holder for the arguments provided to the templates used to create materialized view population scripts.
 * 
 * @since 4.2
 */
public class MaterializedViewData implements TemplateData {

    private static final String VIEW_NAME = "viewName"; //$NON-NLS-1$
    private static final String COLUMN_NAMES = "columnNames"; //$NON-NLS-1$
    private static final String VIRTUAL_GROUP_NAME = "virtualGroupName"; //$NON-NLS-1$
    private static final String MATERIALIZATION_TABLE_NAME_IN_SRC = "materializationTableNameInSrc"; //$NON-NLS-1$
    private static final String MATERIALIZATION_TABLE_NAME = "materializationTableName"; //$NON-NLS-1$
    private static final String MATERIALIZATION_STAGE_TABLE_NAME_IN_SRC = "materializationStageTableNameInSrc"; //$NON-NLS-1$
    private static final String MATERIALIZATION_STAGE_TABLE_NAME = "materializationStageTableName"; //$NON-NLS-1$
    private static final String DATABASE_DIALECT = "databaseDialect"; //$NON-NLS-1$
    
    private String viewName;
    private String[] columnNames;
    private String virtualGroupName;
    private String materializationTableNameInScr;
    private String materializationTableName;
    private String materializationStageTableNameInSrc;
    private String materializationStageTableName;

    /**
     * Groups all of the parameters used to drive the creation of materialized view scripts.
     * @param viewName is a logical name for this materialized view.
     * @param columnNames are the names of the columns in the materialized view.
     * @param virtualGroupName is the name of the virtual group being materialized.
     * @param materializationTableNameInScr is the name of the physical table in the underlying data source 
     * that holds the cached materialized view data.
     * @param materializationTableName is the name of the physical group that corresponds to the materialized 
     * view physical table.
     * @param materializationStageTableNameInSrc The name of the staging table.
     * @param materializationStageTableName TODO
     * @param database is the type of the physical database holding the materialized view table.
     * @since 4.2
     */
    public MaterializedViewData(String viewName,
                                String[] columnNames,
                                String virtualGroupName,
                                String materializationTableNameInScr,
                                String materializationTableName, 
                                String materializationStageTableNameInSrc, 
                                String materializationStageTableName) {
        super();
        this.viewName = viewName;
        this.columnNames = columnNames;
        this.virtualGroupName = virtualGroupName;
        this.materializationTableNameInScr = materializationTableNameInScr;
        this.materializationTableName = materializationTableName;
        this.materializationStageTableNameInSrc = materializationStageTableNameInSrc;
        this.materializationStageTableName = materializationStageTableName;
    }

    /**
     * Translate all of the data into the parameter names used by the template. 
     */
    public void populateTemplate(Template template, DatabaseDialect database) {
        template.setAttribute(VIEW_NAME, viewName);
        for (int i = 0; i < columnNames.length; i++) {
            template.setAttribute(COLUMN_NAMES, columnNames[i]);
        }
        template.setAttribute(VIRTUAL_GROUP_NAME, virtualGroupName);
        template.setAttribute(MATERIALIZATION_TABLE_NAME_IN_SRC, materializationTableNameInScr);
        template.setAttribute(MATERIALIZATION_TABLE_NAME, materializationTableName);
        template.setAttribute(MATERIALIZATION_STAGE_TABLE_NAME_IN_SRC, materializationStageTableNameInSrc);
        template.setAttribute(MATERIALIZATION_STAGE_TABLE_NAME, materializationStageTableName);
        template.setAttribute(DATABASE_DIALECT, database);
    }

}
