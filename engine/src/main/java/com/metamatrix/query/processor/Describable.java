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

package com.metamatrix.query.processor;

import java.util.Map;

/**
 * Interface for processor plans, nodes, etc to mark them as being
 * describable by a set of properties.
 */
public interface Describable {

    // Common 
    public static final String PROP_TYPE = "type"; //$NON-NLS-1$
    public static final String PROP_CHILDREN = "children"; //$NON-NLS-1$
    public static final String PROP_OUTPUT_COLS = "outputCols"; //$NON-NLS-1$
    
    // Relational
    public static final String PROP_CRITERIA = "criteria"; //$NON-NLS-1$
    public static final String PROP_SELECT_COLS = "selectCols"; //$NON-NLS-1$
    public static final String PROP_GROUP_COLS = "groupCols"; //$NON-NLS-1$
    public static final String PROP_SQL = "sql"; //$NON-NLS-1$
    public static final String PROP_MODEL_NAME = "modelName"; //$NON-NLS-1$
    public static final String PROP_JOIN_STRATEGY = "joinStrategy"; //$NON-NLS-1$
    public static final String PROP_JOIN_TYPE = "joinType"; //$NON-NLS-1$
    public static final String PROP_JOIN_CRITERIA = "joinCriteria"; //$NON-NLS-1$
    public static final String PROP_EXECUTION_PLAN = "execPlan"; //$NON-NLS-1$
    public static final String PROP_INTO_GROUP = "intoGrp"; //$NON-NLS-1$
    public static final String PROP_SORT_COLS = "sortCols"; //$NON-NLS-1$
    public static final String PROP_REMOVE_DUPS = "removeDups"; //$NON-NLS-1$
    public static final String PROP_NODE_STATS_LIST = "nodeStatistics"; //$NON-NLS-1$
    public static final String PROP_NODE_STATS_PROPS = "nodeStatisticsProperties"; //$NON-NLS-1$
    public static final String PROP_NODE_COST_ESTIMATES = "nodeCostEstimates";  //$NON-NLS-1$
    public static final String PROP_ROW_OFFSET = "rowOffset";  //$NON-NLS-1$
    public static final String PROP_ROW_LIMIT = "rowLimit";  //$NON-NLS-1$
    
    // XML
    public static final String PROP_MESSAGE = "message"; //$NON-NLS-1$
    public static final String PROP_TAG = "tag"; //$NON-NLS-1$
    public static final String PROP_NAMESPACE = "namespace"; //$NON-NLS-1$
    public static final String PROP_DATA_COL = "dataCol"; //$NON-NLS-1$
    public static final String PROP_NAMESPACE_DECL = "namespaceDeclarations"; //$NON-NLS-1$
    public static final String PROP_OPTIONAL = "optional"; //$NON-NLS-1$
    public static final String PROP_DEFAULT = "default"; //$NON-NLS-1$
    public static final String PROP_PROGRAM = "program";  //$NON-NLS-1$
    public static final String PROP_RECURSE_DIR = "recurseDir";  //$NON-NLS-1$
    public static final String PROP_RESULT_SET = "resultSet"; //$NON-NLS-1$
    public static final String PROP_BINDINGS = "bindings"; //$NON-NLS-1$
    public static final String PROP_IS_STAGING = "isStaging"; //$NON-NLS-1$
    public static final String PROP_IN_MEMORY = "inMemory"; //$NON-NLS-1$
    public static final String PROP_CONDITIONS = "conditions"; //$NON-NLS-1$
    public static final String PROP_PROGRAMS = "programs"; //$NON-NLS-1$
    public static final String PROP_DEFAULT_PROGRAM = "defaultProgram"; //$NON-NLS-1$
    public static final String PROP_ENCODING = "encoding"; //$NON-NLS-1$
    public static final String PROP_FORMATTED = "formatted"; //$NON-NLS-1$
    public static final String PROP_EXPRESSION = "expression"; //$NON-NLS-1$
    
    // Procedure
    public static final String PROP_VARIABLE = "variable"; //$NON-NLS-1$
    public static final String PROP_GROUP = "group"; //$NON-NLS-1$
    public static final String PROP_THEN = "then"; //$NON-NLS-1$
    public static final String PROP_ELSE = "else"; //$NON-NLS-1$
    
    /**
     * Get a description as a set of properties of primitive types such 
     * as String, Integer, etc.  
     * @return Map of properties
     */
    Map getDescriptionProperties();
    
}
