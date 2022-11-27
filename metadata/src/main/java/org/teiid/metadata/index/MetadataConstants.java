/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.teiid.metadata.index;

import org.teiid.core.util.StringUtil;

/**
 * MetadataConstants are all the constant values used to identify all the valid values for a multi-value attribute.
 * All assigned short values start with 1.  Therefore, when the get...TypeName(type) method is called, the
 * method needs to subtract 1 from the argument.
 */
public final class MetadataConstants {

    /** Definition of not defined long type. */
    public static final long NOT_DEFINED_LONG = Long.MIN_VALUE;
    /**  Definition of not defined int type. */
    public static final int NOT_DEFINED_INT = Integer.MIN_VALUE;
    /**  Definition of not defined short type. */
    public static final short NOT_DEFINED_SHORT = Short.MIN_VALUE;

    public final static String BLANK = StringUtil.Constants.EMPTY_STRING;

    //properties
    public static final String VERSION_DATE = "versionDate"; //$NON-NLS-1$

    /**
     * These types are associated with a KEY, indicating the type of matching that can be performed on it.
     */
    public final static class MATCH_TYPES {
        public final static short FULL_MATCH    = 0;
        public final static short PARTIAL_MATCH = 1;
        public final static short NEITHER_MATCH = 2;
        public final static short NA            = 3;
        public final static String[] TYPE_NAMES = { "Full",  //$NON-NLS-1$
                                                    "Partial",   //$NON-NLS-1$
                                                    "Neither",  //$NON-NLS-1$
                                                    "N/A" }; //$NON-NLS-1$
    }

    public final static String getMatchTypeName(short type) {
        return  MATCH_TYPES.TYPE_NAMES[type];
    }


    /**
     * These types indicate the type of KEY it is.
     * The values must be kept consistent with the values referenced in
     * KeyTypeEnumeration.properties in connector.metadata
     */
    public final static class KEY_TYPES {
        public final static short PRIMARY_KEY    = 0;
        public final static short FOREIGN_KEY    = 1;
        public final static short UNIQUE_KEY     = 2;
        public final static short NON_UNIQUE_KEY = 3;
        public final static short ACCESS_PATTERN = 4;
        public final static short INDEX          = 5;
        public final static String[] TYPE_NAMES = { "Primary",  //$NON-NLS-1$
                                                    "Foreign",   //$NON-NLS-1$
                                                    "Unique",  //$NON-NLS-1$
                                                    "NonUnique",  //$NON-NLS-1$
                                                    "AccessPattern",  //$NON-NLS-1$
                                                    "Index" }; //$NON-NLS-1$
    }
    public final static String getKeyTypeName(short type) {
        return  KEY_TYPES.TYPE_NAMES[type];
    }

    /**
     * These types indicate the type of COLUMN_SET it is.
     * The values must be kept consistent with the values referenced in
     * KeyTypeEnumeration.properties in connector.metadata
     */
    public final static class COLUMN_SET_TYPES {
        public final static short FOREIGN_KEY      = 0;
        public final static short UNIQUE_KEY       = 1;
        public final static short ACCESS_PATTERN   = 2;
        public final static short INDEX            = 3;
        public final static short PROCEDURE_RESULT = 4;
        public final static short TABLE            = 5;
        public final static String[] TYPE_NAMES = { "Foreign",   //$NON-NLS-1$
                                                    "Unique",  //$NON-NLS-1$
                                                    "AccessPattern",  //$NON-NLS-1$
                                                    "Index",  //$NON-NLS-1$
                                                    "Procedure_Result", //$NON-NLS-1$
                                                    "Table"}; //$NON-NLS-1$
    }
    public final static String getColumnSetTypeName(short type) {
        return  KEY_TYPES.TYPE_NAMES[type];
    }

    /**
     * These types indicate the type of PROCEDURE it is.
     * The values must be kept consistent with the values referenced in
     * ProcTypeEnumeration.properties in connector.metadata
     */
    public final static class PROCEDURE_TYPES {
        public final static short FUNCTION         = 0;
        public final static short STORED_PROCEDURE = 1;
        public final static short STORED_QUERY     = 2;
        public final static String[] TYPE_NAMES = { "Function",  //$NON-NLS-1$
                                                    "StoredProc", //$NON-NLS-1$
                                                    "StoredQuery" }; //$NON-NLS-1$
    }
    public final static String getProcedureTypeName(short type) {
        return PROCEDURE_TYPES.TYPE_NAMES[type];
    }

    /**
     * These types indicate the type of TRANSFORMATION it it.
     */
    public final static class SQL_TRANSFORMATION_TYPES {
        public final static short MAPPING_DEFN            = 0;
        public final static short QUERY_PLAN_SELECT_QUERY = 1;
        public final static short QUERY_PLAN_INSERT_QUERY = 2;
        public final static short QUERY_PLAN_UPDATE_QUERY = 3;
        public final static short QUERY_PLAN_DELETE_QUERY = 4;
        public final static short QUERY_PLAN_STORED_QUERY = 5;
        public final static String[] TYPE_NAMES = { "MappingDefn",  //$NON-NLS-1$
                                                    "QueryPlanSelectQuery", //$NON-NLS-1$
                                                    "QueryPlanInsertQuery", //$NON-NLS-1$
                                                    "QueryPlanUpdateQuery", //$NON-NLS-1$
                                                    "QueryPlanDeleteQuery", //$NON-NLS-1$
                                                    "QueryPlanStoredQuery"}; //$NON-NLS-1$
    }
    public final static String getSqlTransformationTypeName(short type) {
        return SQL_TRANSFORMATION_TYPES.TYPE_NAMES[type];
    }

    /**
     * These types indicate the type of  PROCEDURE_PARAMETER it is.
     * The values must be kept consistent with the DirectionKind enumeration in the relational
     * metamodel and the values referenced in ProcParamDirectionEnumeration.properties in connector.metadata
     */
    public final static class PARAMETER_TYPES {
        public final static short IN_PARM      = 0;
        public final static short OUT_PARM     = 1;
        public final static short INOUT_PARM   = 2;
        public final static short RETURN_VALUE = 3;
        public final static short RESULT_SET   = 4;
        public final static String[] TYPE_NAMES = { "In",  //$NON-NLS-1$
                                                    "Out",   //$NON-NLS-1$
                                                    "InOut",  //$NON-NLS-1$
                                                    "ReturnValue",  //$NON-NLS-1$
                                                    "ResultSet" }; //$NON-NLS-1$
    }
    public final static String getParameterTypeName(short type) {
        return PARAMETER_TYPES.TYPE_NAMES[type];
    }

    /**
     * These types are associated with the Element having valid search types.
     * The values must be kept consistent with the SearchabilityType enumeration in the relational
     * metamodel and the values referenced in SearchTypeEnumeration.properties in connector.metadata
     */
    public final static class SEARCH_TYPES {
        public final static short SEARCHABLE    = 0;
        public final static short ALLEXCEPTLIKE = 1;
        public final static short LIKE_ONLY     = 2;
        public final static short UNSEARCHABLE  = 3;
        public final static String[] TYPE_NAMES = { "Searchable",  //$NON-NLS-1$
                                                    "All Except Like",   //$NON-NLS-1$
                                                    "Like Only",  //$NON-NLS-1$
                                                    "Unsearchable" }; //$NON-NLS-1$
    }
    public final static String getSearchTypeName(short type) {
        return SEARCH_TYPES.TYPE_NAMES[type];
    }

    /**
     * A DataType object will be identified as being of one of these types.
     * The values must be kept consistent with the values referenced in
     * DatatypeTypeEnumeration.properties in connector.metadata
     */
    public final static class DATATYPE_TYPES {
        public final static short BASIC        = 0;
        public final static short USER_DEFINED = 1;
        public final static short RESULT_SET   = 2;
        public final static String[] TYPE_NAMES = { "Basic",  //$NON-NLS-1$
                                                    "UserDefined", //$NON-NLS-1$
                                                    "ResultSet" }; //$NON-NLS-1$
    }
    public final static String getDataTypeTypeName(short type) {
        return DATATYPE_TYPES.TYPE_NAMES[type];
    }

    /**
     * User defined DataType objects will be categorized by a variety
     * The values must be kept consistent with the XSDVariety enumeration in the xsd
     * metamodel and the values referenced in DatatypeVarietyEnumeration.properties in
     * connector.metadata
     */
    public final static class DATATYPE_VARIETIES {
        public final static short ATOMIC  = 0;
        public final static short LIST    = 1;
        public final static short UNION   = 2;
        public final static short COMPLEX = 3;
        public final static String[] TYPE_NAMES = { "Atomic",  //$NON-NLS-1$
                                                    "List", //$NON-NLS-1$
                                                    "Union", //$NON-NLS-1$
                                                    "Complex" }; //$NON-NLS-1$
    }
    public final static String getDataTypeVarietyName(short type) {
        return DATATYPE_VARIETIES.TYPE_NAMES[type];
    }

    /**
     * These types represent the type of table a Group is.
     */
    public final static class TABLE_TYPES {
        public static final short TABLE_TYPE             = 0;
        public static final short VIEW_TYPE              = 1;
        public static final short DOCUMENT_TYPE          = 2;
        public static final short XML_MAPPING_CLASS_TYPE = 3;
        public static final short XML_STAGING_TABLE_TYPE = 4;
        public static final short MATERIALIZED_TYPE      = 5;
        public final static String[] TYPE_NAMES = { "Table", //$NON-NLS-1$
                                                    "View",  //$NON-NLS-1$
                                                    "Document",  //$NON-NLS-1$
                                                    "XmlMappingClass",  //$NON-NLS-1$
                                                    "XmlStagingTable", //$NON-NLS-1$
                                                    "MaterializedTable" }; //$NON-NLS-1$
    }
    public final static String getTableTypeName(short type) {
        return TABLE_TYPES.TYPE_NAMES[type];
    }

    /**
     * These types are associated with a DataType or an Element needing the indication of null types.
     * The values must be kept consistent with the NullableType enumeration in the relational
     * metamodel and the values referenced in NullTypeEnumeration.properties in connector.metadata
     */
    public final static class NULL_TYPES {
        public static final short NOT_NULL = 0;
        public static final short NULLABLE = 1;
        public static final short UNKNOWN  = 2;
        public final static String[] TYPE_NAMES = { "No Nulls",  //$NON-NLS-1$
                                                    "Nullable",   //$NON-NLS-1$
                                                    "Unknown" }; //$NON-NLS-1$
    }
    //Record type Constants
    public static class RECORD_TYPE {
        public final static char MODEL               = 'A';
        public final static char TABLE               = 'B';
        public final static char RESULT_SET          = 'C';
        public final static char JOIN_DESCRIPTOR     = 'D';
        public final static char CALLABLE            = 'E';
        public final static char CALLABLE_PARAMETER  = 'F';
        public final static char COLUMN              = 'G';
        public final static char ACCESS_PATTERN      = 'H';
        public final static char UNIQUE_KEY          = 'I';
        public final static char FOREIGN_KEY         = 'J';
        public final static char PRIMARY_KEY         = 'K';
        public final static char INDEX               = 'L';
        public final static char DATATYPE            = 'M';
        //public final static char DATATYPE_ELEMENT    = 'N';
        //public final static char DATATYPE_FACET      = 'O';
        public final static char SELECT_TRANSFORM    = 'P';
        public final static char INSERT_TRANSFORM    = 'Q';
        public final static char UPDATE_TRANSFORM    = 'R';
        public final static char DELETE_TRANSFORM    = 'S';
        public final static char PROC_TRANSFORM      = 'T';
        public final static char MAPPING_TRANSFORM   = 'U';
        public final static char VDB_ARCHIVE         = 'V';
        public final static char ANNOTATION          = 'W';
        public final static char PROPERTY            = 'X';
        public final static char FILE                 = 'Z';
        public final static char RECORD_CONTINUATION = '&';
    }

    public final static String getNullTypeName(short type) {
        return NULL_TYPES.TYPE_NAMES[type];
    }
}