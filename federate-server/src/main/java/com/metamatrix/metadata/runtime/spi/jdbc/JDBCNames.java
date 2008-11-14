/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.metadata.runtime.spi.jdbc;

public class JDBCNames {
    //table definitions
//    public static class DataTypes{
//        public static final String TABLE_NAME               = "RT_DT";
//        public static class ColumnName {
//            public static final String DT_UID               = "DT_UID";
//            public static final String DT_NM                = "DT_NM";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//            public static final String MDL_UID              = "MDL_UID";
//            public static final String IS_STANDARD          = "IS_STANDARD";
//            public static final String IS_PHYSICAL          = "IS_PHYSICAL";
//            public static final String TYPE                 = "TYP";
//            public static final String SCALE                = "SCALE";
//            public static final String LENGTH               = "LNGTH";
//            public static final String PRCSN_LNGTH          = "PRCSN_LNGTH";
//            public static final String RADIX                = "RADIX";
//            public static final String FXD_PRCSN_LNGTH      = "FXD_PRCSN_LNGTH";
//            public static final String SRCH_TYPE            = "SRCH_TYPE";
//            public static final String NULL_TYPE              = "NULL_TYPE";
//            public static final String IS_SIGNED            = "IS_SIGNED";
//            public static final String IS_AUTOINCREMENT     = "IS_AUTOINCREMENT";
//            public static final String IS_CASE_SENSITIVE    = "IS_CASE_SENSITIVE";
//            public static final String SUP_SELECT           = "SUP_SELECT";
//            public static final String MIN_RANGE            = "MIN_RANGE";
//            public static final String MAX_RANGE            = "MAX_RANGE";
//            public static final String NATIVE_TYPE          = "NATIVE_TYPE";
//            public static final String PATH                 = "PATH_1";
//            public static final String JAVA_CLASS_1         = "JAVA_CLASS_1";
//            public static final String RUNTIME_TYPE         = "RUNTIME_TYPE";
//            public static final String BASE_TYPE            = "BASE_TYPE";
//            public static final String DT_UUID              = "DT_UUID";
//       }
//    }

//    public static class DataTypeElementPropNames{
//        public static final String TABLE_NAME               = "RT_DT_ELMNT_PRP_NMS";
//        public static class ColumnName {
//            public static final String PRP_UID              ="PRP_UID";
//            public static final String DT_ELMNT_UID         ="DT_ELMNT_UID";
//            public static final String PRP_NM               ="PRP_NM";
//        }
//    }
//
//
//    public static class DataTypeElementPropValues{
//        public static final String TABLE_NAME               = "RT_DT_ELMNT_PRP_VLS";
//        public static class ColumnName {
//            public static final String PRP_UID              ="PRP_UID";
//            public static final String PART_ID              ="PART_ID";
//            public static final String PRP_VL               ="PRP_VL";
//        }
//    }
//
//
//    public static class DataTypeElements{
//        public static final String TABLE_NAME               = "RT_DT_ELMNTS";
//        public static class ColumnName {
//            public static final String DT_ELMNT_UID         = "DT_ELMNT_UID";
//            public static final String PARENT_UID           = "PRNT_DT_UID";
//            public static final String DT_UID               = "DT_UID";
//            public static final String POSITION             = "POSITION";
//            public static final String NM                   = "NM";
//            public static final String EXCLUDE_DATA         = "EXCLUDE_DATA";
//            public static final String SCALE                = "SCALE";
//            public static final String LENGTH               = "LNGTH";
//            public static final String IS_NULLABLE          = "IS_NULLABLE";
//        }
//    }
//
//    public static class DataTypePropNames{
//        public static final String TABLE_NAME               = "RT_DT_PRP_NMS";
//        public static class ColumnName{
//            public static final String PRP_UID              ="PRP_UID";
//            public static final String DT_UID               ="DT_UID";
//            public static final String PRP_NM               ="PRP_NM";
//        }
//    }
//
//    public static class DataTypePropValues{
//        public static final String TABLE_NAME               = "RT_DT_PRP_VLS";
//        public static class ColumnName{
//            public static final String PRP_UID              ="PRP_UID";
//            public static final String PART_ID              ="PART_ID";
//            public static final String PRP_VL               ="PRP_VL";
//        }
//    }
//
//    public static class DataTypeTypes{
//        public static final String TABLE_NAME               = "RT_DT_TYPES";
//        public static class ColumnName{
//            public static final String TYPE_CODE            ="TYPE_CODE";
//            public static final String TYPE_NM              ="TYPE_NM";
//            public static final String DESCRIPTION          ="DESCRIPTION";
//        }
//    }

//    public static class ElementPropNames{
//        public static final String TABLE_NAME               = "RT_ELMNT_PRP_NMS";
//        public static class ColumnName{
//            public static final String PRP_UID              = "PRP_UID";
//            public static final String ELMNT_UID            = "ELMNT_UID";
//            public static final String PRP_NM               = "PRP_NM";
//        }
//    }
//
//    public static class ElementPropValues{
//        public static final String TABLE_NAME               = "RT_ELMNT_PRP_VLS";
//        public static class ColumnName{
//            public static final String PRP_UID              = "PRP_UID";
//            public static final String PART_ID              = "PART_ID";
//            public static final String PRP_VL               = "PRP_VL";
//        }
//    }

//    public static class Elements{
//        public static final String TABLE_NAME               = "RT_ELMNTS";
//        public static class ColumnName{
//            public static final String ELMNT_UID            = "ELMNT_UID";
//            public static final String ELMNT_NM             = "ELMNT_NM";
//            public static final String ELMNT_UPPR_NM        = "ELMNT_UPPR_NM";
//            public static final String POSITION             = "POSITION";
//            public static final String GRP_UID              = "GRP_UID";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//            public static final String ALIAS                = "ALIAS";
//            public static final String LABEL                = "LABEL";
//            public static final String DT_UID               = "DT_UID";
//            public static final String SCALE                = "SCALE";
//            public static final String LENGTH               = "LNGTH";
//            public static final String PRCSN_LNGTH          = "PRCSN_LNGTH";
//            public static final String RADIX                = "RADIX";
//            public static final String CHR_OCTCT_LNGTH      = "CHR_OCTCT_LNGTH";
//            public static final String IS_PHYSICAL          = "IS_PHYSICAL";
//            public static final String IS_LENGTH_FIXED      = "IS_LENGTH_FIXED";
//            public static final String NULL_TYPE              = "NULL_TYPE";
//            public static final String SUP_SELECT           = "SUP_SELECT";
//            public static final String SUP_SET              = "SUP_SET";
//            public static final String SUP_SUBSCRIPTION     = "SUP_SUBSCRIPTION";
//            public static final String SUP_UPDATE           = "SUP_UPDATE";
//            public static final String IS_CASE_SENSITIVE    = "IS_CASE_SENSITIVE";
//            public static final String IS_SIGNED            = "IS_SIGNED";
//            public static final String IS_CURRENCY          = "IS_CURRENCY";
//            public static final String IS_AUTOINCREMENT     = "IS_AUTOINCREMENT";
//            public static final String MIN_RANGE            = "MIN_RANGE";
//            public static final String MAX_RANGE            = "MAX_RANGE";
//            public static final String SEARCH_TYPE          = "SEARCH_TYPE";
//            public static final String FORMAT               = "FORMAT";
//            public static final String MULTIPLICITY         = "MULTIPLICITY";
//            public static final String DEFAULT_VL           = "DEFAULT_VL";
//        }
//    }

//    public static class GroupPropNames{
//        public static final String TABLE_NAME               = "RT_GRP_PRP_NMS";
//        public static class ColumnName{
//            public static final String PRP_UID              = "PRP_UID";
//            public static final String GRP_UID              = "GRP_UID";
//            public static final String PRP_NM               = "PRP_NM";
//        }
//    }
//
//    public static class GroupPropValues{
//        public static final String TABLE_NAME               = "RT_GRP_PRP_VLS";
//        public static class ColumnName{
//            public static final String PRP_UID              = "PRP_UID";
//            public static final String PART_ID              = "PART_ID";
//            public static final String PRP_VL               = "PRP_VL";
//        }
//    }
//
//    public static class Groups{
//        public static final String TABLE_NAME               = "RT_GRPS";
//        public static class ColumnName{
//            public static final String GRP_UID              = "GRP_UID";
//            public static final String GRP_NM               = "GRP_NM";
//            public static final String GRP_UPPR_NM          = "GRP_UPPR_NM";
//            public static final String MDL_UID              = "MDL_UID";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//            public static final String ALIAS                = "ALIAS";
//            public static final String PATH                 = "PATH_1";
//            public static final String TABLE_TYPE           = "TABLE_TYPE";
//            public static final String IS_PHYSICAL          = "IS_PHYSICAL";
//            public static final String HAS_QUERY_PLAN       = "HAS_QUERY_PLAN";
//            public static final String SUP_UPDATE           = "SUP_UPDATE";
//        }
//    }
//
//    public static class KeyTypes{
//        public static final String TABLE_NAME               = "RT_KEY_TYPES";
//        public static class ColumnName{
//            public static final String KEY_TYPE_CODE        = "KEY_TYPE_CODE";
//            public static final String KEY_TYPE_NM          = "KEY_TYPE_NM";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//        }
//    }
//
//    public static class KeyIndexElements{
//        public static final String TABLE_NAME               = "RT_KY_IDX_ELMNTS";
//        public static class ColumnName{
//            public static final String ELMNT_UID            = "ELMNT_UID";
//            public static final String KEY_UID              = "KEY_UID";
//            public static final String POSITION             = "POSITION";
//        }
//    }
//
//    public static class KeyIndexPropNames{
//        public static final String TABLE_NAME               = "RT_KY_IDX_PRP_NMS";
//        public static class ColumnName{
//            public static final String PRP_UID              = "PRP_UID";
//            public static final String KEY_UID              = "KEY_UID";
//            public static final String PRP_NM               = "PRP_NM";
//        }
//    }
//
//    public static class KeyIndexPropValues{
//        public static final String TABLE_NAME               = "RT_KY_IDX_PRP_VLS";
//        public static class ColumnName{
//            public static final String PRP_UID              = "PRP_UID";
//            public static final String PART_ID              = "PART_ID";
//            public static final String PRP_VL               = "PRP_VL";
//        }
//    }
//
//    public static class KeyIndex{
//        public static final String TABLE_NAME               = "RT_KY_IDXES";
//        public static class ColumnName{
//            public static final String KEY_UID              = "KEY_UID";
//            public static final String GRP_UID              = "GRP_UID";
//            public static final String KEY_NM               = "KEY_NM";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//            public static final String ALIAS                = "ALIAS";
//            public static final String KEY_TYPE             = "KEY_TYPE";
//            public static final String IS_INDEXED           = "IS_INDEXED";
//            public static final String REF_KEY_UID          = "REF_KEY_UID";
//            public static final String MATCH_TYPE           = "MATCH_TYPE";
//        }
//    }

    public static class MatchTypes{
        public static final String TABLE_NAME               = "RT_MATCH_TYPES";
        public static class ColumnName{
            public static final String MATCH_TYPE_CODE      = "MATCH_TYPE_CODE";
            public static final String MATCH_TYPE_NM        = "MATCH_TYPE_NM";
            public static final String DESCRIPTION          = "DESCRIPTION";
        }
    }

    public static class MetaBaseProps{
        public static final String TABLE_NAME                  = "RT_MB_PROPS";
        public static class ColumnName{
            public static final String MB_UID               = "MB_UID";
            public static final String PROP_NM              = "PROP_NM";
            public static final String PROP_VALUE           = "PROP_VALUE";
        }
    }

    public static class MetaBaseVersions{
        public static final String TABLE_NAME                  = "RT_MB_VERSIONS";
        public static class ColumnName{
            public static final String MB_UID               = "MB_UID";
            public static final String VERSION              = "VERSION";
            public static final String VERSION_DT           = "VERSION_DT";
        }
    }

        public static class ModelPropNames{
            public static final String TABLE_NAME               = "RT_MDL_PRP_NMS";
            public static class ColumnName{
                public static final String PRP_UID              = "PRP_UID";
                public static final String MDL_UID              = "MDL_UID";
                public static final String PRP_NM               = "PRP_NM";
        }
    }

    public static class ModelPropValues{
        public static final String TABLE_NAME               = "RT_MDL_PRP_VLS";
        public static class ColumnName{
            public static final String PRP_UID              = "PRP_UID";
            public static final String PART_ID              = "PART_ID";
            public static final String PRP_VL               = "PRP_VL";
        }
    }

    public static class Models{
        public static final String TABLE_NAME               = "RT_MDLS";
        public static class ColumnName{
            public static final String MDL_UID              = "MDL_UID";
            public static final String MDL_NM               = "MDL_NM";
            public static final String MDL_VERSION          = "MDL_VERSION";
            public static final String MDL_UUID             = "MDL_UUID";
            public static final String DESCRIPTION          = "DESCRIPTION";
            public static final String IS_PHYSICAL          = "IS_PHYSICAL";
            public static final String MDL_TYPE             = "MDL_TYPE";
            public static final String MDL_URI              = "MDL_URI";
            public static final String MULTI_SOURCED        = "MULTI_SOURCED";
            public static final String VISIBILITY           = "VISIBILITY";           
         }
    }
    
//    public static class Schemas{
//        public static final String TABLE_NAME               = "RT_XSD";
//        public static class ColumnName{
//            public static final String SCHEMA_UID           = "XSD_UID";
//            public static final String POSITION             = "BLOCK_POSITION";
//            public static final String VALUE                = "BLOCK_VALUE";
//        }
//    }
//    
//    public static class DocumentSchemas{
//        public static final String TABLE_NAME               = "RT_XML_DOC_XSD";
//        public static class ColumnName{
//            public static final String DOCUMENT_UID         = "XML_DOCUMENT_UID";
//            public static final String SCHEMA_UID           = "RT_XSD_UID ";
//         }
//    }
//
//    public static class ParameterTypes{
//        public static final String TABLE_NAME               = "RT_PARM_TYPES";
//        public static class ColumnName{
//            public static final String PARM_TYPE            = "PARM_TYPE";
//            public static final String PARM_TYPE_NM         = "PARM_TYPE_NM";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//        }
//    }
//
//    public static class ProcedureParameters{
//        public static final String TABLE_NAME               = "RT_PROC_PARMS";
//        public static class ColumnName{
//            public static final String PARM_NM              = "PARM_NM";
//            public static final String PROC_UID             = "PROC_UID";
//            public static final String DT_UID               = "DT_UID";
//            public static final String RESULTSET_POSITION   = "RESULTSET_POSITION";
//            public static final String PARM_POSITION        = "PARM_POSITION";
//            public static final String PARM_TYPE            = "PARM_TYPE";
//            public static final String OPTIONAL             = "OPTIONAL";
//        }
//    }
//
//    public static class ProcedurePropertyNames{
//        public static final String TABLE_NAME               = "RT_PROC_PRP_NMS";
//            public static class ColumnName{
//                public static final String PRP_UID              = "PRP_UID";
//                public static final String PROC_UID             = "PROC_UID";
//                public static final String PRP_NM               = "PRP_NM";
//        }
//    }
//
//    public static class ProcedurePropertyValues{
//        public static final String TABLE_NAME               = "RT_PROC_PRP_VLS";
//        public static class ColumnName{
//            public static final String PRP_UID              = "PRP_UID";
//            public static final String PART_ID              = "PART_ID";
//            public static final String PRP_VL               = "PRP_VL";
//        }
//    }
//
//    public static class ProcedureTypes{
//        public static final String TABLE_NAME               = "RT_PROC_TYPES";
//        public static class ColumnName{
//            public static final String PROC_TYPE            = "PROC_TYPE";
//            public static final String PROC_TYPE_NM         = "PROC_TYPE_NM";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//        }
//    }
//
//    public static class Procedures {
//        public static final String TABLE_NAME               = "RT_PROCS";
//        public static class ColumnName{
//            public static final String PROC_UID             = "PROC_UID";
//            public static final String PROC_NM              = "PROC_NM";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//            public static final String ALIAS                = "ALIAS";
//            public static final String MDL_UID              = "MDL_UID";
//            public static final String PATH                 = "PATH_1";
//            public static final String RETURNS_RESULTS      = "RETURNS_RESULTS";
//            public static final String PROC_TYPE            = "PROC_TYPE";
//        }
//    }
//
//    public static class QueryPlans{
//        public static final String TABLE_NAME               = "RT_QUERY_PLANS";
//        public static class ColumnName{
//            public static final String GRP_UID              = "GRP_UID";
//            public static final String PART_ID              = "PART_ID";
//            public static final String QUERY_PLAN           = "QUERY_PLAN";
//            public static final String QUERY_PLAN_TYPE      = "QUERY_PLAN_TYPE";
//        }
//    }
//
//    public static class SearchTypes{
//        public static final String TABLE_NAME               = "RT_SEARCH_TYPES";
//        public static class ColumnName{
//            public static final String SEARCH_TYPE_CODE     = "SEARCH_TYPE_CODE";
//            public static final String SEARCH_TYPE_NM       = "SEARCH_TYPE_NM";
//            public static final String DESCRIPTION          = "DESCRIPTION";
//        }
//    }

    public static class VirtualDatabases{
        public static final String TABLE_NAME               = "RT_VIRTUAL_DBS";
        public static class ColumnName{
            public static final String VDB_UID              = "VDB_UID";
            public static final String VDB_VERSION          = "VDB_VERSION";
            public static final String VDB_NM               = "VDB_NM";
            public static final String DESCRIPTION          = "DESCRIPTION";
            public static final String PROJECT_GUID         = "PROJECT_GUID";
            public static final String VDB_STATUS           = "VDB_STATUS";
            public static final String WSDL_DEFINED         = "WSDL_DEFINED";
            public static final String VERSION_BY           = "VERSION_BY";
            public static final String VERSION_DATE         = "VERSION_DATE";
            public static final String CREATED_BY           = "CREATED_BY";
            public static final String CREATION_DATE        = "CREATION_DATE";
            public static final String UPDATED_BY           = "UPDATED_BY";
            public static final String UPDATED_DATE         = "UPDATED_DATE";
            public static final String VDB_FILE_NAME        = "VDB_FILE_NM";
        }
    }

    public static class VDBModels{
        public static final String TABLE_NAME               = "RT_VDB_MDLS";
        public static class ColumnName{
            public static final String VDB_UID              = "VDB_UID";
            public static final String MDL_UID              = "MDL_UID";
            public static final String CNCTR_BNDNG_NM       = "CNCTR_BNDNG_NM";
        }
    }

    //special views
    public static class VCommonProperties{
        public static class ColumnName{
            public static final String PRP_UID              = "PRP_UID";
            public static final String PRP_NM               = "PRP_NM";
            public static final String PART_ID              = "PART_ID";
            public static final String PRP_VL               = "PRP_VL";
        }
    }

    public static final class VModelProperties{
        public static final String TABLE_NAME               = "RT_MDL_PRP_NMS, RT_MDL_PRP_VLS";
        public static class ColumnName{
            public static final String MDL_UID              = "MDL_UID";
        }
    }

//    public static final class VElementProperties{
//        public static final String TABLE_NAME               = "RT_ELMNT_PRP_NMS, RT_ELMNT_PRP_VLS";
//        public static class ColumnName{
//            public static final String ELMNT_UID            = "ELMNT_UID";
//        }
//    }
//
//    public static final class VGroupProperties{
//        public static final String TABLE_NAME               = "RT_GRP_PRP_NMS, RT_GRP_PRP_VLS";
//        public static class ColumnName{
//            public static final String GRP_UID              = "GRP_UID";
//        }
//    }
//
//    public static final class VProcedureProperties{
//        public static final String TABLE_NAME               = "RT_PROC_PRP_NMS, RT_PROC_PRP_VLS";
//        public static class ColumnName{
//            public static final String PROC_UID             = "PROC_UID";
//        }
//    }
//
//    public static final class VDataTypeProperties{
//        public static final String TABLE_NAME               = "RT_DT_PRP_NMS, RT_DT_PRP_VLS";
//        public static class ColumnName{
//            public static final String DT_UID               = "DT_UID";
//        }
//    }
//
//    public static final class VKeyIndexProperties{
//        public static final String TABLE_NAME               = "RT_KY_IDX_PRP_NMS, RT_KY_IDX_PRP_VLS";
//        public static class ColumnName{
//            public static final String KEY_UID              = "KEY_UID";
//        }
//    }
//
//    public static final class VDTElementProperties{
//        public static final String TABLE_NAME               = "RT_DT_ELMNTS_PR_NMS, RT_DT_ELMNTS_PR_VLS";
//        public static class ColumnName{
//            public static final String ELMNT_UID              = "DT_ELMNT_UID";
//        }
//    }
}

