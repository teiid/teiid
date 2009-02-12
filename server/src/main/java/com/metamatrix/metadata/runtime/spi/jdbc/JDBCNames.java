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

package com.metamatrix.metadata.runtime.spi.jdbc;

public class JDBCNames {

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

}

