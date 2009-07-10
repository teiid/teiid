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

package org.teiid.metadata.index;

import org.teiid.connector.metadata.runtime.MetadataConstants;

/**
 * IndexConstants
 */
public class IndexConstants {

    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    public final static String INDEX_EXT        = "INDEX";     //$NON-NLS-1$
    public final static String SEARCH_INDEX_EXT = "SEARCH_INDEX";     //$NON-NLS-1$
	public static final String EXTENSION_CHAR   = "."; //$NON-NLS-1$

    public final static char NAME_DELIM_CHAR = '.';

    //Index file name Constants
    public static class INDEX_NAME {
        public final static String TABLES_INDEX            = "TABLES.INDEX"; //$NON-NLS-1$
        public final static String KEYS_INDEX              = "KEYS.INDEX"; //$NON-NLS-1$        
        public final static String COLUMNS_INDEX           = "COLUMNS.INDEX"; //$NON-NLS-1$
        public final static String MODELS_INDEX            = "MODELS.INDEX"; //$NON-NLS-1$
        public final static String VDBS_INDEX              = "VDBS.INDEX"; //$NON-NLS-1$
        public final static String PROCEDURES_INDEX        = "PROCEDURES.INDEX"; //$NON-NLS-1$        
        public final static String DATATYPES_INDEX         = "DATATYPES.INDEX"; //$NON-NLS-1$
        public final static String SELECT_TRANSFORM_INDEX  = "SELECT_TRANSFORM.INDEX"; //$NON-NLS-1$
        public final static String INSERT_TRANSFORM_INDEX  = "INSERT_TRANSFORM.INDEX"; //$NON-NLS-1$
        public final static String UPDATE_TRANSFORM_INDEX  = "UPDATE_TRANSFORM.INDEX"; //$NON-NLS-1$
        public final static String DELETE_TRANSFORM_INDEX  = "DELETE_TRANSFORM.INDEX"; //$NON-NLS-1$
        public final static String PROC_TRANSFORM_INDEX    = "PROC_TRANSFORM.INDEX"; //$NON-NLS-1$
        public final static String MAPPING_TRANSFORM_INDEX = "MAPPING_TRANSFORM.INDEX"; //$NON-NLS-1$
        public final static String ANNOTATION_INDEX        = "ANNOTATION.INDEX"; //$NON-NLS-1$
        public final static String PROPERTIES_INDEX        = "PROPERTIES.INDEX"; //$NON-NLS-1$
        public final static String FILES_INDEX        	 = "FILES.INDEX"; //$NON-NLS-1$
        public final static String[] INDEX_NAMES  = new String[]{TABLES_INDEX, KEYS_INDEX, 
                                                                  COLUMNS_INDEX, PROCEDURES_INDEX, MODELS_INDEX, 
                                                                  VDBS_INDEX, DATATYPES_INDEX, SELECT_TRANSFORM_INDEX,
                                                                  INSERT_TRANSFORM_INDEX, UPDATE_TRANSFORM_INDEX,
                                                                  DELETE_TRANSFORM_INDEX, PROC_TRANSFORM_INDEX,
                                                                  MAPPING_TRANSFORM_INDEX, ANNOTATION_INDEX, PROPERTIES_INDEX, FILES_INDEX};

        public final static boolean isKnownIndex(String indexName) {
            for (int i = 0; i < INDEX_NAMES.length; i++) {
                if (INDEX_NAMES[i].equalsIgnoreCase(indexName)) {
                    return true;
                }
            }
            return false;
        }
    }

    //Search Record type Constants
	public static class SEARCH_RECORD_TYPE {
		public final static char RESOURCE       	= 'A';
		public final static char MODEL_IMPORT       = 'B';
		public final static char OBJECT       	    = 'C';
		public final static char OBJECT_REF     	= 'D';
		public final static char RELATIONSHIP       = 'E';
		public final static char RELATED_OBJECT     = 'F';
		public final static char RELATIONSHIP_TYPE  = 'G';
		public final static char RELATIONSHIP_ROLE  = 'H';
        public final static char TYPED_OBJECT       = 'I';
        public final static char ANNOTATION         = 'J';
	}

	public static final char[] SEARCH_RECORD_TYPES = new char[]{
		SEARCH_RECORD_TYPE.RESOURCE,
		SEARCH_RECORD_TYPE.MODEL_IMPORT,
		SEARCH_RECORD_TYPE.OBJECT,
		SEARCH_RECORD_TYPE.OBJECT_REF,
		SEARCH_RECORD_TYPE.RELATIONSHIP,
		SEARCH_RECORD_TYPE.RELATED_OBJECT,
		SEARCH_RECORD_TYPE.RELATIONSHIP_TYPE,
		SEARCH_RECORD_TYPE.RELATIONSHIP_ROLE,
        SEARCH_RECORD_TYPE.TYPED_OBJECT,
        SEARCH_RECORD_TYPE.ANNOTATION
	};

    public static final char[] RECORD_TYPES = new char[]{
        MetadataConstants.RECORD_TYPE.MODEL,
        MetadataConstants.RECORD_TYPE.TABLE,
        MetadataConstants.RECORD_TYPE.RESULT_SET,
        MetadataConstants.RECORD_TYPE.JOIN_DESCRIPTOR,
        MetadataConstants.RECORD_TYPE.CALLABLE,
        MetadataConstants.RECORD_TYPE.CALLABLE_PARAMETER,
        MetadataConstants.RECORD_TYPE.COLUMN,
        MetadataConstants.RECORD_TYPE.ACCESS_PATTERN,
        MetadataConstants.RECORD_TYPE.UNIQUE_KEY,
        MetadataConstants.RECORD_TYPE.FOREIGN_KEY,
        MetadataConstants.RECORD_TYPE.PRIMARY_KEY,
        MetadataConstants.RECORD_TYPE.INDEX,
        MetadataConstants.RECORD_TYPE.DATATYPE,
        //RECORD_TYPE.DATATYPE_ELEMENT,
        //RECORD_TYPE.DATATYPE_FACET,
        MetadataConstants.RECORD_TYPE.SELECT_TRANSFORM,
        MetadataConstants.RECORD_TYPE.INSERT_TRANSFORM,
        MetadataConstants.RECORD_TYPE.UPDATE_TRANSFORM,
        MetadataConstants.RECORD_TYPE.DELETE_TRANSFORM,
        MetadataConstants.RECORD_TYPE.PROC_TRANSFORM,
        MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM,
        MetadataConstants.RECORD_TYPE.VDB_ARCHIVE,
        MetadataConstants.RECORD_TYPE.ANNOTATION,
        MetadataConstants.RECORD_TYPE.PROPERTY,
        MetadataConstants.RECORD_TYPE.FILE,
		MetadataConstants.RECORD_TYPE.RECORD_CONTINUATION
    };

    public static class RECORD_STRING {
        public final static char TRUE              = '1';
        public final static char FALSE             = '0';
        public final static char SPACE             = ' ';
        // The record delimiter is the character used to delineate the fields of an index record.
        // Since a field in our index records may represent a description, sql string, or xml/html text
        // a delimiter was chosen that will never be present in typed text.
        public final static char RECORD_DELIMITER  = '\u00A0';  // separator between data values (ASCII no-break space)
//        public final static char RECORD_DELIMITER  = '|';  // separator between data values

        // Fix for defect 13393
        public final static char LIST_DELIMITER_OLD = ',';  // separator used when the data value is a list
		public final static char PROP_DELIMITER_OLD = '=';  // separator used to seperate prop-value pairs
        public final static char LIST_DELIMITER     = '\u001F';  // separator used when the data value is a list (ASCII unit separator)
        public final static char PROP_DELIMITER     = '\u2060';  // separator used to seperate prop-value pairs (ASCII word joiner)

        public final static char MATCH_CHAR        = '*';  // wild card that may match one or more characters
        public final static char SINGLE_CHAR_MATCH = '?';  // match a single character    
        public final static String MATCH_CHAR_STRING        = (new Character(MATCH_CHAR)).toString();         // wild card that may match one or more characters
        public final static String SINGLE_CHAR_MATCH_STRING = (new Character(SINGLE_CHAR_MATCH)).toString();  // match a single character    

        public final static char INDEX_VERSION_MARKER = '\u00A1';  // marker to indicate index version information (ASCII inverted exclamation mark)
    }

}
