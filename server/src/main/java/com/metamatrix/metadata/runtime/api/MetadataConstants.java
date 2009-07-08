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

package com.metamatrix.metadata.runtime.api;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import com.metamatrix.common.vdb.api.ModelInfo;


/**
 * MetadataConstants are all the constant values used to identify all the valid values for a multi-value attribute.
 * All assigned short values start with 1.  Therefore, when the get...TypeName(type) method is called, the
 * method needs to subtract 1 from the argument.
 * 
 * 2/28/06 VAH - commented out the statics that were no longer referenced so that no maintenanance is required in the future
 *              These variables have been moved to ResourceNameUtil.
 */
final public class MetadataConstants {
	/**
	 *  Definition of not defined long type.
	 */
    public static final long NOT_DEFINED_LONG = Long.MIN_VALUE;
	/**
	 *  Definition of not defined int type.
	 */
    public static final int NOT_DEFINED_INT = Integer.MIN_VALUE;
	/**
	 *  Definition of not defined short type.
	 */
    public static final short NOT_DEFINED_SHORT = Short.MIN_VALUE;

    public final static String BLANK = ""; //$NON-NLS-1$
    
    //properties
    public static final String VERSION_DATE = "versionDate"; //$NON-NLS-1$
   

	/**
	 * These types are associated with a KEY, indicating the type of matching that can be performed on it.
	 */
    final public static class MATCH_TYPES {
        public final static short FULL_MATCH = 1;
        public final static short PARTIAL_MATCH = 2;
        public final static short NEITHER_MATCH = 3;
        public final static short NA = 4;
    }

    final static String[] MATCH_TYPE_NAMES = {"Full", "Partial",  "Neither", "N/A"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

    public final static String getMatchTypeName(short type) {
        return  MATCH_TYPE_NAMES[type - 1];

    }
    
	/**
	 * These types indicate the type of KEY it is. 
	 */
    final public static class KEY_TYPES {
        public final static short PRIMARY_KEY    = 1;
        public final static short FOREIGN_KEY    = 2;
        public final static short UNIQUE_KEY     = 3;
        public final static short NON_UNIQUE_KEY = 4;
        public final static short ACCESS_PATTERN = 5;
        public final static short INDEX          = 6;
    }

    final static String[] KEY_TYPE_NAMES = {"Primary", "Foreign",  "Unique", "NonUnique", "AccessPattern", "Index"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$

    public final static String getKeyTypeName(short type) {
        return  KEY_TYPE_NAMES[type - 1];
    }


	/**
	 * These types indicate the type of PROCEDURE it it. 
	 */
    public final static class PROCEDURE_TYPES {
        public final static short FUNCTION=1;
        public final static short STORED_PROCEDURE = 2;
        public final static short STORED_QUERY = 3;
    }

    final static String[] PROCEDURE_TYPE_NAMES = {"Function", "StoredProc", "StoredQuery"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    public final static String getProcedureTypeName(short type) {
        return PROCEDURE_TYPE_NAMES[type - 1];
    }

    public static short getProcType(String typeName){
        if("Function".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return PROCEDURE_TYPES.FUNCTION;
        else if("StoredProc".equalsIgnoreCase(typeName) || "StoredProcedure".equalsIgnoreCase(typeName)) //$NON-NLS-1$ //$NON-NLS-2$
            return PROCEDURE_TYPES.STORED_PROCEDURE;
        else if("StoredQuery".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return PROCEDURE_TYPES.STORED_QUERY;

        return NOT_DEFINED_SHORT;
    }
    
    public final static class QUERY_TYPES {
        public static final int SELECT_QUERY = 0;
        public static final int UPDATE_QUERY = 1;
        public static final int INSERT_QUERY = 2;
        public static final int DELETE_QUERY = 3;
        public static final int[] TYPES = new int[]{SELECT_QUERY, UPDATE_QUERY, INSERT_QUERY, DELETE_QUERY};
        public static final String[] TYPE_NAMES = new String[]{"SelectQuery","UpdateQuery","InsertQuery","DeleteQuery"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
    /**
     * These types indicate the type of PROCEDURE it it. 
     */
    public final static class QUERY_PLAN_TYPES {
        public final static short MAPPING_DEFN            = 0;
        public final static short QUERY_PLAN_GROUP        = 1;
        public final static short QUERY_PLAN_STORED_QUERY = 2;
        public final static short QUERY_PLAN_INSERT_QUERY = 3;
        public final static short QUERY_PLAN_UPDATE_QUERY = 4;
        public final static short QUERY_PLAN_DELETE_QUERY = 5;
    }

    final static String[] QUERY_PLAN_TYPE_NAMES = {"MappingDefn",  //$NON-NLS-1$
                                                    "QueryPlanGroup",  //$NON-NLS-1$
                                                    "QueryPlanStoredQuery", //$NON-NLS-1$
                                                    "QueryPlanInsertQuery", //$NON-NLS-1$
                                                    "QueryPlanUpdateQuery", //$NON-NLS-1$
                                                    "QueryPlanDeleteQuery"}; //$NON-NLS-1$

    public final static String getQueryPlanTypeName(short type) {
        return QUERY_PLAN_TYPE_NAMES[type];
    }

    public static short getQueryPlanType(String typeName){
        if("MappingDefn".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return QUERY_PLAN_TYPES.MAPPING_DEFN;
        else if("QueryPlanGroup".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return QUERY_PLAN_TYPES.QUERY_PLAN_GROUP;
        else if("QueryPlanStoredQuery".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return QUERY_PLAN_TYPES.QUERY_PLAN_STORED_QUERY;

        return NOT_DEFINED_SHORT;
    }

	/**
	 * These types indicate the type of  PROCEDURE_PARAMETER it is.
	 */
    public final static class PARAMETER_TYPES {
        public final static short IN_PARM = 1;
        public final static short OUT_PARM = 2;
        public final static short INOUT_PARM = 3;
        public final static short RETURN_VALUE = 4;
        public final static short RESULT_SET = 5;
    }

    final static String[] PARAMETER_TYPE_NAMES = {"In", "Out",  "InOut", "ReturnValue", "ResultSet"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$

    public final static String getParameterTypeName(short type) {
        return PARAMETER_TYPE_NAMES[type - 1];
    }

    public static short getParameterType(String typeName){
        if("In".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return PARAMETER_TYPES.IN_PARM;
        else if("Out".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return PARAMETER_TYPES.OUT_PARM;
        else if("InOut".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return PARAMETER_TYPES.INOUT_PARM;
        else if("ReturnValue".equalsIgnoreCase(typeName) || "Return".equalsIgnoreCase(typeName)) //$NON-NLS-1$ //$NON-NLS-2$
            return PARAMETER_TYPES.RETURN_VALUE;
        else if("ResultSet".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return PARAMETER_TYPES.RESULT_SET;

        return NOT_DEFINED_SHORT;
    }
    
	/**
	 * These types are associated with the Element having valid search types. 
	 */
    public final static class SEARCH_TYPES {
        public final static short SEARCHABLE = 1;
        public final static short ALLEXCEPTLIKE = 2;
        public final static short LIKE_ONLY = 3;
        public final static short UNSEARCHABLE = 4;
    }

    final static String[] SEARCH_TYPE_NAMES = {"Searchable", "All Except Like",  "Like Only", "Unsearchable"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

    public final static String getSearchTypeName(short type) {
        return SEARCH_TYPE_NAMES[type - 1];
    }

    public final static short getSearchType(String typeName) {
        if("Searchable".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return SEARCH_TYPES.SEARCHABLE;
        else if("All Except Like".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return SEARCH_TYPES.ALLEXCEPTLIKE;
        else if("Like Only".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return SEARCH_TYPES.LIKE_ONLY;
        else if("Unsearchable".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return SEARCH_TYPES.UNSEARCHABLE;

        return NOT_DEFINED_SHORT;
    }

	/**
	 * A DataType object will be identified as being of one of these types.
	 */
    final public static class DATATYPE_TYPES {
        public final static short BASIC = 1;
        public final static short USER_DEFINED = 2;
        public final static short RESULT_SET = 3;
    }

    final static String[] DATATYPE_TYPE_NAMES = {"Basic", "UserDefined",  "ResultSet"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    public final static String getDataTypeTypeName(short type) {
        return DATATYPE_TYPE_NAMES[type - 1];
    }

    public static short getDataTypeType(String typeName){
        if("Basic".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return DATATYPE_TYPES.BASIC;
        else if("UserDefined".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return DATATYPE_TYPES.USER_DEFINED;
        else if("ResultSet".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return DATATYPE_TYPES.RESULT_SET;

        return NOT_DEFINED_SHORT;
    }
    
	/**
	 * These types represent the type of table a Group is. 
	 */
    final public static class TABLE_TYPES {
        public static final short TABLE_TYPE           = 1;
        public static final short SYSTEM_TYPE          = 2;
        public static final short VIEW_TYPE            = 3;
        public static final short DOCUMENT_TYPE        = 4;
        public static final short MAPPING_CLASS_TYPE   = 5;
        public static final short XML_TEMP_TABLE_TYPE  = 6;
        public static final short SYSTEM_DOCUMENT_TYPE = 7;
    }

    final static String[] TABLE_TYPE_NAMES = {"Table", "SystemTable", "View", "Document", "MappingClass", "XmlTempTable"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$

    public final static String getTableTypeName(short type) {
        return TABLE_TYPE_NAMES[type - 1];
    }

	/**
	 * These types are associated with a DataType or an Element needing the indication of null types. 
	 */
    final public static class NULL_TYPES {
        public static final short NOT_NULL = 1;
        public static final short NULLABLE = 2;
        public static final short UNKNOWN = 3;
    }

    final static String[] NULL_TYPE_NAMES = {"Not Nullable", "Nullable",  "Unknown"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    public final static String getNullTypeName(short type) {
        return NULL_TYPE_NAMES[type - 1];
    }

    public static short getNullType(String typeName){
        if("Not Nullable".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return NULL_TYPES.NOT_NULL;
        else if("Nullable".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return NULL_TYPES.NULLABLE;
        else if("Unknown".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return NULL_TYPES.UNKNOWN;

        return NOT_DEFINED_SHORT;
    }


//    /**
//     * These are virtual database model status values
//     */
//    final public static class VDB_MODEL_STATUS {
//        public static final short INCOMPLETE = 1;
//        public static final short LOADING    = 2;
//        public static final short ACCESSIBLE = 3;
//        public static final short UNLOADING  = 4;
//        public static final short DELETED    = 5;
//    }
//
//    final static String[] VDB_MODEL_STATUS_NAMES = {"Incomplete", "Loading",  "Accessible", "Unloading", "Deleted"};
//
//    public final static String getVDBModelStatusName(short status) {
//        return VDB_MODEL_STATUS_NAMES[status - 1];
//    }

    /**
     * Visibility type constants.
     */
    public final static class VISIBILITY_TYPES{
		public static final short PUBLIC_VISIBILITY = ModelInfo.PUBLIC;
		public static final short PRIVATE_VISIBILITY = ModelInfo.PRIVATE;
    }

	public final static String[] VISIBILITY_TYPE_NAMES = {"Public", "Private"}; //$NON-NLS-1$ //$NON-NLS-2$

	public final static String getVisibilityTypeName(short type){
        if(type == VISIBILITY_TYPES.PUBLIC_VISIBILITY)
            return VISIBILITY_TYPE_NAMES[0];
        else if(type == VISIBILITY_TYPES.PRIVATE_VISIBILITY)
            return VISIBILITY_TYPE_NAMES[1];
        return null;
    }

	public final static short getVisibilityType(String typeName){
        if("Public".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return VISIBILITY_TYPES.PUBLIC_VISIBILITY;
        else if("Private".equalsIgnoreCase(typeName)) //$NON-NLS-1$
            return VISIBILITY_TYPES.PRIVATE_VISIBILITY;
        return NOT_DEFINED_SHORT;
    }
    
    /**
     * Simple Type constants
     */
    public final static class SIMPLE_TYPES{    
	   
	    /**
	     * Simple Type facets
	     */
	    public final static class DATA_TYPE_FACETS{
			public final static class FUNDAMENTAL{
				public static final short EQUAL = 1;
				public static final short ORDERED = 2;
				public static final short BOUNDED = 3;
				public static final short CARDINALITY = 4;
				public static final short NUMERIC = 5;		
			}
			
			public final static class CONSTRAINING{
				public static final short LENGTH = 6;
				public static final short MIN_LENGTH = 7;
				public static final short MAX_LENGTH = 8;
				public static final short PATTERN = 9;
				public static final short ENUMERATION = 10;
				public static final short WHITE_SPACE = 11;
				public static final short MAX_INCLUSIVE = 12;
				public static final short MIN_INCLUSIVE = 13;
				public static final short MAX_EXCLUSIVE= 14;
				public static final short MIN_EXCLUSIVE = 15;
				public static final short TOTAL_DIGITS = 16;
				public static final short FRACTION_DIGITS = 17;
			}
	    }
	    
	    public final static String[] DATA_TYPE_FACETS_NAMES = {"equal", "ordered", "bounded", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    	"cardinality", "numeric", "length", "minLength", "maxLength", "pattern", "enumeration", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
	    	"whiteSpace", "maxInclusive", "minInclusive", "maxExclusive", "minExclusive", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
	    	"totalDigits", "fractionDigits"}; //$NON-NLS-1$ //$NON-NLS-2$
	    	
	    public final static String getDataTypeFacetName(short facetType){
	    	if(facetType < 1 || facetType > DATA_TYPE_FACETS_NAMES.length){
				return null;
			} 
			
	    	return DATA_TYPE_FACETS_NAMES[facetType -1]; 
	    }
	    
	    public final static short getDataTypeFacet(String facetName){
	    	if(DATA_TYPE_FACETS_NAMES[0].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.FUNDAMENTAL.EQUAL;
	    	}else if(DATA_TYPE_FACETS_NAMES[1].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.FUNDAMENTAL.ORDERED;
	    	}else if(DATA_TYPE_FACETS_NAMES[2].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.FUNDAMENTAL.BOUNDED;
	    	}else if(DATA_TYPE_FACETS_NAMES[3].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.FUNDAMENTAL.CARDINALITY;
	    	}else if(DATA_TYPE_FACETS_NAMES[4].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.FUNDAMENTAL.NUMERIC;
	    	}else if(DATA_TYPE_FACETS_NAMES[5].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.LENGTH;
	    	}else if(DATA_TYPE_FACETS_NAMES[6].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.MIN_LENGTH;
	    	}else if(DATA_TYPE_FACETS_NAMES[7].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.MAX_LENGTH;
	    	}else if(DATA_TYPE_FACETS_NAMES[8].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.PATTERN;
	    	}else if(DATA_TYPE_FACETS_NAMES[9].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.ENUMERATION;
	    	}else if(DATA_TYPE_FACETS_NAMES[10].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.WHITE_SPACE;
	    	}else if(DATA_TYPE_FACETS_NAMES[11].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.MAX_INCLUSIVE;
	    	}else if(DATA_TYPE_FACETS_NAMES[512].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.MIN_INCLUSIVE;
	    	}else if(DATA_TYPE_FACETS_NAMES[13].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.MAX_EXCLUSIVE;
	    	}else if(DATA_TYPE_FACETS_NAMES[14].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.MIN_EXCLUSIVE;
	    	}else if(DATA_TYPE_FACETS_NAMES[15].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.TOTAL_DIGITS;
	    	}else if(DATA_TYPE_FACETS_NAMES[16].equalsIgnoreCase(facetName)){
	    		return DATA_TYPE_FACETS.CONSTRAINING.FRACTION_DIGITS;
	    	}
	    	
	    	
	    	return NOT_DEFINED_SHORT;
	    }
	    
	    /**
	     * White space types. Value of Simple Type whitSpace facet.
	     */
	    public final static class WHITE_SPACE_TYPES{
			public static final short PRESERVE = 1;
			public static final short REPLACE = 2;
			public static final short COLLAPSE = 3;
	    }
	
		public final static String[] WHITE_SPACE_TYPE_NAMES = {"preserve", "replace", "collapse"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	
		public final static String getWhiteSapceTypeName(short type){	
			if(type < 1 || type > WHITE_SPACE_TYPE_NAMES.length){
				return null;
			} 
			
	        return WHITE_SPACE_TYPE_NAMES[type-1];
	    }
	
		public final static short getWhiteSapceType(String typeName){
	        if("preserve".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
	            return WHITE_SPACE_TYPES.PRESERVE;
	        }else if("replace".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
	            return WHITE_SPACE_TYPES.REPLACE;
	        }else if("collapse".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
	            return WHITE_SPACE_TYPES.COLLAPSE;
	        }
	        return NOT_DEFINED_SHORT;
	    }
	    
    	/**
	     * Variety types.
	     */
	    public final static class VARIETY_TYPES{
	    	public static final short ATOMIC = 1;	
	    	public static final short LIST = 2;
	    	public static final short UNION = 3;
	    }
	    
	    public final static String[] VARIETY_TYPE_NAMES = {"atomic", "list", "union"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    
	    public final static String getVarietyTypeName(short type){	
			if(type < 1 || type > VARIETY_TYPE_NAMES.length){
				return null;
			} 
			
	        return VARIETY_TYPE_NAMES[type-1];
	    }
	    
	    public final static short getVarietyType(String typeName){
	        if("atomic".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
	            return VARIETY_TYPES.ATOMIC;
	        }else if("list".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
	            return VARIETY_TYPES.LIST;
	        }else if("union".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
	            return VARIETY_TYPES.UNION;
	        }
	        return NOT_DEFINED_SHORT;
	    }
	    
	    /**
	     * Final types. Can be a subset of "restriction", "list", and "union".
	     */    
	    public final static class FINAL_TYPES{
	    	public static final short RESTRICTION = 1;	
	    	public static final short LIST = 2;
	    	public static final short UNION = 4;
	    	
	    	public static final short RESTRICTION_AND_LIST = 3;
	    	public static final short RESTRICTION_AND_UNION = 5;
	    	public static final short LIST_AND_UNION = 6;
	    	public static final short RESTRICTION_AND_LISTAND_UNION = 7;
	    }
	
	    public final static String[] FINAL_TYPE_NAMES = {"restriction", "list", "union"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    
    	public final static Collection getFinalTypeNames(short type){	
			if(type < 1 || type > 7){
				return Collections.EMPTY_SET;
			} 
			Collection result = new HashSet(3);
			if((type & FINAL_TYPES.RESTRICTION) == FINAL_TYPES.RESTRICTION){
				result.add(FINAL_TYPE_NAMES[0]);
			}
			if((type & FINAL_TYPES.LIST) == FINAL_TYPES.LIST){
				result.add(FINAL_TYPE_NAMES[1]);
			}
			if((type & FINAL_TYPES.UNION) == FINAL_TYPES.UNION){
				result.add(FINAL_TYPE_NAMES[2]);
			}
			
	        return result;
	    }
	    
	    public final static short getFinalType(Collection typeNames){
	    	if(typeNames == null || typeNames.isEmpty()){
	    		return NOT_DEFINED_SHORT;	
	    	}
	    	short result = 0;
	    	Iterator iter = typeNames.iterator();
	    	while(iter.hasNext()){
	    		String typeName = (String)iter.next();
	    
		        if("restriction".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
		            result = (short) (result | FINAL_TYPES.RESTRICTION);
		        }else if("list".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
		            result = (short) (result | FINAL_TYPES.LIST);
		        }else if("union".equalsIgnoreCase(typeName)){ //$NON-NLS-1$
		            result = (short) (result | FINAL_TYPES.UNION);
		        }
	    	}
	        return result;
	    }
    }
    
    /**
     * These property names are used to set connector capability properties on
     * a model.  They are currently used only for SAP-specific connector capabilities.
     * They could be used however, by defining a metamodel extension to add these
     * properties to a model, which is actually what we do for testing. 
     */
    final public static class CAPABILITY_PROPERTY_NAMES {
        public static final String PROP_BLACK_BOX = "supportsBlackBoxJoin"; //$NON-NLS-1$
        public static final String PROP_SINGLE_GROUP_SELECT = "requiresSingleGroupSelect"; //$NON-NLS-1$
        public static final String PROP_LEAF_JOIN_SELECT = "requiresLeafJoinSelect"; //$NON-NLS-1$
    }
    
}

