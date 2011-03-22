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

package org.teiid.query.unittest;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FakeMetadataObject implements Comparable, Serializable {

	public static final String MODEL = "Model"; //$NON-NLS-1$
	public static final String GROUP = "Group"; //$NON-NLS-1$
	public static final String ELEMENT = "Element"; //$NON-NLS-1$
    public static final String KEY = "Key"; //$NON-NLS-1$
    public static final String PROCEDURE = "Procedure"; //$NON-NLS-1$
    public static final String PARAMETER = "Parameter"; //$NON-NLS-1$
    public static final String RESULT_SET = "ResultSet"; //$NON-NLS-1$

    //KEY TYPES
    public static final Integer TYPE_PRIMARY_KEY = new Integer(2);
    public static final Integer TYPE_FOREIGN_KEY = new Integer(3);
    public static final Integer TYPE_INDEX = new Integer(4);
    public static final Integer TYPE_ACCESS_PATTERN = new Integer(5);


	private String name;
	private String type;
	private Object defaultValue;	
	private Map props = new HashMap();
	private Properties extensionProps;
	
	public FakeMetadataObject(String name, String type) { 
		this.name = name;
		this.type = type;
	}

	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	public String getName() {
		return this.name;
	}

	public String getType() { 
		return this.type;
	}

	public Object getDefaultValue() {
		return this.defaultValue;
	}

	public void putProperty(String propName, Object value) {
		props.put(propName, value);
	}
	
	public Object getProperty(String propName) { 
		return props.get(propName);
	}
	
	public Object getProperty(String propName, Object defaultValue) { 
        Object result = props.get(propName);
        if (result == null) {
            return defaultValue;
        }
        return result;
    }
		
	public boolean equals(Object obj) { 
		if(this == obj) { 
			return true;
		}
		
		if(obj == null || ! (obj instanceof FakeMetadataObject)) { 
			return false;
		}
		
		FakeMetadataObject other = (FakeMetadataObject) obj;	
		return (	this.getType().equals(other.getType()) && 
					this.getName().equalsIgnoreCase(other.getName()) );
	}
	
	public int hashCode() { 
		return this.getName().toUpperCase().hashCode();
	}
	
	public int compareTo(Object obj) { 
		FakeMetadataObject other = (FakeMetadataObject) obj;

        if ( this.getProperty(Props.INDEX) != null &&
            other.getProperty(Props.INDEX) != null){

            return compareToWithIndices(other);           
        }
        return compareToWithHashCodes(other);
	}

    private int compareToWithIndices(FakeMetadataObject other) { 
        Integer otherIndex = (Integer)other.getProperty(Props.INDEX);
        Integer myIndex = (Integer)this.getProperty(Props.INDEX);
        return myIndex.compareTo(otherIndex);
    }
    
    private int compareToWithHashCodes(FakeMetadataObject other) { 
            
        int otherHash = other.hashCode();
        int myHash = this.hashCode();
        
        if(myHash < otherHash) { 
            return -1;
        } else if(myHash > otherHash) { 
            return 1;
        } else {
            return 0;
        }
    }
    
    public void setExtensionProp(String name, String value) {
		if (this.extensionProps == null) {
			this.extensionProps = new Properties();
		}
		this.extensionProps.setProperty(name, value);
	}
    
    public Properties getExtensionProps() {
		return extensionProps;
	}
	
	public String toString() { 
		return getType() + "(" + getName() + ")"; //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	
	public static class Props { 
        // Shared properties
		public static final String IS_VIRTUAL = "isVirtual";  // model/group //$NON-NLS-1$
		public static final String MODEL = "model";			// element/group/procedure //$NON-NLS-1$
		public static final String TYPE = "type";			// element/parameter //$NON-NLS-1$
        public static final String INDEX = "index";           // element/parameter - integer //$NON-NLS-1$
        public static final String NAME_IN_SOURCE = "nameInSource";  // group/element //$NON-NLS-1$
	
		// Model properties
        public static final String PLAN = "plan";                     // object - query plan //$NON-NLS-1$
		public static final String UPDATE = "supUpdate";              // Boolean //$NON-NLS-1$
        public static final String MAX_SET_SIZE = "maxSetSize";       // Integer //$NON-NLS-1$

        // Group properties
        public static final String TEMP = "temp";                     // Boolean //$NON-NLS-1$
        public static final String KEYS = "keys";                     // Collection<FakeMetadataObject keys> //$NON-NLS-1$
		public static final String CARDINALITY = "cardinality";       // Integer - estimated size of table (# of rows) //$NON-NLS-1$
        public static final String MAT_GROUP = "matGroup";            // FakeMetadataObject - materialization group //$NON-NLS-1$
        public static final String MAT_STAGE = "matStage";            // FakeMetadataObject - materialization staging group //$NON-NLS-1$
        public static final String XML_SCHEMAS = "xmlSchemas";        // List<String> - xml schemas for a doc //$NON-NLS-1$
		
		// Element properties		
        public static final String GROUP = "group";                   // FakeMetadataObject group         //$NON-NLS-1$
		public static final String SELECT = "supSelect";              // Boolean //$NON-NLS-1$
		public static final String SEARCHABLE_LIKE = "supLike";       // Boolean //$NON-NLS-1$
		public static final String SEARCHABLE_COMPARE = "supCompare"; // Boolean //$NON-NLS-1$
		public static final String NULL = "supNull";                  // Boolean //$NON-NLS-1$
		public static final String AUTO_INCREMENT = "supAutoIncrement";   // Boolean //$NON-NLS-1$
		public static final String DEFAULT_VALUE = "supDefaultvalue";	// Boolean	 //$NON-NLS-1$
        public static final String LENGTH = "length";   // Integer   //$NON-NLS-1$
        public static final String CASE_SENSITIVE = "caseSensitive";   // Boolean //$NON-NLS-1$
        public static final String SIGNED = "signed";   // Boolean //$NON-NLS-1$
        public static final String PRECISION = "precision";   // Integer //$NON-NLS-1$
        public static final String SCALE = "scale";   // Integer //$NON-NLS-1$
        public static final String RADIX = "radix";   // Integer //$NON-NLS-1$
        public static final String NATIVE_TYPE = "nativeType";  // String  //$NON-NLS-1$
        public static final String MAX_VALUE = "maxValue";      // String //$NON-NLS-1$
        public static final String MIN_VALUE = "minValue";      // String //$NON-NLS-1$
        public static final String DISTINCT_VALUES = "distinctValues";      // Integer //$NON-NLS-1$
        public static final String NULL_VALUES = "nullValues";      // Integer //$NON-NLS-1$
        public static final String MODELED_TYPE = "modeledType";    // String //$NON-NLS-1$
        public static final String MODELED_BASE_TYPE = "baseType";    // String //$NON-NLS-1$
        public static final String MODELED_PRIMITIVE_TYPE = "primitiveType";    // String //$NON-NLS-1$
         
        // Key properties
        public static final String KEY_TYPE = "keyType";              // Integer //$NON-NLS-1$
        public static final String KEY_ELEMENTS = "keyElements";      // List<FakeMetadataObject elements> //$NON-NLS-1$
        public static final String REFERENCED_KEY = "referencedKey";       // FakeMetadataObject referenced primary key (if this is a fk) //$NON-NLS-1$
        
        // Procedure properties
        public static final String PARAMS = "params";                 // List<FakeMetadataObject parameters> //$NON-NLS-1$
        public static final String INSERT_PROCEDURE = "insertProcedure"; // string giving the insert procedure //$NON-NLS-1$
        public static final String UPDATE_PROCEDURE = "updateProcedure"; // string giving the update procedure //$NON-NLS-1$
        public static final String DELETE_PROCEDURE = "deleteProcedure"; // string giving the delete procedure //$NON-NLS-1$
        public static final String UPDATE_COUNT = "updateCount";       // integer giving the update count //$NON-NLS-1$
        
        // Parameter properties
        public static final String DIRECTION = "direction";           // integer - see query.sql.lang.SPParameter //$NON-NLS-1$
        public static final String RESULT_SET = "isRS";               // FakeMetadataObject result set //$NON-NLS-1$
        
        // Result set properties
        public static final String COLUMNS = "columns";               // List<FakeMetadataObject elements> //$NON-NLS-1$
	}

}
