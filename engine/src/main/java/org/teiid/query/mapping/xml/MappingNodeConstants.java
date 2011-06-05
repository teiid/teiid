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

package org.teiid.query.mapping.xml;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Constants, property names, and property default values for
 * a {@link MappingNode}.
 */
public final class MappingNodeConstants {
   
    // Can't instantiate
    private MappingNodeConstants() { }    

    // =========================================================================
    // CONSTANTS
    // =========================================================================
    
    /**
     * Indicates a search of a mapping document should be upward.  See
     * {@link MappingNode#findFirstNodeWithProperty}
     */
    public static final int SEARCH_UP = 2;

    /**
     * Indicates a search of a mapping document should be downward, DEPTH FIRST.  See
     * {@link MappingNode#findFirstNodeWithProperty}
     */
    public static final int SEARCH_DOWN = 3; // DEPTH FIRST

    /**
     * Indicates a search of a mapping document should be downward, BREADTH FIRST.  See
     * {@link MappingNode#findFirstNodeWithProperty}
     */
    public static final int SEARCH_DOWN_BREADTH_FIRST = 4; //BREADTH FIRST
    
    /** Constant for children returned by non-complex nodes (never have children). */
    static final List NO_CHILDREN = Collections.EMPTY_LIST;    

    /** Schema node path delimeter. */
    public static final String PATH_DELIM = "."; //$NON-NLS-1$
    
    /** The value used to indicate an unbounded maximum cardinality */
    public static final Integer CARDINALITY_UNBOUNDED = new Integer(-1);

    public static final String CARDINALITY_UNBOUNDED_STRING = "unbounded"; //$NON-NLS-1$
    
    /** Constant defining a target node type of "attribute". */
    public static final String ATTRIBUTE = "attribute"; //$NON-NLS-1$

    /** Constant defining a target node type of "element". */
    public static final String ELEMENT   = "element";     //$NON-NLS-1$
    
    /** Constant defining a target node type of "comment". */
    public static final String COMMENT   = "comment";     //$NON-NLS-1$
    
    /** Constant defining a target node type of "sequence". */
    public static final String SEQUENCE = "sequence"; //$NON-NLS-1$

    /** Constant defining a target node type of "choice". */
    public static final String CHOICE = "choice"; //$NON-NLS-1$

    /** Constant defining a target node type of "All". */
    public static final String ALL = "all"; //$NON-NLS-1$
    
    /** Constant defining a target node type of "Criteria". */
    public static final String CRITERIA = "criteria"; //$NON-NLS-1$
    
    /** Constant defining a target node type of "source". */
    public static final String SOURCE = "source"; //$NON-NLS-1$
    

    /**
     * Defines a default namespace holder for the nodes with out any
     * namespace declarations.
     */
    public static final Namespace NO_NAMESPACE = new Namespace("", ""); //$NON-NLS-1$ //$NON-NLS-2$

    /** 
     * This constant is merely used as a placeholder in the declaration of a
     * default namespace in an XML doc instance.  A default namespace has no
     * prefix associated with it, so the String used here is a forbidden XML
     * character that could never be used as a real namespace prefix.
     */
    public static final String DEFAULT_NAMESPACE_PREFIX = ""; //$NON-NLS-1$

    /**
     * Prefix of the XML Schema namespace for instances - needed to make
     * use of the "nil" attribute defined in that namespace, for
     * nillable nodes
     * @see #INSTANCES_NAMESPACE
     */
    public static final String INSTANCES_NAMESPACE_PREFIX = "xsi"; //$NON-NLS-1$
    
    /**
     * The XML Schema namespace for instances - needed to make
     * use of the "nil" attribute defined in that namespace, for
     * nillable nodes
     * @see #INSTANCES_NAMESPACE_PREFIX
     */
    public static final String INSTANCES_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance"; //$NON-NLS-1$
   
    public static final String NAMESPACE_DECLARATION_ATTRIBUTE_NAMESPACE = "xmlns"; //$NON-NLS-1$

    /**
     * PRESERVE -No normalization is done, the value is not changed.
     * REPLACE - All occurrences of tab, line feed and carriage return are replaced with space
     * COLLAPSE - After the processing implied by replace, contiguous sequences of space are 
     * collapsed to a single space, and leading and trailing spaces are removed.
     */
    public static final String NORMALIZE_TEXT_PRESERVE = "preserve"; //$NON-NLS-1$
    public static final String NORMALIZE_TEXT_REPLACE  = "replace"; //$NON-NLS-1$
    public static final String NORMALIZE_TEXT_COLLAPSE = "collapse"; //$NON-NLS-1$
    
    /**
     * Default built-in type = no type info, which is an empty string
     */
    public static final String NO_TYPE = ""; //$NON-NLS-1$
    
    // =========================================================================
    // PROPERTIES
    // =========================================================================

    /** 
     * Property names for type-specific node properties.  Values will be of
     * type String unless otherwise specified.
     */
    public enum Properties {
        
        /** The basic name of this node. Will be the element or attribute tag name. */
        NAME,

        /** 
         * The namespace prefix, which indicates the namespace for this node. 
         * The namespace must be declared either at this node or an ancestor
         * node; use the {@link #NAMESPACE_DECLARATIONS} property.
         */
        NAMESPACE_PREFIX,

        /** 
         * <p>This property allows for one or more namespace declarations
         * (a namespace prefix and a namespace uri) at a given node.  The 
         * object value should be a java.util.Properties object, where each
         * key is a String prefix and each value is the namespace String
         * uri.  The prefix may then be referenced by other nodes via
         * the {@link #NAMESPACE_PREFIX} property.</p>
         * 
         * <p>One common example would be the XML Schema namespace for  
         * instances.  In this case, the common convention is to use
         * "xsi" for the prefix, and the uri is
         * "http://www.w3.org/2001/XMLSchema-instance".  This is commonly
         * declared at the root node of the document instance.  Then, elsewhere,
         * a node may for example use the "nil" attribute from that namespace:
         * 
         * <pre>&lt;shipDate xsi:nil="true"/&gt;</pre>
         * </p>
         */
        NAMESPACE_DECLARATIONS,
       
        /**
         * The target node type.  Can take on one of the values {@link #ATTRIBUTE}
         * or {@link #ELEMENT}.
         */        
        NODE_TYPE,  // Values: ATTRIBUTE|ELEMENT

        /** 
         * <p>The minimum number of times this node must occur in a document.</p>
         * <p>Type: <code>java.lang.Integer</code></p>
         */
        CARDINALITY_MIN_BOUND,

        /** 
         * <p>The maximum number of times this node may occur in a document.</p>
         * <p>Type: <code>java.lang.Integer</code></p>
         */
        CARDINALITY_MAX_BOUND,

        /**
         * An optional constraint that applies for the node.  If a constraint is
         * defined, the input tuple to the node will be compared to the constraint.
         * The node will be processed only if the constraint is satisfied.
         */
        CRITERIA,
        
        /**
         * This property represents a default value for an XML node
         */
        DEFAULT_VALUE,

        /**
         * This property represents a fixed value for an XML node
         */
        FIXED_VALUE,

        /**
         * <p>Value will be of type Boolean.  Indicates that the node is nillable, 
         * i.e. may have a child attribute <code>xsi:nil="true"</code>, 
         * where xsi indicates the W3C namespace for instances.  This explicitly
         * indicates when the element has null content.</p>
         * 
         * <p><b>Note:</b> This property may only be set to true if this node
         * is an element (i.e. the {@link #NODETYPE} property must have 
         * a value of {@link #ELEMENT}), although this constraint is not
         * enforced anywhere in the MappingNode framework.</p>
         */
        IS_NILLABLE,

        /**
         * <p>This node will be completely ignored, not output, not
         * processed</p>
         * <p>Type: <code>java.lang.Boolean</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_IS_EXCLUDED}</p>
         */
        IS_EXCLUDED,


        /** The name of the result being returned by this node */
        RESULT_SET_NAME,

        /** 
         * The name(s) of the temporary group(s) to be materialized at this 
         * document node.
         * <p>Type: <code>java.util.List</code> of </code>java.lang.String</code>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_TEMP_GROUP_NAMES}</p>
         */
        TEMP_GROUP_NAMES,

        /** The symbol from a result set that maps to this node. */
        ELEMENT_NAME,

		/** The temporary property to mark whether this node should be included. */
		IS_INCLUDED,

        /**
         * The text for a comment.
         */
        COMMENT_TEXT,
        
        /** 
         * Indicates that the element or attribute is to be considered optional,
         * even if it has a fixed or default value - it should be removed from the
         * result doc if it doesn't have child content or doesn't receive character
         * content from the underlying data store.  See defect 12077
         * <p>Type: <code>java.lang.Boolean</code> 
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_IS_OPTIONAL}</p>
         */
        IS_OPTIONAL,
        
        /**
         * <p>Indicates the level of text normalization that will be applied
         * to the text content for a given element or attribute.  </p>
         * <p>Type: <code>java.lang.String</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_NORMALIZE_TEXT}</p>
         */
        NORMALIZE_TEXT,
        
        /**
         * Specifies the design-time base built-in type for the virtual document node.
         * This property is <strong>optional</strong> and may not exist, particularly for 
         * legacy XML virtual docs or for nodes that are not mapped to data.  This information
         * is used to determine special translations from the runtime value to the expected
         * XML schema output value string.  
         */
        BUILT_IN_TYPE,
        
        // ========================================================================
        // CHOICE NODE RELATED PROPERTIES
        // ========================================================================

        /**
         * <p>This property of a <i>single child</i> of a choice node marks that
         * child as representing the default choice of the choice node.</p>
         * <p>Type: <code>java.lang.Boolean</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_IS_DEFAULT_CHOICE}</p>
         */
        IS_DEFAULT_CHOICE,
        
        /**
         * <p>This property of a choice node indicates that, by 
         * default (if none of the choices evaluate to true), an 
         * exception will be thrown.  The order in which this
         * will be considered: first, check that a child node
         * is marked as the {@link #IS_DEFAULT_CHOICE default choice};
         * if not, then check this property.  If it is true, throw
         * an exception, if false, do nothing.</p>
         * <p>Type: <code>java.lang.Boolean</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_EXCEPTION_ON_DEFAULT}</p>
         */
        EXCEPTION_ON_DEFAULT,

        // ========================================================================
        // RECURSI0N RELATED PROPERTIES
        // ========================================================================

        /**
         * Indicates if the node is the root of a recursive XML fragment or not.
         * <p>Type: <code>java.lang.Boolean</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_IS_RECURSIVE}</p>
         */
        IS_RECURSIVE,

        /**
         * The criteria of a node representing the root of a recursive
         * XML fragment.  The criteria should specify under what circumstances the
         * recursion should terminate.  i.e. "resultSetName.employeeName = 'Jones'"
         * @see #RECURSION_LIMIT
         */
        RECURSION_CRITERIA,

        /**
         * The recursion limit of a recursive XML fragment - if the 
         * {@link #RECURSION_CRITERIA} does not terminate the recursion before
         * the limit is reached, the recursion will be terminated after this
         * many iterations.  This is to prevent runaway recursion.
         * <p>Type: <code>java.lang.Integer</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_RECURSION_LIMIT}</p>
         * @see #EXCEPTION_ON_RECURSION_LIMIT
         */
        RECURSION_LIMIT,

        /**
         * If recursion is terminated due to the safeguard {@link #RECURSION_LIMIT} being
         * reached, this property controls whether an exception will be thrown or not.
         * <p>Type: <code>java.lang.Boolean</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_EXCEPTION_ON_RECURSION_LIMIT}</p>
         * @see #RECURSION_CRITERIA
         * @see #RECURSION_LIMIT
         */
        EXCEPTION_ON_RECURSION_LIMIT,

        /**
         * This property should be set on each document node at which a recursive
         * mapping class is anchored.  The value of this property should be the
         * String name of the ancestor mapping class which is rooted at the 
         * recursive root node (i.e. the root of the recursive fragment of the
         * document).
         * <p>Type: <code>java.lang.String</code></p>
         */
        RECURSION_ROOT_MAPPING_CLASS,
        
        /**
         * Indicates if the node is the root of a recursive XML fragment or not.
         * <p>Type: <code>java.lang.Boolean</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_IS_RECURSIVE_ROOT}</p>
         */
        IS_RECURSIVE_ROOT,
        
        // ==================================================================================
        // DOCUMENT PROPERTIES (read from root node only, applicable to document as a whole)
        // ==================================================================================
        
        /**
         * <p>The encoding format of the document.  This property only needs to be
         * set at the root MappingNode of the document.</p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_DOCUMENT_ENCODING}</p>
         */
        DOCUMENT_ENCODING,
        
        /**
         * <p>Indicates whether the document will be outputted as a compressed
         * String, or in readable form, with line breaks or indenting.  This 
         * property only needs to be set at the root MappingNode
         * of the document.</p>
         * <p>Type: <code>java.lang.Boolean</code></p>
         * <p>Default: {@link MappingNodeConstants.Defaults#DEFAULT_FORMATTED_DOCUMENT}</p>
         */
        FORMATTED_DOCUMENT,
        
        /**
         * A property to mark to implicity include a node which needs to be 
         * added to result document. The implicit nodes are such nodes which
         * define the encoding information and type defination information.
         */
        ALWAYS_INCLUDE, 
        
        /**
         * In the case of the recursive mapping element nodes, the source nodes
         * may be different, in that case the original result set name
         * (i.e. mapping class in recurive node) is alias to the source node which is
         * above the recursive node.
         */
        ALIAS_RESULT_SET_NAME,
     
        /**
         * Result Set Info object which contains the query and the plan for
         * the source node.
         */
        RESULT_SET_INFO,        
    }

    // =========================================================================
    // XML DOCUMENT TAGS
    // =========================================================================

    /** 
     * These are the String values to be used for XML document tags.  
     * They map to one of the property keys defined in {@link Properties},
     * With the exceptions of the namespace ones:
     * {@link #NAMESPACE_DECLARATION} and child tags.
     * @see getPropertyString
     * @see getPropertyInteger
     */
    public static final class Tags {
        private Tags() { }
        
        public static final String MAPPING_ROOT_NAME      = "xmlMapping"; //$NON-NLS-1$
        public static final String MAPPING_NODE_NAME      = "mappingNode"; //$NON-NLS-1$

        public static final String NAME = "name"; //$NON-NLS-1$
        public static final String NAMESPACE_PREFIX = "namespace"; //$NON-NLS-1$
        /** 
         * The wrapper tag around a single namespace declaration
         */
        public static final String NAMESPACE_DECLARATION = "namespaceDeclaration"; //$NON-NLS-1$
        /** 
         * The attribute, child of the namespace declaration element, 
         * which specifies the prefix of a namespace declaration
         */
        public static final String NAMESPACE_DECLARATION_PREFIX = "prefix"; //$NON-NLS-1$
        /** 
         * The attribute, child of the namespace declaration element, 
         * which specifies the uri of a namespace declaration
         */
        public static final String NAMESPACE_DECLARATION_URI = "uri"; //$NON-NLS-1$
        public static final String NODE_TYPE = "nodeType"; //$NON-NLS-1$
        public static final String CARDINALITY_MIN_BOUND = "minOccurs"; //$NON-NLS-1$
        public static final String CARDINALITY_MAX_BOUND = "maxOccurs"; //$NON-NLS-1$
        public static final String CRITERIA = "criteria"; //$NON-NLS-1$
        public static final String DEFAULT_VALUE = "default"; //$NON-NLS-1$
        public static final String FIXED_VALUE = "fixed"; //$NON-NLS-1$
        public static final String RESULT_SET_NAME = "source"; //$NON-NLS-1$
        //public static final String ALIAS_RESULT_SET_NAME="aliasSource";//$NON-NLS-1$
        /** 
         * specifies a single temp group name - this tag has
         * multiplicity of zero or more
         */
        public static final String TEMP_GROUP_NAME = "tempGroup"; //$NON-NLS-1$
        public static final String ELEMENT_NAME = "symbol";         //$NON-NLS-1$
        public static final String COMMENT_TEXT = "comment"; //$NON-NLS-1$
        public static final String IS_OPTIONAL = "optional"; //$NON-NLS-1$
        public static final String IS_NILLABLE = "isNillable"; //$NON-NLS-1$
        public static final String IS_EXCLUDED = "isExcluded"; //$NON-NLS-1$
        public static final String IS_DEFAULT_CHOICE = "isDefaultChoice"; //$NON-NLS-1$
        public static final String EXCEPTION_ON_DEFAULT = "exceptionOnDefault"; //$NON-NLS-1$
        public static final String DOCUMENT_ENCODING = "documentEncoding"; //$NON-NLS-1$
        public static final String NORMALIZE_TEXT = "textNormalization"; //$NON-NLS-1$
        public static final String BUILT_IN_TYPE = "builtInType"; //$NON-NLS-1$
        public static final String FORMATTED_DOCUMENT = "formattedDocument"; //$NON-NLS-1$
        public static final String IS_RECURSIVE = "isRecursive"; //$NON-NLS-1$
        public static final String RECURSION_CRITERIA = "recursionCriteria"; //$NON-NLS-1$
        public static final String RECURSION_LIMIT = "recursionLimit"; //$NON-NLS-1$
        public static final String RECURSION_LIMIT_EXCEPTION = "recursionLimitException"; //$NON-NLS-1$
        public static final String RECURSION_ROOT_MAPPING_CLASS = "recursionRootMappingClass"; //$NON-NLS-1$
        public static final String ALWAYS_INCLUDE = "includeAlways"; //$NON-NLS-1$
        /**
         * The List of the tags, defined in this Class, which are
         * child elements of a {@link #MAPPING_NODE_NAME mapping node}
         * tag.  They are in a fixed order, so that the 
         * {@link MappingOutputter} can output documents in a consistent
         * manner.  Some tags are left out that the MappingOutputter 
         * handles separately.
         */
        static final List<String> OUTPUTTER_PROPERTY_TAGS;
        
        // Initialize static variables...
        static {
            List<String> temp = Arrays.asList( new String[]{
                MappingNodeConstants.Tags.NAME, 
                MappingNodeConstants.Tags.NODE_TYPE, 
                MappingNodeConstants.Tags.NAMESPACE_PREFIX, 
                MappingNodeConstants.Tags.DOCUMENT_ENCODING,
                MappingNodeConstants.Tags.FORMATTED_DOCUMENT,
                MappingNodeConstants.Tags.CRITERIA, 
                MappingNodeConstants.Tags.DEFAULT_VALUE, 
                MappingNodeConstants.Tags.FIXED_VALUE, 
                MappingNodeConstants.Tags.CARDINALITY_MIN_BOUND, 
                MappingNodeConstants.Tags.CARDINALITY_MAX_BOUND, 
                MappingNodeConstants.Tags.RESULT_SET_NAME,                
                MappingNodeConstants.Tags.TEMP_GROUP_NAME, 
                MappingNodeConstants.Tags.ELEMENT_NAME,
                MappingNodeConstants.Tags.COMMENT_TEXT,
                MappingNodeConstants.Tags.IS_OPTIONAL,
                MappingNodeConstants.Tags.IS_NILLABLE,
                MappingNodeConstants.Tags.IS_EXCLUDED,
                MappingNodeConstants.Tags.IS_DEFAULT_CHOICE,
                MappingNodeConstants.Tags.EXCEPTION_ON_DEFAULT,
                MappingNodeConstants.Tags.IS_RECURSIVE,
                MappingNodeConstants.Tags.RECURSION_CRITERIA,
                MappingNodeConstants.Tags.RECURSION_LIMIT,
                MappingNodeConstants.Tags.RECURSION_LIMIT_EXCEPTION,
                MappingNodeConstants.Tags.RECURSION_ROOT_MAPPING_CLASS,
                MappingNodeConstants.Tags.NORMALIZE_TEXT,                
                MappingNodeConstants.Tags.ALWAYS_INCLUDE,
                MappingNodeConstants.Tags.BUILT_IN_TYPE
            } );
            OUTPUTTER_PROPERTY_TAGS = Collections.unmodifiableList(temp);
        }
    }


    /** 
     * Convert a property String into one of the Integer property keys
     * defined in {@link Properties}
     * @param property String representation of property
     * @return one of the properties defined in the
     * {@link Properties} inner class
     * @throw IllegalArgumentException if parameter isn't for one of the properties 
     * defined in {@link Properties}
     * @see getPropertyString
     */
    public static final MappingNodeConstants.Properties getProperty(String property) {
        if(property.equals(Tags.NAME)) return Properties.NAME;
        else if (property.equals(Tags.NAMESPACE_PREFIX)) return Properties.NAMESPACE_PREFIX;
        else if (property.equals(Tags.NODE_TYPE)) return Properties.NODE_TYPE;
        else if (property.equals(Tags.CARDINALITY_MIN_BOUND)) return Properties.CARDINALITY_MIN_BOUND;
        else if (property.equals(Tags.CARDINALITY_MAX_BOUND)) return Properties.CARDINALITY_MAX_BOUND;
        else if (property.equals(Tags.CRITERIA)) return Properties.CRITERIA;
        else if (property.equals(Tags.DEFAULT_VALUE)) return Properties.DEFAULT_VALUE;
        else if (property.equals(Tags.FIXED_VALUE)) return Properties.FIXED_VALUE;
        else if (property.equals(Tags.RESULT_SET_NAME)) return Properties.RESULT_SET_NAME;
        //else if (property.equals(Tags.ALIAS_RESULT_SET_NAME)) return Properties.ALIAS_RESULT_SET_NAME;
        else if (property.equals(Tags.TEMP_GROUP_NAME)) return Properties.TEMP_GROUP_NAMES;
        else if (property.equals(Tags.ELEMENT_NAME)) return Properties.ELEMENT_NAME;
        else if (property.equals(Tags.COMMENT_TEXT)) return Properties.COMMENT_TEXT;
        else if (property.equals(Tags.IS_OPTIONAL)) return Properties.IS_OPTIONAL;
        else if (property.equals(Tags.IS_NILLABLE)) return Properties.IS_NILLABLE;
        else if (property.equals(Tags.IS_EXCLUDED)) return Properties.IS_EXCLUDED;
        else if (property.equals(Tags.IS_DEFAULT_CHOICE)) return Properties.IS_DEFAULT_CHOICE;
        else if (property.equals(Tags.EXCEPTION_ON_DEFAULT)) return Properties.EXCEPTION_ON_DEFAULT;
        else if (property.equals(Tags.DOCUMENT_ENCODING)) return Properties.DOCUMENT_ENCODING;
        else if (property.equals(Tags.FORMATTED_DOCUMENT)) return Properties.FORMATTED_DOCUMENT;
        else if (property.equals(Tags.IS_RECURSIVE)) return Properties.IS_RECURSIVE;
        else if (property.equals(Tags.RECURSION_CRITERIA)) return Properties.RECURSION_CRITERIA;
        else if (property.equals(Tags.RECURSION_LIMIT)) return Properties.RECURSION_LIMIT;
        else if (property.equals(Tags.RECURSION_LIMIT_EXCEPTION)) return Properties.EXCEPTION_ON_RECURSION_LIMIT;
        else if (property.equals(Tags.RECURSION_ROOT_MAPPING_CLASS)) return Properties.RECURSION_ROOT_MAPPING_CLASS;
        else if (property.equals(Tags.NORMALIZE_TEXT)) return Properties.NORMALIZE_TEXT;
        else if (property.equals(Tags.BUILT_IN_TYPE)) return Properties.BUILT_IN_TYPE;
        else if (property.equals(Tags.ALWAYS_INCLUDE)) return Properties.ALWAYS_INCLUDE;
        else {
            throw new IllegalArgumentException("Unknown property ("+property+")"); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    /** 
     * Convert a property Integer into a String (suitable for an XML tag). 
     * @param property one of the properties defined in the
     * {@link Properties} inner class
     * @return String representation of property
     * @throw IllegalArgumentException if parameter isn't one of the properties 
     * defined in {@link Properties}
     * @see getPropertyInteger
     */
    public static final String getPropertyString(Integer property){
        if(property.equals(Properties.NAME)) return Tags.NAME;
        else if (property.equals(Properties.NAMESPACE_PREFIX)) return Tags.NAMESPACE_PREFIX;
        else if (property.equals(Properties.NODE_TYPE)) return Tags.NODE_TYPE;
        else if (property.equals(Properties.CARDINALITY_MIN_BOUND)) return Tags.CARDINALITY_MIN_BOUND;
        else if (property.equals(Properties.CARDINALITY_MAX_BOUND)) return Tags.CARDINALITY_MAX_BOUND;
        else if (property.equals(Properties.CRITERIA)) return Tags.CRITERIA;
        else if (property.equals(Properties.DEFAULT_VALUE)) return Tags.DEFAULT_VALUE;
        else if (property.equals(Properties.FIXED_VALUE)) return Tags.FIXED_VALUE;
        else if (property.equals(Properties.RESULT_SET_NAME)) return Tags.RESULT_SET_NAME;
        else if (property.equals(Properties.TEMP_GROUP_NAMES)) return Tags.TEMP_GROUP_NAME;
        else if (property.equals(Properties.ELEMENT_NAME)) return Tags.ELEMENT_NAME;
        else if (property.equals(Properties.COMMENT_TEXT)) return Tags.COMMENT_TEXT;
        else if (property.equals(Properties.IS_OPTIONAL)) return Tags.IS_OPTIONAL;
        else if (property.equals(Properties.IS_NILLABLE)) return Tags.IS_NILLABLE;
        else if (property.equals(Properties.IS_EXCLUDED)) return Tags.IS_EXCLUDED;
        else if (property.equals(Properties.IS_DEFAULT_CHOICE)) return Tags.IS_DEFAULT_CHOICE;
        else if (property.equals(Properties.EXCEPTION_ON_DEFAULT)) return Tags.EXCEPTION_ON_DEFAULT;
        else if (property.equals(Properties.DOCUMENT_ENCODING)) return Tags.DOCUMENT_ENCODING;
        else if (property.equals(Properties.FORMATTED_DOCUMENT)) return Tags.FORMATTED_DOCUMENT;
        else if (property.equals(Properties.IS_RECURSIVE)) return Tags.IS_RECURSIVE;
        else if (property.equals(Properties.RECURSION_CRITERIA)) return Tags.RECURSION_CRITERIA;
        else if (property.equals(Properties.RECURSION_LIMIT)) return Tags.RECURSION_LIMIT;
        else if (property.equals(Properties.EXCEPTION_ON_RECURSION_LIMIT)) return Tags.RECURSION_LIMIT_EXCEPTION;
        else if (property.equals(Properties.RECURSION_ROOT_MAPPING_CLASS)) return Tags.RECURSION_ROOT_MAPPING_CLASS;
        else if (property.equals(Properties.NORMALIZE_TEXT)) return Tags.NORMALIZE_TEXT;
        else if (property.equals(Properties.BUILT_IN_TYPE)) return Tags.BUILT_IN_TYPE;
        else if (property.equals(Properties.ALWAYS_INCLUDE)) return Tags.ALWAYS_INCLUDE;
        else {
            throw new IllegalArgumentException("Unknown property ("+property+")"); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    // =========================================================================
    // DEFAULTS
    // =========================================================================
    
    /** 
     * Default property values for node {@link MappingNodeConstants.Properties properties}
     */
    public static final class Defaults {
        private Defaults() { }
       
        /**
         * Default property values, keyed off the properties defined in 
         * {@link MappingNodeConstants.Properties}.  The {@link MappingNode} class will return
         * these values if none are defined, for each property.
         */
        public static final Map<Properties, Object> DEFAULT_VALUES;
        
        /** The default minimum bound of the cardinality of a node. */
        public static final Integer DEFAULT_CARDINALITY_MINIMUM_BOUND = new Integer(1);
        
        /** The default maximum bound of the cardinality of a node. */
        public static final Integer DEFAULT_CARDINALITY_MAXIMUM_BOUND = new Integer(1);

        /** The default output type of the node, if no value is specified */
        public static final String DEFAULT_NODE_TYPE = ELEMENT;

        /** The default value for {@link MappingNodeConstants.Properties#IS_NILLABLE} */
        public static final Boolean DEFAULT_IS_NILLABLE = Boolean.FALSE;

        /** The default value for {@link MappingNodeConstants.Properties#TEMP_GROUP_NAMES} */
        public static final Collection DEFAULT_TEMP_GROUP_NAMES = Collections.EMPTY_LIST;

        /** The default value for {@link MappingNodeConstants.Properties#IS_OPTIONAL} */
        public static final Boolean DEFAULT_IS_OPTIONAL = Boolean.FALSE;

        /** The default value for {@link MappingNodeConstants.Properties#NORMALIZE_TEXT} */
        public static final String DEFAULT_NORMALIZE_TEXT = NORMALIZE_TEXT_PRESERVE;

        /** The default value for {@link MappingNodeConstants.Properties#BUILT_IN_TYPE} */
        public static final String DEFAULT_BUILT_IN_TYPE = NO_TYPE;

        // ========================================================================
        // CHOICE RELATED DEFAULTS
        // ========================================================================

        /** The default value for {@link MappingNodeConstants.Properties#IS_NILLABLE} */
        public static final Boolean DEFAULT_IS_EXCLUDED = Boolean.FALSE;

		/** The default value for {@link MappingNodeConstants.Properties#IS_INCLUDED} */
		public static final Boolean DEFAULT_IS_INCLUDED = Boolean.FALSE;

        /** The default value for {@link MappingNodeConstants.Properties#IS_DEFAULT_CHOICE} */
        public static final Boolean DEFAULT_IS_DEFAULT_CHOICE = Boolean.FALSE;
        
        /** The default value for {@link MappingNodeConstants.Properties#EXCEPTION_ON_DEFAULT} */
        public static final Boolean DEFAULT_EXCEPTION_ON_DEFAULT = Boolean.FALSE;

        // ========================================================================
        // RECURSI0N RELATED DEFAULTS
        // ========================================================================

        /** The default value for {@link MappingNodeConstants.Properties#IS_RECURSIVE} */
        public static final Boolean DEFAULT_IS_RECURSIVE = Boolean.FALSE;

        /** The default value for {@link MappingNodeConstants.Properties#IS_RECURSIVE_ROOT} */
        public static final Boolean DEFAULT_IS_RECURSIVE_ROOT = Boolean.FALSE;
        
        /** The default value for {@link MappingNodeConstants.Properties#RECURSION_LIMIT} */
        public static final Integer DEFAULT_RECURSION_LIMIT = new Integer(10);

        /** The default value for {@link MappingNodeConstants.Properties#EXCEPTION_ON_RECURSION_LIMIT} */
        public static final Boolean DEFAULT_EXCEPTION_ON_RECURSION_LIMIT = Boolean.FALSE;

        // ========================================================================
        // DOCUMENT RELATED DEFAULTS
        // ========================================================================

        /** The default value for {@link MappingNodeConstants.Properties#DOCUMENT_ENCODING} */
        public static final String DEFAULT_DOCUMENT_ENCODING = "UTF-8"; //$NON-NLS-1$

        /** The default value for {@link MappingNodeConstants.Properties#FORMATTED_DOCUMENT} */
        public static final Boolean DEFAULT_FORMATTED_DOCUMENT = Boolean.FALSE;

        static{
            HashMap<Properties, Object> temp = new HashMap<Properties, Object>();
            temp.put(Properties.CARDINALITY_MIN_BOUND, DEFAULT_CARDINALITY_MINIMUM_BOUND);
            temp.put(Properties.CARDINALITY_MAX_BOUND, DEFAULT_CARDINALITY_MAXIMUM_BOUND);
            temp.put(Properties.NODE_TYPE, DEFAULT_NODE_TYPE);
            temp.put(Properties.IS_NILLABLE, DEFAULT_IS_NILLABLE);
            temp.put(Properties.TEMP_GROUP_NAMES, DEFAULT_TEMP_GROUP_NAMES);
            temp.put(Properties.IS_OPTIONAL, DEFAULT_IS_OPTIONAL);
            temp.put(Properties.IS_EXCLUDED, DEFAULT_IS_EXCLUDED);
            temp.put(Properties.IS_INCLUDED, DEFAULT_IS_INCLUDED);
            temp.put(Properties.IS_DEFAULT_CHOICE, DEFAULT_IS_DEFAULT_CHOICE);
            temp.put(Properties.EXCEPTION_ON_DEFAULT, DEFAULT_EXCEPTION_ON_DEFAULT);
            temp.put(Properties.IS_RECURSIVE, DEFAULT_IS_RECURSIVE);
            temp.put(Properties.IS_RECURSIVE_ROOT, DEFAULT_IS_RECURSIVE_ROOT);
            temp.put(Properties.RECURSION_LIMIT, DEFAULT_RECURSION_LIMIT);
            temp.put(Properties.EXCEPTION_ON_RECURSION_LIMIT, DEFAULT_EXCEPTION_ON_RECURSION_LIMIT);
            temp.put(Properties.DOCUMENT_ENCODING, DEFAULT_DOCUMENT_ENCODING);
            temp.put(Properties.FORMATTED_DOCUMENT, DEFAULT_FORMATTED_DOCUMENT);
            temp.put(Properties.NORMALIZE_TEXT, DEFAULT_NORMALIZE_TEXT);            
            DEFAULT_VALUES = Collections.unmodifiableMap(temp);
        }
    }
}
