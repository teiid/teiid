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

package com.metamatrix.metadata.runtime.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class MetaModelConstants {
    
    public static final String DELIMITER = ".";
    public static final String PRODUCT_VERSION = "3.0";
    public static final String NAMESPACE_URI_PREFIX = "http://www.metamatrix.com/metabase/"+PRODUCT_VERSION+"/metamodels/";
    public static final String METAMODEL_FILE_EXTENSION = ".xml";

    /** Property names common to multiple metamodel components */
    public static final String UUID_ATTRIBUTE             = "uuid";
    public static final String NAME_ATTRIBUTE             = "name";
    public static final String NAMESPACE_ATTRIBUTE        = "location";
    public static final String DESCRIPTION_ATTRIBUTE      = "description";
    public static final String KEYWORDS_ATTRIBUTE         = "keywords";
    public static final String ITEM_TYPE_ATTRIBUTE        = "itemType";
    public static final String MEMBER_TYPE_ATTRIBUTE      = "memberTypes";
    public static final String RUNTIME_TYPE_ATTRIBUTE     = "runtimeDataType";
    public static final String BASE_TYPE_ATTRIBUTE        = "baseType";
    public static final String TYPE_ATTRIBUTE             = "type";
    public static final String REFERENCE_TYPE_ATTRIBUTE   = "reference";
    public static final String TARGET_NAMESPACE_ATTRIBUTE = "targetNamespace";
    public static final String COMPONENT_ID_ATTRIBUTE     = "id";
    public static final String ALIAS_ATTRIBUTE            = "alias";
    public static final String RECURSIVE_ATTRIBUTE        = "recursive";
    public static final String RECURSION_CRITERIA_ATTRIBUTE       = "recursionCriteria";
    public static final String RECURSION_LIMIT_ATTRIBUTE          = "recursionLimit";
    public static final String RECURSION_LIMIT_ERROR_ATTRIBUTE    = "recursionLimitError";

    /** Property names associated with DataTypes metamodel components */
    public static final String IN_DIRECTION_KIND     = "In";       
    public static final String OUT_DIRECTION_KIND    = "Out";       
    public static final String INOUT_DIRECTION_KIND  = "InOut";       
    public static final String RETURN_DIRECTION_KIND = "Return";       
    
    /** Property names associated with XML Attribute kind metamodel components */
    public static final String PROHIBITED_ATTRIBUTE_KIND   = "prohibited";       
    public static final String OPTIONAL_ATTRIBUTE_KIND     = "optional";       
    public static final String REQUIRED_ATTRIBUTE_KIND     = "required";       
    public static final String DEFAULT_ATTRIBUTE_KIND      = OPTIONAL_ATTRIBUTE_KIND;   
    
    /** Property names associated with Foundation metamodel components */
    public static final String FOUNDATION_CHANGEABILITY_ATTRIBUTE         = "changeability";
    public static final String FOUNDATION_MULTIPLICITY_ATTRIBUTE          = "multiplicity";
    public static final String FOUNDATION_DIRECTION_ATTRIBUTE             = "direction";
    public static final String FOUNDATION_INITIAL_VALUE_ATTRIBUTE         = "initialValue";
    public static final String FOUNDATION_DEFAULT_VALUE_ATTRIBUTE         = "defaultValue";
    public static final String FOUNDATION_SUPPORTS_AND_ATTRIBUTE          = "supportsAnd";
    public static final String FOUNDATION_SUPPORTS_OR_ATTRIBUTE           = "supportsOr";
    public static final String FOUNDATION_SUPPORTS_SET_ATTRIBUTE          = "supportsSet";
    public static final String FOUNDATION_SUPPORTS_WHEREALL_ATTRIBUTE     = "supportsWhereAll";
    public static final String FOUNDATION_SUPPORTS_ORDERBY_ATTRIBUTE      = "supportsOrderBy";
    public static final String FOUNDATION_SUPPORTS_DISTINCT_ATTRIBUTE     = "supportsDistinct";
    public static final String FOUNDATION_SUPPORTS_JOIN_ATTRIBUTE         = "supportsJoin";
    public static final String FOUNDATION_SUPPORTS_OUTERJOIN_ATTRIBUTE    = "supportsOuterJoin";
    public static final String FOUNDATION_SUPPORTS_TRANSACTION_ATTRIBUTE  = "supportsTransaction";
    public static final String FOUNDATION_SUPPORTS_SUBSCRIPTION_ATTRIBUTE = "supportsSubscription";
    public static final String FOUNDATION_SUPPORTS_UPDATE_ATTRIBUTE       = "supportsUpdate";
    public static final String FOUNDATION_SUPPORTS_SELECT_ATTRIBUTE       = "supportsSelect";
    public static final String FOUNDATION_MAX_SETSIZE_ATTRIBUTE           = "maxSetSize";
    public static final String FOUNDATION_SCALE_ATTRIBUTE                 = "scale";
    public static final String FOUNDATION_LENGTH_ATTRIBUTE                = "length";
    public static final String FOUNDATION_LENGTH_FIXED_ATTRIBUTE          = "isLengthFixed";
    public static final String FOUNDATION_IS_NULLABLE_ATTRIBUTE           = "isNullable";
    public static final String FOUNDATION_IS_CASE_SENSITIVE_ATTRIBUTE     = "isCaseSensitive";
    public static final String FOUNDATION_IS_SIGNED_ATTRIBUTE             = "isSigned";
    public static final String FOUNDATION_IS_CURRENCY_ATTRIBUTE           = "isCurrency";
    public static final String FOUNDATION_IS_AUTO_INCREMENTED_ATTRIBUTE   = "isAutoIncremented";
    public static final String FOUNDATION_MIN_RANGE_ATTRIBUTE             = "minRange";
    public static final String FOUNDATION_MAX_RANGE_ATTRIBUTE             = "maxRange";
    public static final String FOUNDATION_FORMAT_ATTRIBUTE                = "format";
    public static final String FOUNDATION_SEARCH_TYPE_ATTRIBUTE           = "searchType";
    public static final String FOUNDATION_CARDINALITY_ATTRIBUTE           = "cardinality";
    

    public static final String FEATURE_ASSOCIATION_END                    = "feature";
    public static final String UNIQUE_KEY_ASSOCIATION_END                 = "uniqueKey";
    public static final String KEY_RELATIONSHIP_ASSOCIATION_END           = "keyRelationship";
   // The "ClassifierFeature" association between Classifier and Feature
    public static final String CLASSIFIER_FEATURE_FEATURE_ASSOCIATION_END = FEATURE_ASSOCIATION_END;
    public static final String CLASSIFIER_FEATURE_OWNER_ASSOCIATION_END   = "owner";
    // The "KeyRelationshipFeatures" association between KeyRelationship and StructuralFeature
    public static final String KEY_RELATIONSHIP_FEATURE_ASSOCIATION_END = FEATURE_ASSOCIATION_END;
    public static final String KEY_RELATIONSHIP_KEYREL_ASSOCIATION_END  = KEY_RELATIONSHIP_ASSOCIATION_END;
    // The "UniqueKeyRelationship" association between KeyRelationship and UniqueKey
    public static final String UNIQUE_KEY_RELATIONSHIP_KEY_ASSOCIATION_END    = UNIQUE_KEY_ASSOCIATION_END;
    public static final String UNIQUE_KEY_RELATIONSHIP_KEYREL_ASSOCIATION_END = KEY_RELATIONSHIP_ASSOCIATION_END;
    // The "UniqueFeature" association between StructuralFeature and UniqueKey
    public static final String UNIQUE_FEATURE_UNIQUEKEY_ASSOCIATION_END = UNIQUE_KEY_ASSOCIATION_END;
    public static final String UNIQUE_FEATURE_FEATURE_ASSOCIATION_END   = FEATURE_ASSOCIATION_END;
    // The "IndexedFeatureInfo" association between Index and IndexFeature
    public static final String INDEXED_FEATURE_INFO_INDEX_ASSOCIATION_END        = "index";
    public static final String INDEXED_FEATURE_INFO_INDEXFEATURE_ASSOCIATION_END = "indexedFeature";
    // The "IndexSpansClass" association between Index and Class
    public static final String INDEX_SPANS_CLASS_INDEX_ASSOCIATION_END         = "index";
    public static final String INDEX_SPANS_CLASS_CLASS_ASSOCIATION_END         = "spannedClass";
    // The "IndexedFeatures" association between IndexFeature and StructuralFeature
    public static final String INDEXED_FEATURE_FEATURE_ASSOCIATION_END = FEATURE_ASSOCIATION_END;
    public static final String INDEXED_FEATURE_INDEX_ASSOCIATION_END   = "indexedFeature";
    // The "ParameterOwnership" association between BehavioralFeature and Parameter
    public static final String PARAMETER_BEHAVIORALFEATURE_PARAMETER_ASSOCIATION_END = "parameter";
    public static final String PARAMETER_BEHAVIORALFEATURE_FEATURE_ASSOCIATION_END   = "behavioralFeature";
    
    /** Property names associated with Diagram metamodel components */
    public static final String DIAGRAM_USERSTRING_ATTRIBUTE      = "userstring";
    public static final String DIAGRAM_USERTYPE_ATTRIBUTE        = "usertype";
    public static final String DIAGRAM_X_ATTRIBUTE               = "x";
    public static final String DIAGRAM_Y_ATTRIBUTE               = "y";
    public static final String DIAGRAM_H_ATTRIBUTE               = "h";
    public static final String DIAGRAM_W_ATTRIBUTE               = "w";
    public static final String DIAGRAM_OBJECTID_ATTRIBUTE        = "objectID";
    public static final String DIAGRAM_MODEL_OBJECTID_ATTRIBUTE  = "modelObjectID";
    // The "DiagramContents" association between Diagram and DiagramComponent
    public static final String DIAGRAM_COMPONENT_ASSOCIATION_END = "component";
    public static final String DIAGRAM_DIAGRAM_ASSOCIATION_END   = "diagram";
    
    /** Property names associated with Virtual metamodel components */
    public static final String VIRTUAL_QUERY_TREE_ATTRIBUTE        = "queryTree";
    public static final String VIRTUAL_UPDATE_QUERY_TREE_ATTRIBUTE = "updateQueryTree";
    public static final String VIRTUAL_INSERT_QUERY_TREE_ATTRIBUTE = "insertQueryTree";
    public static final String VIRTUAL_DELETE_QUERY_TREE_ATTRIBUTE = "deleteQueryTree";

    public static final String VIRTUAL_SQL_ATTRIBUTE            = "sql";
    public static final String VIRTUAL_ALIAS_ATTRIBUTE          = ALIAS_ATTRIBUTE;
    public static final String VIRTUAL_LABEL_ATTRIBUTE          = "label";
    public static final String VIRTUAL_OBJECTID_ATTRIBUTE       = "objectID";
    public static final String VIRTUAL_MODEL_OBJECTID_ATTRIBUTE = "modelObjectID";
    public static final String VIRTUAL_OBJECT_PATH_ATTRIBUTE    = "objectPath";
    public static final String VIRTUAL_IS_INPUT_ATTRIBUTE       = "isInput";
    
    public static final String VIRTUAL_UPDATE_SQL_ATTRIBUTE     = "updateSQLStatement";
    public static final String VIRTUAL_INSERT_SQL_ATTRIBUTE     = "insertSQLStatement";
    public static final String VIRTUAL_DELETE_SQL_ATTRIBUTE     = "deleteSQLStatement";
    public static final String VIRTUAL_ALLOWS_UPDATE_ATTRIBUTE  = "allowsUpdate";
    public static final String VIRTUAL_ALLOWS_INSERT_ATTRIBUTE  = "allowsInsert";
    public static final String VIRTUAL_ALLOWS_DELETE_ATTRIBUTE  = "allowsDelete";

    public static final String VIRTUAL_SELECT_STRING_ATTRIBUTE  = "selectSQLString";
    public static final String VIRTUAL_UPDATE_STRING_ATTRIBUTE  = "updateSQLString";
    public static final String VIRTUAL_INSERT_STRING_ATTRIBUTE  = "insertSQLString";
    public static final String VIRTUAL_DELETE_STRING_ATTRIBUTE  = "deleteSQLString";

    public static final String TRANSFORM_ASSOCIATION_END        = "transform";
    // The "TransformationContents" association between Transformation and TransformationOperation
    public static final String TRANSFORMATION_CONTENTS_OPERATION_ASSOCIATION_END = "operations";
    public static final String TRANSFORMATION_CONTENTS_TRANSFORM_ASSOCIATION_END = TRANSFORM_ASSOCIATION_END;
    // The "TransformationClass" association between Transformation and TemporaryGroup
    public static final String TRANSFORMATION_CLASS_CLASS_ASSOCIATION_END     = "temporaryGroups";
    public static final String TRANSFORMATION_CLASS_TRANSFORM_ASSOCIATION_END = TRANSFORM_ASSOCIATION_END;
    // the "QueryOperationLinks" association between QueryOperation and TransformationLink
    public static final String QUERY_OPERATION_LINK_ASSOCIATION_END   = "links";
    public static final String QUERY_OPERATION_QUERY_ASSOCIATION_END  = TRANSFORM_ASSOCIATION_END;
    
    /** Property names associated with MetaMatrix Functions metamodel components */
    public static final String FUNCTION_CATEGORY_ATTRIBUTE          = "category";
    public static final String FUNCTION_INVOCATION_CLASS_ATTRIBUTE  = "invocationClass";
    public static final String FUNCTION_INVOCATION_METHOD_ATTRIBUTE = "invocationMethod";
    public static final String FUNCTION_DIRECTION_ATTRIBUTE         = "direction";
    // the "ParameterOwnership" association between function and Parameter
    public static final String PARAMETER_OWNERSHIP_PARAMETER_ASSOCIATION_END = "parameter";
    public static final String PARAMETER_OWNERSHIP_FUNCTION_ASSOCIATION_END  = "function";
    
    /** Property names associated with SimpleDataTypes metamodel components */
    public static final String SDT_VARIETY_ATTRIBUTE               = "variety";
    public static final String SDT_FINAL_ATTRIBUTE                 = "final";
    public static final String SDT_ORDERED_FACET_ATTRIBUTE         = "ordered";
    public static final String SDT_BOUNDED_FACET_ATTRIBUTE         = "bounded";
    public static final String SDT_CARDINALITY_FACET_ATTRIBUTE     = "cardinality";
    public static final String SDT_NUMERIC_FACET_ATTRIBUTE         = "numeric";
    public static final String SDT_PATTERN_FACET_ATTRIBUTE         = "pattern";
    public static final String SDT_ENUMERATION_FACET_ATTRIBUTE     = "enumeration";
    public static final String SDT_LENGTH_FACET_ATTRIBUTE          = "length";
    public static final String SDT_MIN_LENGTH_FACET_ATTRIBUTE      = "minLength";
    public static final String SDT_MAX_LENGTH_FACET_ATTRIBUTE      = "maxLength";
    public static final String SDT_WHITE_SPACE_FACET_ATTRIBUTE     = "whitespace";
    public static final String SDT_MIN_INCLUSIVE_FACET_ATTRIBUTE   = "minInclusive";
    public static final String SDT_MIN_EXCLUSIVE_FACET_ATTRIBUTE   = "minExclusive";
    public static final String SDT_MAX_INCLUSIVE_FACET_ATTRIBUTE   = "maxInclusive";
    public static final String SDT_MAX_EXCLUSIVE_FACET_ATTRIBUTE   = "maxExclusive";
    public static final String SDT_TOTAL_DIGITS_FACET_ATTRIBUTE    = "totalDigits";
    public static final String SDT_FRACTION_DIGITS_FACET_ATTRIBUTE = "fractionDigits";
    public static final String SDT_PATTERN_FACET_VALUE_ATTRIBUTE      = "value";
    public static final String SDT_ENUMERATION_FACET_VALUE_ATTRIBUTE  = "value";
    public static final String SDT_CONSTRAINT_FACET_DESCRIPTION_SUFFIX = "Description";
    public static final String SDT_CONSTRAINT_FACET_FIXED_SUFFIX       = "Fixed";
    
    /** Property names associated with XML Schema metamodel components */
    public static final String SCHEMA_FIXED_OR_DEFAULT_CONSTRAINT_ATTRIBUTE       = "constraint";
    public static final String SCHEMA_FIXED_OR_DEFAULT_CONSTRAINT_VALUE_ATTRIBUTE = "constraintValue";
    public static final String SCHEMA_FORM_KIND_ATTRIBUTE                         = "form";
    public static final String SCHEMA_ELEMENT_FORM_DEFAULT_ATTRIBUTE              = "elementFormDefault";
    public static final String SCHEMA_ATTRIBUTE_FORM_DEFAULT_ATTRIBUTE            = "attributeFormDefault";
    public static final String SCHEMA_NILLABLE_ATTRIBUTE                          = "nillable";
    public static final String SCHEMA_ABSTRACT_ATTRIBUTE                          = "abstract";
    public static final String SCHEMA_ANONYMOUS_ATTRIBUTE                         = "anonymous";
    public static final String SCHEMA_MAX_OCCURS_ATTRIBUTE                        = "maxOccurs";
    public static final String SCHEMA_MIN_OCCURS_ATTRIBUTE                        = "minOccurs";
    public static final String SCHEMA_PROCESS_CONTENTS_ATTRIBUTE                  = "processContents";
    public static final String SCHEMA_NAMESPACE_ATTRIBUTE                         = "namespace";
    public static final String SCHEMA_SCHEMA_LOCATION_ATTRIBUTE                   = "schemaLocation";
    public static final String SCHEMA_SOURCE_ATTRIBUTE                            = "source";
    public static final String SCHEMA_CONTENT_ATTRIBUTE                           = "content";
    public static final String SCHEMA_INCL_OR_EXCL_CONSTRAINT_ATTRIBUTE           = "constraint";
    public static final String SCHEMA_USE_KIND_ATTRIBUTE                          = "use";
    public static final String SCHEMA_MIXED_ATTRIBUTE                             = "mixed";
    public static final String SCHEMA_FINAL_ATTRIBUTE                             = "final";
    public static final String SCHEMA_DERIVATION_METHOD_ATTRIBUTE                 = "derivationMethod";
//    public static final String SCHEMA_ALLOWS_CHARACTER_DATA_ATTRIBUTE             = "allowsCharacterData";
//    public static final String SCHEMA_ALLOWS_ELEMENTS_ATTRIBUTE                   = "allowsElements";
//    public static final String SCHEMA_ALLOWS_ATTRIBUTES_ATTRIBUTE                 = "allowsAttributes";
    public static final String SCHEMA_SUBSTITUTION_GROUP_ATTRIBUTE                = "substitutionGroup";
    public static final String SCHEMA_PUBLIC_ATTRIBUTE                            = "public";
    public static final String SCHEMA_SYSTEM_ATTRIBUTE                            = "system";
    public static final String SCHEMA_BLOCK_ATTRIBUTE                             = "block";
    
    /** Property names associated with XML Document metamodel components */
    public static final String XML_SCHEMA_REF_ATTRIBUTE                   = "schemaReference";
    public static final String XML_SCHEMA_OBJ_REF_ATTRIBUTE               = "schemaObjectReference";
    public static final String XML_DOCUMENT_ENCODING_ATTRIBUTE            = "charEncoding";
    public static final String XML_DOCUMENT_REFERENCE_ATTRIBUTE           = "documentReference";
    public static final String XML_DOCUMENT_MAPPING_ATTRIBUTE             = "mapping";
    public static final String XML_DOCUMENT_CHOICE_SQL_CRITERIA_ATTRIBUTE = "criteria";
    public static final String XML_DOCUMENT_COMMENT_ATTRIBUTE             = "comment";
    public static final String MAPPING_DEFINITION_ATTRIBUTE               = "mappingDefinition";
    public static final String XML_DOCUMENT_FORMATTING_ATTRIBUTE          = "formatted";
    public static final String XML_DOCUMENT_FIXED_OR_DEFAULT_CONSTRAINT_ATTRIBUTE       = SCHEMA_FIXED_OR_DEFAULT_CONSTRAINT_ATTRIBUTE;
    public static final String XML_DOCUMENT_FIXED_OR_DEFAULT_CONSTRAINT_VALUE_ATTRIBUTE = SCHEMA_FIXED_OR_DEFAULT_CONSTRAINT_VALUE_ATTRIBUTE;
    public static final String XML_DOCUMENT_NAMESPACE_URI_ATTRIBUTE       = "namespaceURI";
    public static final String XML_DOCUMENT_NAMESPACE_PREFIX_ATTRIBUTE    = "namespacePrefix";

    public static final String XML_DOCUMENT_CHOICE_DEFAULT_OBJ_ATTRIBUTE     = "choiceDefaultObject";
    public static final String XML_DOCUMENT_CRITERIA_DEFAULT_ERROR_ATTRIBUTE = "criteriaDefaultError";
    public static final String XML_DOCUMENT_CRITERIA_ATTRIBUTE               = "criteria";
    public static final String XML_DOCUMENT_EXCLUDE_OBJ_ATTRIBUTE            = "excludeFromDocument";
    
    private static final Map METAMODEL_NAMES = new HashMap();
    /** Map of metamodel name keyed on metamodel package name */
    public static final Map METAMODEL_NAME_MAP = Collections.unmodifiableMap(METAMODEL_NAMES);
    
    // Build a map between the metamodel package name and the metamodel name.  The full name
    // of an metamodel entity is of the form packageName.className and the packageName is not
    // always the same as the metamodel name.
    static {
        METAMODEL_NAMES.put(MetaModelConstants.DataTypes.PACKAGE_NAME,      MetaModelConstants.DataTypes.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.Foundation.PACKAGE_NAME,     MetaModelConstants.Foundation.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.Relational.PACKAGE_NAME,     MetaModelConstants.Relational.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.DataAccess.PACKAGE_NAME,     MetaModelConstants.DataAccess.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.Diagram.PACKAGE_NAME,        MetaModelConstants.Diagram.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.Virtual.PACKAGE_NAME,        MetaModelConstants.Virtual.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.Function.PACKAGE_NAME,       MetaModelConstants.Function.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.SimpleDataTypes.PACKAGE_NAME,MetaModelConstants.SimpleDataTypes.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.XMLSchema.PACKAGE_NAME,      MetaModelConstants.XMLSchema.NAME);    
        METAMODEL_NAMES.put(MetaModelConstants.XMLDocument.PACKAGE_NAME,    MetaModelConstants.XMLDocument.NAME);    
    }

    public static class DataTypes {
        public static final String NAME = "MMDataTypes";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "MMDataTypes";
        public static final String PACKAGE_NAME = "DataTypes";
        public static interface Class {
        }
    }

    public static class Foundation {
        public static final String NAME = "Foundation";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "Foundation";
        public static final String PACKAGE_NAME = "Foundation";
        public static interface Class {
            public static final String ELEMENT                      = "Element";
            public static final String MODEL_ELEMENT                = "ModelElement";
            public static final String NAMESPACE                    = "Namespace";
            public static final String PACKAGE                      = "Package";
            public static final String MODEL                        = "Model";
            public static final String ACCESS_MODEL                 = "AccessModel";
            public static final String CLASSIFIER                   = "Classifier";
            public static final String DATA_TYPE                    = "DataType";
            public static final String CLASS                        = "Class";
            public static final String FEATURE                      = "Feature";
            public static final String STRUCTURAL_FEATURE           = "StructuralFeature";
            public static final String ATTRIBUTE                    = "Attribute";
            public static final String RELATIONSHIP                 = "Relationship";
            public static final String ASSOCIATION                  = "Association";
            public static final String ASSOCIATION_END              = "AssociationEnd";
            public static final String UNIQUE_KEY                   = "UniqueKey";
            public static final String PRIMARY_KEY                  = "PrimaryKey";
            public static final String KEY_RELATIONSHIP             = "KeyRelationship";
            public static final String INDEX                        = "Index";
            public static final String INDEXED_FEATURE              = "IndexedFeature";
            public static final String BEHAVIORIAL_FEATURE          = "BehavioralFeature";
            public static final String METHOD                       = "Method";
            public static final String PROCEDURE                    = "Procedure";
            public static final String STORED_QUERY                 = "StoredQuery";
            public static final String PARAMETER                    = "Parameter";
            public static final String STORED_QUERY_PARAMETER       = "StoredQueryParameter";
            public static final String OPERATION                    = "Operation";
        }
    }

    public static class Relational {
        public static final String NAME = "Relational";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "Relational";
        public static final String PACKAGE_NAME = "Relational";
        public static interface Class {
            public static final String MODEL                        = "Model";
            public static final String CATALOG                      = "Catalog";
            public static final String SCHEMA                       = "Schema";
            public static final String TABLE                        = "Table";
            public static final String COLUMN                       = "Column";
            public static final String AP_COLUMNS                   = "columns";
            public static final String PROCEDURE                    = "StoredProcedure";
            public static final String PARAMETER                    = "Parameter";
            public static final String STORED_QUERY_PARAMETER       = "StoredQueryParameter";
            public static final String STORED_QUERY                 = "StoredQuery";
            public static final String BASE_TABLE                   = "BaseTable";
            public static final String VIEW                         = "View";
            public static final String RESULT_SET                   = "ResultSet";
            public static final String SQL_INDEX                    = "SQLIndex";
            public static final String SQL_INDEX_COLUMN             = "SQLIndexColumn";
            public static final String UNIQUE_CONSTRAINT            = "UniqueConstraint";
            public static final String PRIMARY_KEY                  = "PrimaryKey";
            public static final String FOREIGN_KEY                  = "ForeignKey";
            public static final String ACCESS_PATTERN               = "AccessPattern";
        }
    }

    public static class DataAccess {
        public static final String NAME = "DataAccess";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "DataAccess";
        public static final String PACKAGE_NAME = "DataAccess";
        public static interface Class {
            public static final String MODEL                        = "Model";
            public static final String CATEGORY                     = "Category";
            public static final String GROUP                        = "Group";
            public static final String ELEMENT                      = "Element";
            public static final String AP_ELEMENTS                  = "elements";
            //public static final String PROCEDURE                    = "Procedure";
            public static final String STORED_QUERY                 = "StoredQuery";
            public static final String PARAMETER                    = "Parameter";
            public static final String STORED_QUERY_PARAMETER       = "StoredQueryParameter";
            public static final String INDEX                        = "Index";
            public static final String INDEX_COLUMN                 = "IndexColumn";
            public static final String UNIQUE_CONSTRAINT            = "UniqueConstraint";
            public static final String PRIMARY_KEY                  = "PrimaryKey";
            public static final String FOREIGN_KEY                  = "ForeignKey";
            public static final String ACCESS_PATTERN               = "AccessPattern";
            public static final String RESULT_SET                   = "ResultSet";
        }
    }

    public static class Diagram {
        public static final String NAME = "Diagram";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "Diagram";
        public static final String PACKAGE_NAME = "Diagram";
        public static interface Class {
            public static final String PRESENTATION_ELEMENT         = "PresentationElement";
            public static final String DIAGRAM                      = "Diagram";
            public static final String DIAGRAM_COMPONENT            = "DiagramComponent";
            public static final String CLASS_DIAGRAM                = "ClassDiagram";
            public static final String PACKAGE_DIAGRAM              = "PackageDiagram";
            public static final String TRANSFORMATION_DIAGRAM       = "TransformationDiagram";
            public static final String MAPPING_DIAGRAM              = "MappingDiagram";
        }
    }

    public static class Virtual {
        public static final String NAME = "Virtual";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "Virtual";
        public static final String PACKAGE_NAME = "Virtual";
        public static interface Class {
            public static final String TRANSFORMATION_ELEMENT        = "TransformationElement";
            public static final String TRANSFORMATION                = "Transformation";
            public static final String TRANSFORMATION_OPERATION      = "TransformationOperation";
            public static final String TEMPORARY_GROUP               = "TemporaryGroup";
            public static final String QUERY                         = "Query";
            public static final String LINK                          = "Link";
            public static final String TRANSFORMATION_LINK           = "TransformationLink";
            public static final String TRANSFORMATION_CONTENT        = "TranformationContent";
        }
    }

    public static class Function {
        public static final String NAME = "MetaMatrixFunction";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "MetaMatrixFunction";
        public static final String PACKAGE_NAME = "MetaMatrixFunction";
        public static interface Class {
            public static final String MODEL                         = "Model";
            public static final String FUNCTION                      = "Function";
            public static final String PARAMETER                     = "Parameter";
        }
    }

    public static class SimpleDataTypes {
        public static final String NAME = "SimpleDatatypes";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "SimpleDatatypes";
        public static final String PACKAGE_NAME = "SimpleDatatypes";
        public static interface Class {
            public static final String MODEL                         = "SimpleDatatypeModel";
            public static final String NAMESPACE                     = "Domain";
        }
    }

    public static class XMLSchema {
        public static final String NAME = "XMLSchema";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "MMXMLSchema";
        public static final String PACKAGE_NAME = "XMLSchema";
        public static interface Class {
            public static final String MODEL                        = "Model";
            public static final String SCHEMA_DOCUMENT              = "SchemaDocument";
            public static final String SIMPLE_DATA_TYPE             = "SimpleType";
            public static final String ATOMIC_DATA_TYPE             = "AtomicType";
            public static final String UNION_DATA_TYPE              = "UnionType";
            public static final String LIST_DATA_TYPE               = "ListType";
            public static final String PATTERN                      = "Pattern";
            public static final String ENUMERATION                  = "Enumeration";
            public static final String ATTRIBUTE                    = "Attribute";
            public static final String ELEMENT                      = "Element";
            public static final String COMPLEX_TYPE                 = "ComplexType";
            public static final String ATTRIBUTE_GROUP              = "AttributeGroup";
            public static final String GROUP                        = "Group";
            public static final String ANY                          = "Any";
            public static final String ANY_TYPE                     = "AnyType";
            public static final String ANY_ATTRIBUTE                = "AnyAttribute";
            public static final String NS_CONSTRAINT                = "NamespaceConstraint";
            public static final String INCLUDE                      = "Include";
            public static final String IMPORT                       = "Import";
            public static final String REDEFINE                     = "Redefine";
            public static final String ANNOTATION                   = "Annotation";
            public static final String APP_INFO                     = "ApplicationInfo";
            public static final String DOCUMENTATION                = "Documentation";
            public static final String CONTENT_MODEL                = "AbstractCompositor";
            public static final String SEQUENCE                     = "Sequence";
            public static final String CHOICE                       = "Choice";
            public static final String ALL                          = "All";
            public static final String NOTATION                     = "Notation";
        }
    }

    public static class XMLDocument {
        public static final String NAME = "XMLDocument";
        public static final String FILENAME = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = "MMXMLDocument";
        public static final String PACKAGE_NAME = "XMLDocument";
        public static interface Class {
            public static final String MODEL                       = "Model";
            public static final String XML_SCHEMA_REF              = "XmlSchema";
            public static final String XML_DOCUMENT                = "Document";
            public static final String ELEMENT                     = "Element";
            public static final String ATTRIBUTE                   = "Attribute";
            public static final String COMMENT                     = "Comment";
            public static final String NAMESPACE                   = "Namespace";
            public static final String CONTENT_MODEL               = "AbstractCompositor";
            public static final String SEQUENCE                    = "Sequence";
            public static final String CHOICE                      = "Choice";
            public static final String ALL                         = "All";
            public static final String MAPPING_CATEGORY            = "MappingClasses";
            public static final String MAPPING_GROUP               = "MappingClass";
            public static final String MAPPING_ELEMENT             = "MappingAttribute";
            public static final String TEMPORARY_TABLE             = "TemporaryTable";
            public static final String TEMPORARY_COLUMN            = "TemporaryColumn";
        }
    }

    public static class Connections {
        public static final String NAME             = "Connections";
        public static final String FILENAME         = NAME + METAMODEL_FILE_EXTENSION;
        public static final String NAMESPACE_PREFIX = NAME;
        public static final String PACKAGE_NAME     = NAME;
        public static interface Class {
            public static final String CONFIGURATION           = "Configuration";
            public static final String CONNECTION              = "Connection";
            public static final String CONNECTIONS             = "Connections";
            public static final String IMPORT_CONNECTION       = "ImportConnection";
            public static final String IMPORTER_EXTENSION_NAME = "ImporterExtensionName"; 
            public static final String OPTION                  = "Option";
            public static final String OPTION_VALUE            = "Value";
            public static final String URL                     = "URL";
            public static final String USER_ID                 = "UserID";
        }
    }
}
