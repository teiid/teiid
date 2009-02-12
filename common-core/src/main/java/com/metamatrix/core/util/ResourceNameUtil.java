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

package com.metamatrix.core.util;

import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.CorePlugin;

/** 
 * Utility class used for determining if proposed resource names are reserved names or not.
 * This applies for *.vdb, *.xmi and *.xsd resources
 * NOTE: These lists are arranged in alphabetical order. When adding new reserved names, please place them appropriately.
 * @since 4.3
 */
public abstract class ResourceNameUtil {
    private static final String INVALID_EXTENSION_ERROR_ID = "ResourceNameUtil.invalidFileExtensionError"; //$NON-NLS-1$
    private static final String COMMA_SPACE = ", "; //$NON-NLS-1$
    
    // STATIC RESERVED NAME CONSTANTS
    public static final String XMI_FILE_EXTENSION   = "xmi"; //$NON-NLS-1$
    public static final String VDB_FILE_EXTENSION   = "vdb"; //$NON-NLS-1$
    public static final String XSD_FILE_EXTENSION   = "xsd"; //$NON-NLS-1$
    public static final String XML_FILE_EXTENSION   = "xml"; //$NON-NLS-1$
    public static final String WSDL_FILE_EXTENSION  = "wsdl"; //$NON-NLS-1$
    
    public static final String DOT_XMI_FILE_EXTENSION = ".xmi"; //$NON-NLS-1$
    public static final String DOT_VDB_FILE_EXTENSION   = ".vdb"; //$NON-NLS-1$
    public static final String DOT_XSD_FILE_EXTENSION   = ".xsd"; //$NON-NLS-1$
    public static final String DOT_XML_FILE_EXTENSION   = ".xml"; //$NON-NLS-1$
    public static final String DOT_WSDL_FILE_EXTENSION   = ".wsdl"; //$NON-NLS-1$
    
    public static final String ADMIN_NAME                       = "Admin"; //$NON-NLS-1$
    public static final String BUILTINDATATYPES_NAME            = "builtInDataTypes"; //$NON-NLS-1$
    public static final String BUILTINRELATIONALTYPES_NAME      = "builtInRelationshipTypes"; //$NON-NLS-1$
    public static final String CORE_NAME                        = "Core"; //$NON-NLS-1$
    public static final String DATAACCESS_NAME                  = "Dataaccess"; //$NON-NLS-1$
    public static final String DATASERVICESYSTEMMODEL_NAME      = "DataServiceSystemModel"; //$NON-NLS-1$
    public static final String DTCBASE_NAME                     = "DtcBase"; //$NON-NLS-1$
    public static final String ECORE_NAME                       = "Ecore"; //$NON-NLS-1$
    public static final String ENTERPRISEDATATYPES_NAME         = "EnterpriseDatatypes"; //$NON-NLS-1$
    public static final String EXTENSION_NAME                   = "Extension"; //$NON-NLS-1$
    public static final String FUNCTION_NAME                    = "Function"; //$NON-NLS-1$
    public static final String HELP_NAME                        = "Help"; //$NON-NLS-1$
    public static final String JDBC_NAME                        = "Jdbc"; //$NON-NLS-1$
    public static final String JDBCMODEL_NAME                   = "jdbcModel"; //$NON-NLS-1$
    public static final String JDBCSYSTEM_NAME                  = "JDBCSystem"; //$NON-NLS-1$
    public static final String MAGICXMLSCHEMA_NAME              = "MagicXMLSchema"; //$NON-NLS-1$
    public static final String MANIFEST_NAME                    = "Manifest"; //$NON-NLS-1$
    public static final String MAPPING_NAME                     = "Mapping"; //$NON-NLS-1$
    public static final String MBR_NAME                         = "Mbr"; //$NON-NLS-1$
    public static final String METAMATRIX_VDBMANIFESTMODEL_NAME = "MetaMatrix-VdbManifestModel"; //$NON-NLS-1$
    public static final String METAMODELRELATIONALMODEL_NAME    = "MetamodelRelationalModel"; //$NON-NLS-1$
    public static final String NAMESPACE_NAME                   = "namespace"; //$NON-NLS-1$
    public static final String PRIMATIVETYPES_NAME              = "primitiveTypes"; //$NON-NLS-1$
    public static final String RELATIONAL_NAME                  = "Relational"; //$NON-NLS-1$
    public static final String RELATIONSHIP_NAME                = "Relationship"; //$NON-NLS-1$
    public static final String SIMPLEDATATYPES_INSTANCE_NAME    = "SimpleDatatypes-instance"; //$NON-NLS-1$
    public static final String SYSTEM_NAME                      = "System"; //$NON-NLS-1$    
    public static final String SYSTEMADMIN_NAME                 = CoreConstants.SYSTEM_ADMIN_MODEL_NAME;
    public static final String SYSTEMADMINPHYSICAL_NAME         = CoreConstants.SYSTEM_ADMIN_PHYSICAL_MODEL_NAME;
    public static final String SYSTEMPHYSICAL_NAME              = CoreConstants.SYSTEM_PHYSICAL_MODEL_NAME;
    public static final String SYSTEMSCHEMA_NAME                = "SystemSchema"; //$NON-NLS-1$
    public static final String SYSTEMVIRTUALDATABASE_NAME       = "SystemVirtualDatabase"; //$NON-NLS-1$
    public static final String SYSTEMODBCMODEL                  = "System.ODBC"; //$NON-NLS-1$
    public static final String TRANSFORMATION_NAME              = "Transformation"; //$NON-NLS-1$
    public static final String UML2_NAME                        = "Uml2"; //$NON-NLS-1$
    public static final String WEBSERVICE_NAME                  = "Webservice"; //$NON-NLS-1$
    public static final String WSDL1_1_NAME                     = "WSDL1_1"; //$NON-NLS-1$
    public static final String WSDLSOAP_NAME                    = "WSDLSOAP"; //$NON-NLS-1$
    public static final String XML_NAME                         = "Xml"; //$NON-NLS-1$
    public static final String XMLSCHEMA_NAME                   = "XMLSchema"; //$NON-NLS-1$
    public static final String XMLSCHEMA_INSTANCE_NAME          = "XMLSchema-instance"; //$NON-NLS-1$
    public static final String XSD_NAME                         = "Xsd"; //$NON-NLS-1$
    
    public static final String[] RESERVED_VDB_NAMES = {
        ADMIN_NAME,
        HELP_NAME,
        SYSTEM_NAME,
        SYSTEMVIRTUALDATABASE_NAME,
    };

    public static final String USERFILES_FOLDERNAME             = "user-files"; //$NON-NLS-1$
    
    public static final String[] RESERVED_XMI_NAMES = {
        CORE_NAME,
        BUILTINRELATIONALTYPES_NAME,
        DATAACCESS_NAME,
        DATASERVICESYSTEMMODEL_NAME,
        DTCBASE_NAME,
        ECORE_NAME,
        ENTERPRISEDATATYPES_NAME,
        EXTENSION_NAME,
        FUNCTION_NAME,
        JDBC_NAME,
        JDBCMODEL_NAME,
        JDBCSYSTEM_NAME,
        MANIFEST_NAME,
        MAPPING_NAME,
        MBR_NAME,
        METAMATRIX_VDBMANIFESTMODEL_NAME,
        METAMODELRELATIONALMODEL_NAME,
        PRIMATIVETYPES_NAME,
        RELATIONAL_NAME,
        RELATIONSHIP_NAME,
        SIMPLEDATATYPES_INSTANCE_NAME,
        SYSTEM_NAME,
        SYSTEMADMIN_NAME,
        SYSTEMADMINPHYSICAL_NAME,
        SYSTEMPHYSICAL_NAME,
        TRANSFORMATION_NAME,
        UML2_NAME,
        WEBSERVICE_NAME,
        WSDL1_1_NAME,
        WSDLSOAP_NAME,
        XML_NAME,
        XSD_NAME,
    };


    public static final String[] RESERVED_XSD_NAMES = {
        BUILTINDATATYPES_NAME,
        ENTERPRISEDATATYPES_NAME,
        MAGICXMLSCHEMA_NAME,
        NAMESPACE_NAME,
        SIMPLEDATATYPES_INSTANCE_NAME,
        SYSTEMSCHEMA_NAME,
        XML_NAME,
        XMLSCHEMA_NAME,
        XMLSCHEMA_INSTANCE_NAME,
    };
    
    
    public static final String[] RESERVED_PROJECT_NAMES = {
        USERFILES_FOLDERNAME,
    };
    
    /**
     * This method checks whether or not a proposed project name is reserved or not.
     * @param proposedName may or may not inlude the file extension
     * @return true if it is reserved, false if not.
     * @since 5.5.3
     */
    public static boolean isReservedProjectName(String proposedName) {
        if( proposedName == null || proposedName.length() <= 0 ){
            return false;
        }
        
        for( int i=0; i<RESERVED_PROJECT_NAMES.length; i++ ) {
            if( proposedName.equalsIgnoreCase(RESERVED_PROJECT_NAMES[i])) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * This method checks whether or not a proposed vdb name is reserved or not.
     * It will return false if the proposed name includes an extension AND one or more "." characters.
     * @param proposedName may or may not inlude the file extension
     * @return true if it is reserved, false if not.
     * @throws IllegalArgumentException if proposed name contains an apparent file extension (one or more '.' characters) 
     * and it is NOT a ".vdb" extension
     * @since 5.0
     */
    public static boolean isReservedVdbName(String proposedName) throws IllegalArgumentException {
        if( proposedName == null || proposedName.length() <= 0 ){
            return false;
        }
        
        // Check the extension
        if ( proposedName.indexOf('.') != -1 ) {
            // Check ends with
            if( !proposedName.endsWith(DOT_VDB_FILE_EXTENSION)) {
                throw new IllegalArgumentException(CorePlugin.Util.getString(INVALID_EXTENSION_ERROR_ID, proposedName, VDB_FILE_EXTENSION));
            }
            
            // So, let's take the extension off
            proposedName = proposedName.substring(0, proposedName.lastIndexOf(DOT_VDB_FILE_EXTENSION));
            
            // If the name still has a "." in it, then it will not be a reserved name
            // Note 
            if ( proposedName.indexOf('.') != -1 ) {
                return false;
            }
        }

        for( int i=0; i<RESERVED_VDB_NAMES.length; i++ ) {
            if( proposedName.equalsIgnoreCase(RESERVED_VDB_NAMES[i])) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * This method checks whether or not a proposed xmi model name is reserved or not.
     * It will return false if the proposed name includes an extension AND one or more "." characters.
     * @param proposedName may or may not inlude the file extension
     * @return true if it is reserved, false if not.
     * @throws IllegalArgumentException if proposed name contains an apparent file extension (one or more '.' characters) 
     * and it is NOT a ".xmi" extension
     * @since 5.0
     */
    public static boolean isReservedModelName(String proposedName) throws IllegalArgumentException  {
        if( proposedName == null || proposedName.length() <= 0 ){
            return false;
        }
        
        // Check the extension
        if ( proposedName.indexOf('.') != -1 ) {
            // Check ends with
            if( !proposedName.endsWith(DOT_XMI_FILE_EXTENSION)) {
                throw new IllegalArgumentException(CorePlugin.Util.getString(INVALID_EXTENSION_ERROR_ID, proposedName, XMI_FILE_EXTENSION));
            }
            
            // So, let's take the extension off
            proposedName = proposedName.substring(0, proposedName.lastIndexOf(DOT_XMI_FILE_EXTENSION));
            
            // If the name still has a "." in it, then it will not be a reserved name
            // Note 
            if ( proposedName.indexOf('.') != -1 ) {
                return false;
            }
        }
        
        for( int i=0; i<RESERVED_XMI_NAMES.length; i++ ) {
            if( proposedName.equalsIgnoreCase(RESERVED_XMI_NAMES[i])) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * This method checks whether or not a proposed xsd model name is reserved or not.
     * It will return false if the proposed name includes an extension AND one or more "." characters.
     * @param proposedName may or may not inlude the file extension
     * @return true if it is reserved, false if not.
     * @throws IllegalArgumentException if proposed name contains an apparent file extension (one or more '.' characters) 
     * and it is NOT a ".xsd" extension
     * @since 5.0
     */
    public static boolean isReservedSchemaName(String proposedName) throws IllegalArgumentException  {
        if( proposedName == null || proposedName.length() <= 0 ){
            return false;
        }
        
        // Check the extension
        if ( proposedName.indexOf('.') != -1 ) {
            // Check ends with
            if( !proposedName.endsWith(DOT_XSD_FILE_EXTENSION)) {
                throw new IllegalArgumentException(CorePlugin.Util.getString(INVALID_EXTENSION_ERROR_ID, proposedName, XSD_FILE_EXTENSION));
            }
            
            // So, let's take the extension off
            proposedName = proposedName.substring(0, proposedName.lastIndexOf(DOT_XSD_FILE_EXTENSION));
            
            // If the name still has a "." in it, then it will not be a reserved name
            // Note 
            if ( proposedName.indexOf('.') != -1 ) {
                return false;
            }
        }
        
        for( int i=0; i<RESERVED_XSD_NAMES.length; i++ ) {
            if( proposedName.equalsIgnoreCase(RESERVED_XSD_NAMES[i])) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * This method checks whether or not a proposed name is reserved or not.
     * It will check all reserved resource names including vdb, xmi and xsd resources.
     * It will return false if the proposed name includes an extension AND one or more "." characters.
     * @param proposedName may or may not inlude the file extension
     * @return true if it is reserved, false if not.
     * @throws IllegalArgumentException if proposed name contains an apparent file extension (one or more '.' characters) 
     * and it is NOT a ".xmi, .xsd, or .vdb" extension
     * @since 5.0
     */
    public static boolean isReservedResourceName(String proposedName) throws IllegalArgumentException  {
        boolean result = false;
        
        if( proposedName == null || proposedName.length() <= 0 ){
            return false;
        }
        
        // Check the extension
        if ( proposedName.indexOf('.') != -1 ) {
            // Check ends with
            if( !proposedName.endsWith(DOT_XSD_FILE_EXTENSION) && 
                !proposedName.endsWith(DOT_XMI_FILE_EXTENSION) &&
                !proposedName.endsWith(DOT_VDB_FILE_EXTENSION)) {
                String allExtensions = XMI_FILE_EXTENSION + COMMA_SPACE + XSD_FILE_EXTENSION + COMMA_SPACE + VDB_FILE_EXTENSION;
                throw new IllegalArgumentException(CorePlugin.Util.getString(INVALID_EXTENSION_ERROR_ID, proposedName, allExtensions));
            }
            
            
            // So, let's take the extension off
            if( proposedName.endsWith(DOT_XSD_FILE_EXTENSION)) {
                proposedName = proposedName.substring(0, proposedName.lastIndexOf(DOT_XSD_FILE_EXTENSION));
                if ( proposedName.indexOf('.') != -1 ) {
                    result = false;
                } else  {
                    result = isReservedSchemaName(proposedName);
                }
            }
            
            if( !result ) {
                if( proposedName.endsWith(DOT_XMI_FILE_EXTENSION)) {
                    proposedName = proposedName.substring(0, proposedName.lastIndexOf(DOT_XMI_FILE_EXTENSION));
                    if ( proposedName.indexOf('.') != -1 ) {
                        result = false;
                    } else  {
                        result = isReservedModelName(proposedName);
                    }
                }
            }
            
            if( !result ) {
                if( proposedName.endsWith(DOT_VDB_FILE_EXTENSION)) {
                    proposedName = proposedName.substring(0, proposedName.lastIndexOf(DOT_VDB_FILE_EXTENSION));
                    if ( proposedName.indexOf('.') != -1 ) {
                        result = false;
                    } else  {
                        result = isReservedVdbName(proposedName);
                    }
                }
            }
        } else {
            result = isReservedSchemaName(proposedName);
            
            if( !result ) {
                result = isReservedModelName(proposedName);
            }
            
            if( !result ) {
                result = isReservedVdbName(proposedName);
            }
        }
        
        return result;
    }
}
