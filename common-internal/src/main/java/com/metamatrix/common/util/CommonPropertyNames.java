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

package com.metamatrix.common.util;


/**
 * Class that defines common property constants
 */
public final class CommonPropertyNames {

	public static final String DOMAIN_ID = "metamatrix.siteID"; //$NON-NLS-1$

    public static final String RMI_CONTEXT_FACTORY = "metamatrix.deployment.rmiContextFactory"; //$NON-NLS-1$

    public static final String BEAN_CONTEXT_FACTORY = "metamatrix.deployment.beanContextFactory"; //$NON-NLS-1$

    public static final String DEFAULT_JNDI_URL = "metamatrix.deployment.defaultJndiURL"; //$NON-NLS-1$

    /**
     * <p>This property indicates the VM is running inside an
     * ApplicationServer. If the property exists and is set to true</p>
     * <p>This is an optional property</p>
     */
    public static final String APP_SERVER_VM = "metamatrix.appserver_vm"; //$NON-NLS-1$
    /**
     * <p>This property indicates the EJB server platform which the MetaMatrix
     * server is running on.  Currently the only two possible values are
     * IBM's {@link #WEBSPHERE_PLATFORM WebSphere} server, and
     * the {@link #WEBLOGIC_PLATFORM Weblogic} server.  The MetaMatrix
     * server</p>
     * <p>This is a required property</p>
     */
    public static final String SERVER_PLATFORM = "metamatrix.deployment.platform"; //$NON-NLS-1$
   

    /**
     * <p>This constant defines one of the possible values for the
     * {@link #SERVER_PLATFORM} property.  It indicates that
     * no application server is running.</p>
     * <p>This is not a property, but rather a possible value for
     * the {@link #SERVER_PLATFORM} property.</p>
     */
    public static final String STANDALONE_PLATFORM = "standalone"; //$NON-NLS-1$

    /**
    * This is the installation directory defined by the bootstrapping process.
    * The bootstrapping information will provide this property by which the
    * {@link #CurrentConfiguration.getProperty()} will return.
    * @since 4.0
    */
    public static final String INSTALLATION_DIRECTORY = "metamatrix.installationDir"; //$NON-NLS-1$
        
    /**
    * This is the location the configuration models are located.  This
    * property is used by the installation and configuration persistent components.
    */
    public static final String CONFIG_MODELS_DIRECTORY = "metamatrix.config.modelsDir"; //$NON-NLS-1$
    
    /**
     * The date of installation of the MetaMatrix suite
     */
    public static final String INSTALL_DATE = "metamatrix.installationDate"; //$NON-NLS-1$
    

    /**
    * This is the property name of the MetaMatrixAdmin password.
    */
    public static final String ADMIN_PASSWORD = "metamatrix.admin.password"; //$NON-NLS-1$

    /**
    * This is the property name of the MetaMatrixAdmin username.
    */
    public static final String ADMIN_USERNAME = "metamatrix.admin.username"; //$NON-NLS-1$

	
	/**
	* If this property is set to true then client side encryption is enabled.
	*/
	public static final String CLIENT_ENCRYPTION_ENABLED = "metamatrix.encryption.client.encryption"; //$NON-NLS-1$

	/**
	* This property indicates the encryption provider, if set to none encryption is disabled.
	*/
	public static final String JCE_PROVIDER = "metamatrix.encryption.jce.provider"; //$NON-NLS-1$
    
    
    /**
    * Extension Module file types to cache in memory.
    * Should be comma-delimited list of values as stored in 
    * JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE.  
    * For example: "JAR File, Function Definition".
    * Default is none.
    */
	public static final String EXTENSION_TYPES_TO_CACHE = "metamatrix.server.extensionTypesToCache"; //$NON-NLS-1$
    

}
