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

package com.metamatrix.vdb.materialization.template;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.vdb.materialization.DatabaseDialect;


/** 
 * Data holder for the arguments provided to the templates used to create materialized view population scripts.
 * 
 * @since 4.2
 */
public class MaterializedViewConnectionData implements TemplateData {

    // These propertiy names are use by StringTemplate to set attribute literals
    // on template parameters (*.stg files).  They must match exactly property
    // names defined here.
    private static final String DATE = "date"; //$NON-NLS-1$
    private static final String VDB = "VDB"; //$NON-NLS-1$
    private static final String VERS = "VERS"; //$NON-NLS-1$
    private static final String HOST = "HOST"; //$NON-NLS-1$
    private static final String PORT = "PORT"; //$NON-NLS-1$
    private static final String MM_DRIVER = "MMDRIVER"; //$NON-NLS-1$
    private static final String MM_PWD = "MMPWD"; //$NON-NLS-1$
    private static final String MM_USER = "MMUSER"; //$NON-NLS-1$
    private static final String MM_PROTOCOL = "MMPROTOCOL"; //$NON-NLS-1$
    private static final String MAT_URL = "MATURL"; //$NON-NLS-1$
    private static final String MAT_DRIVER = "MATDRIVER"; //$NON-NLS-1$
    private static final String MAT_PWD = "MATPWD"; //$NON-NLS-1$
    private static final String MAT_USER = "MATUSER"; //$NON-NLS-1$
    private static final String TRUNC_SCRIPT = "TRUNC_SCRIPT"; //$NON-NLS-1$
    private static final String LOAD_SCRIPT = "LOAD_SCRIPT"; //$NON-NLS-1$
    private static final String SWAP_SCRIPT = "SWAP_SCRIPT"; //$NON-NLS-1$
    private static final String LOG_FILE = "LOG_FILE"; //$NON-NLS-1$
    
    private String date;
    private String vdb;
    private String vdbVersion;
    private String host;
    private String port;
    private String mmDriver;
    private String mmPwd;
    private String mmUser;
    private String mmProtocol;
    private String matUrl;
    private String matDriver;
    private String matPwd;
    private String matUser;
    private String truncScript;
    private String loadScript;
    private String swapScript;
    private String logFileName;

    /** 
     * Groups all the parameters used to drive creation of materialized view connection property file.
     * Strings that go into the connection properties file as property values must be converted to
     * properly escape restricted chars.
     * @param vdb
     * @param vdbVersion
     * @param host
     * @param mmDriver
     * @param mmPwd
     * @param mmUser
     * @param matUrl
     * @param matDriver
     * @param matPwd
     * @param matUser
     * @param loadScript
     * @param swapScript
     * @param logFileName 
     * @param viewName
     * @param trunScript
     * @since 4.2
     */
    public MaterializedViewConnectionData(String vdb,
                                          String vdbVersion,
                                          String host,
                                          String port,
                                          String mmDriver,
                                          String mmPwd,
                                          String mmUser,
                                          String mmProtocol,
                                          String matUrl,
                                          String matDriver,
                                          String matPwd,
                                          String matUser,
                                          String truncScript,
                                          String loadScript,
                                          String swapScript, 
                                          String logFileName) {
        super();
        this.vdb = PropertiesUtils.saveConvert(vdb, false);
        this.vdbVersion = vdbVersion;
        this.host = PropertiesUtils.saveConvert(host, false);
        this.port = port;
        this.mmDriver = PropertiesUtils.saveConvert(mmDriver, false);
        this.mmPwd = mmPwd;
        this.mmUser = PropertiesUtils.saveConvert(mmUser, false);
        this.matUrl = PropertiesUtils.saveConvert(matUrl, false);
        this.matDriver = PropertiesUtils.saveConvert(matDriver, false);
        this.matPwd = matPwd;
        this.matUser = PropertiesUtils.saveConvert(matUser, false);
        this.truncScript = PropertiesUtils.saveConvert(truncScript, false);
        this.loadScript = PropertiesUtils.saveConvert(loadScript, false);
        this.swapScript = PropertiesUtils.saveConvert(swapScript, false);
        this.logFileName = PropertiesUtils.saveConvert(logFileName, false);
        this.date = DateUtil.getCurrentDateAsString();
        this.mmProtocol = mmProtocol;
        
        
    }

    /**
     * Translate all of the data into the parameter names used by the template. 
     */
    public void populateTemplate(Template template, DatabaseDialect database) {
        template.setAttribute(DATE, date);
        template.setAttribute(VDB,  vdb);
        template.setAttribute(VERS, vdbVersion);
        template.setAttribute(HOST, host);
        template.setAttribute(PORT, port);
        template.setAttribute(MM_DRIVER, mmDriver);
        template.setAttribute(MM_PWD, mmPwd);
        template.setAttribute(MM_USER, mmUser);
        template.setAttribute(MM_PROTOCOL, mmProtocol);
        template.setAttribute(MAT_URL, matUrl);
        template.setAttribute(MAT_DRIVER, matDriver);
        template.setAttribute(MAT_PWD, matPwd);
        template.setAttribute(MAT_USER, matUser);
        template.setAttribute(TRUNC_SCRIPT, truncScript);
        template.setAttribute(LOAD_SCRIPT, loadScript);
        template.setAttribute(SWAP_SCRIPT, swapScript);
        template.setAttribute(LOG_FILE, logFileName);
    }

}
