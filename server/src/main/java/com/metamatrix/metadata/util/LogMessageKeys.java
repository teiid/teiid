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

/*
 */
package com.metamatrix.metadata.util;

/**
 * @author lphillips
 * @since 3.1
 * Class for maintaining the property file keys for Metadata log messages.
 */
public interface LogMessageKeys {
//  *********************** Generic Metadata ***********************  
    //  generic (000)
    public final static String GEN_0001 = "MSG.008.000.0001";

//  *********************** Metadata Runtime ***********************      
    // runtime (001)
        //RuntimeMetadataCatalog
    public final static String RTMDC_0001 = "MSG.008.001.0001";
    public final static String RTMDC_0002 = "MSG.008.001.0002";
    public final static String RTMDC_0003 = "MSG.008.001.0003";
     
    // runtime.api (002)

    // runtime.event (003)

    // runtime.exception (004)

    // runtime.model (005)
        //MetadataCache
    public final static String MDC_0001 = "MSG.008.005.0001";
    
        //MetadataLoadinCache
    public final static String MLC_0001 = "MSG.008.005.0005";
    
        //UpdateController
    public final static String UC_0001 = "MSG.008.005.0002";
    public final static String UC_0002 = "MSG.008.005.0003";
    public final static String UC_0003 = "MSG.008.005.0004";

    // runtime.spi (006)
        //JDBCConnector
    public final static String JDBCC_0001 = "MSG.008.006.0001";
    public final static String JDBCC_0002 = "MSG.008.006.0002";
    public final static String JDBCC_0003 = "MSG.008.006.0003";
        //JDBCRuntimeMetadataReader
    public final static String JDBCR_0001 = "MSG.008.006.0004";
    public final static String JDBCR_0002 = "MSG.008.006.0005";
    public final static String JDBCR_0003 = "MSG.008.006.0006";
    
    
    // runtime.util (007)
    
//  *********************** Metadata Searchbase ***********************
    // searchbase (008)
        //SearchBaseManager
    public final static String SBM_0001 = "MSG.008.008.0001";
    public final static String SBM_0002 = "MSG.008.008.0002";
    public final static String SBM_0003 = "MSG.008.008.0003"; 
    public final static String SBM_0004 = "MSG.008.008.0004"; 
    public final static String SBM_0005 = "MSG.008.008.0005";
    public final static String SBM_0006 = "MSG.008.008.0006";
    public final static String SBM_0007 = "MSG.008.008.0007";            
    public final static String SBM_0008 = "MSG.008.008.0008";

    // searchbase.api (009)

    // searchbase.apiimpl (010)

    // searchbase.command (011)

    // searchbase.exception (012)

    // searchbase.jdbc (013)

    // searchbase.processor (014)

    // searchbase.tool (015)

    // searchbase.transform (016)

    // searchbase.transform.xml (017)

    // searchbase.util (018)

    // searchbase.xmi (019)
    
//  *********************** Metadata Server *********************** 
    // server.api (020)
    // server.api.dtc (021)
    // server.api.event (022)
    // server.api.exception (023)
    
    // server.apiimpl (024)
        //MetadataServerRequest
    public final static String MSR_0001 = "MSG.008.024.0001";
        //ResponseObjectBuilder
    public final static String ROB_0001 = "MSG.008.024.0002";
    public final static String ROB_0002 = "MSG.008.024.0003";      

    // server.apiimpl.dtc (025)
    // server.directory.api.exception (026)
    // server.directory.api.service (027)
        //DirectoryServiceProxy
    public final static String DSP_0001 = "MSG.008.027.0001";
    
    // server.directory.dtc (028)
        //DTCInProgressMgr
    public final static String DIPM_0001 = "MSG.008.028.0001";
    public final static String DIPM_0002 = "MSG.008.028.0002";
        //DTCValidation
    public final static String DTCV_0001 = "MSG.008.028.0003";
    public final static String DTCV_0002 = "MSG.008.028.0004";
    public final static String DTCV_0003 = "MSG.008.028.0005";
    public final static String DTCV_0004 = "MSG.008.028.0006";
    public final static String DTCV_0005 = "MSG.008.028.0007";
    public final static String DTCV_0006 = "MSG.008.028.0008";
    public final static String DTCV_0007 = "MSG.008.028.0009";
    public final static String DTCV_0008 = "MSG.008.028.0010";
    public final static String DTCV_0009 = "MSG.008.028.0011";
    public final static String DTCV_0010 = "MSG.008.028.0012";
    public final static String DTCV_0011 = "MSG.008.028.0013";
    public final static String DTCV_0012 = "MSG.008.028.0014";
    public final static String DTCV_0013 = "MSG.008.028.0015";
    public final static String DTCV_0014 = "MSG.008.028.0016";
    public final static String DTCV_0015 = "MSG.008.028.0017";
    public final static String DTCV_0016 = "MSG.008.028.0018";
    
    // server.directory.service (029)
        //DirectoryServiceImpl
    public final static String DSI_0001 = "MSG.008.029.0001";
    public final static String DSI_0002 = "MSG.008.029.0002";
    public final static String DSI_0003 = "MSG.008.029.0003";
    public final static String DSI_0004 = "MSG.008.029.0004";
    public final static String DSI_0005 = "MSG.008.029.0005";
    public final static String DSI_0006 = "MSG.008.029.0006";
    public final static String DSI_0007 = "MSG.008.029.0007";
    public final static String DSI_0008 = "MSG.008.029.0008";
    public final static String DSI_0009 = "MSG.008.029.0009";
    public final static String DSI_0010 = "MSG.008.029.0010";
    public final static String DSI_0011 = "MSG.008.029.0011";
    public final static String DSI_0012 = "MSG.008.029.0012";
    public final static String DSI_0013 = "MSG.008.029.0013";
    public final static String DSI_0014 = "MSG.008.029.0014";
    
    // server.directory.spi (030)
    // server.directory.spi.jdbc (031)
        //JDBCDirectoryConnection
    public final static String JDC_0001 = "MSG.008.031.0001";
    public final static String JDC_0002 = "MSG.008.031.0002";
        //JDBCDirectoryWriter   
    public final static String JDW_0001 = "MSG.008.031.0003";
    
    // server.resource (032)
        //DirectoryResourceResolverEnvironment
    public final static String DRRE_0001 = "MSG.008.032.0001";
    public final static String DRRE_0002 = "MSG.008.032.0002";
    public final static String DRRE_0003 = "MSG.008.032.0003";
    public final static String DRRE_0004 = "MSG.008.032.0004";
    public final static String DRRE_0005 = "MSG.008.032.0005";
    public final static String DRRE_0006 = "MSG.008.032.0006";
    public final static String DRRE_0007 = "MSG.008.032.0007";
    public final static String DRRE_0008 = "MSG.008.032.0008";
    public final static String DRRE_0009 = "MSG.008.032.0009";
    public final static String DRRE_0010 = "MSG.008.032.0010";
    public final static String DRRE_0011 = "MSG.008.032.0011";
    public final static String DRRE_0012 = "MSG.008.032.0012";
    
    // server.searchbase.api.service (033)
        //SearchbaseServiceProxy
    public final static String SSP_0001 = "MSG.008.033.0001";
    
    // server.searchbase.service (034)
        //  SearchbaseServiceImpl
    public final static String SSI_0001 = "MSG.008.034.0001";
    public final static String SSI_0002 = "MSG.008.034.0002";
    public final static String SSI_0003 = "MSG.008.034.0003";
    
    // server.serverapi (035)
    // server.serverapi.beans (036)
    // server.serverapi.beans.metadataserverapi (037)
    // server.serverapi.exception (038)
    // server.service (039)
    // server.util (040)   

//  *********************** Metadata Toolkit *********************** 
}
