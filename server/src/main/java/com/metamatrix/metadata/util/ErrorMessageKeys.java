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
 * Class for maintaining the property file keys for Metadata error messages. 
 */
public interface ErrorMessageKeys {
//  *********************** Generic Metadata ***********************  
    // generic (000)
    public final static String GEN_0001 = "ERR.008.000.0001";
    public final static String GEN_0002 = "ERR.008.000.0002";
    public final static String GEN_0003 = "ERR.008.000.0003";
    public final static String GEN_0004 = "ERR.008.000.0004";
    public final static String GEN_0005 = "ERR.008.000.0005";
    public final static String GEN_0006 = "ERR.008.000.0006";
    public final static String GEN_0007 = "ERR.008.000.0007";
    public final static String GEN_0008 = "ERR.008.000.0008";
    public final static String GEN_0009 = "ERR.008.000.0009";
    public final static String GEN_0010 = "ERR.008.000.0010";
    public final static String GEN_0011 = "ERR.008.000.0011";
    public final static String GEN_0012 = "ERR.008.000.0012";

// *********************** Metadata Runtime ***********************    
    // runtime (001)
        //RuntimeMetadataCatalog
    public final static String RTMDC_0001 = "ERR.008.001.0001";
    public final static String RTMDC_0002 = "ERR.008.001.0002";
    public final static String RTMDC_0003 = "ERR.008.001.0003";
    public final static String RTMDC_0004 = "ERR.008.001.0004";
    public final static String RTMDC_0005 = "ERR.008.001.0005";
    public final static String RTMDC_0006 = "ERR.008.001.0006";
    public final static String RTMDC_0007 = "ERR.008.001.0007";
    public final static String RTMDC_0008 = "ERR.008.001.0008";
    public final static String RTMDC_0009 = "ERR.008.001.0009";
    public final static String RTMDC_0010 = "ERR.008.001.0010";
    public final static String RTMDC_0011 = "ERR.008.001.0011";
    public final static String RTMDC_0012 = "ERR.008.001.0012";
    public final static String RTMDC_0013 = "ERR.008.001.0013";
    public final static String RTMDC_0014 = "ERR.008.001.0014";
    public final static String RTMDC_0015 = "ERR.008.001.0015";
    public final static String RTMDC_0016 = "ERR.008.001.0016";
    public final static String RTMDC_0017 = "ERR.008.001.0017";
    public final static String RTMDC_0018 = "ERR.008.001.0018"; 
    public final static String RTMDC_0019 = "ERR.008.001.0019";
    public final static String RTMDC_0020 = "ERR.008.001.0020";
    public final static String RTMDC_0021 = "ERR.008.001.0021";
    public final static String RTMDC_0022 = "ERR.008.001.0022";
    public final static String RTMDC_0023 = "ERR.008.001.0023";              
    public final static String RTMDC_0024 = "ERR.008.001.0024";
    public final static String RTMDC_0025 = "ERR.008.001.0025";

        //VDBDeleteUtility
    public final static String VDBDU_0001 = "ERR.008.001.0026";
    public final static String VDBDU_0002 = "ERR.008.001.0027";
    public final static String VDBDU_0003 = "ERR.008.001.0028";
    public final static String VDBDU_0004 = "ERR.008.001.0029";
    public final static String VDBDU_0005 = "ERR.008.001.0030";
    
    
    /**
     * VDB Tree Utility
     */
    public final static String VDBTREE_0001 = "ERR.008.001.0031";
    public final static String VDBTREE_0002 = "ERR.008.001.0032";
//    public final static String VDBTREE_0003 = "ERR.008.001.0033";
    

             
    // runtime.api (002)

    // runtime.event (003)

    // runtime.exception (004)

    // runtime.model (005)
        //BasicElementID
    public final static String BEID_0001 = "ERR.008.005.0001";
        //BasicGroupID
    public final static String BGID_0001 = "ERR.008.005.0002";    
        //BasicKeyID
    public final static String BKID_0001 = "ERR.008.005.0003";
        //BasicMetadataID
    public final static String BMDID_0001 = "ERR.008.005.0004"; 
    public final static String BMDID_0002 = "ERR.008.005.0005";
//    public final static String BMDID_0003 = "ERR.008.005.0006"; 
        //BasicMetadataObject
    public final static String BMO_0001 = "ERR.008.005.0007"; 
    public final static String BMO_0002 = "ERR.008.005.0008"; 
    public final static String BMO_0003 = "ERR.008.005.0009"; 
        //BasicMetadataSupplierRequest
//    public final static String BMSR_0001 = "ERR.008.005.0010";
//    public final static String BMSR_0002 = "ERR.008.005.0011";
//    public final static String BMSR_0003 = "ERR.008.005.0012";
//    public final static String BMSR_0004 = "ERR.008.005.0013";      
//    public final static String BMSR_0005 = "ERR.008.005.0014";
//    public final static String BMSR_0006 = "ERR.008.005.0015";
        //BasicModelID
    public final static String BMID_0001 = "ERR.008.005.0016";
        //BasicProcedureID
    public final static String BPID_0001 = "ERR.008.005.0017";
        //BasicVirtualDatabaseFactory
//    public final static String BVDBF_0001 = "ERR.008.005.0018";
//    public final static String BVDBF_0002 = "ERR.008.005.0019";
//    public final static String BVDBF_0003 = "ERR.008.005.0020";
//    public final static String BVDBF_0004 = "ERR.008.005.0021";
//    public final static String BVDBF_0005 = "ERR.008.005.0022";
//    public final static String BVDBF_0006 = "ERR.008.005.0023";
//    public final static String BVDBF_0007 = "ERR.008.005.0024";
//    public final static String BVDBF_0008 = "ERR.008.005.0025";
//    public final static String BVDBF_0009 = "ERR.008.005.0026";
//    public final static String BVDBF_0010 = "ERR.008.005.0027";
//    public final static String BVDBF_0011 = "ERR.008.005.0028";
//    public final static String BVDBF_0012 = "ERR.008.005.0029";
//    public final static String BVDBF_0013 = "ERR.008.005.0030";
//    public final static String BVDBF_0014 = "ERR.008.005.0031";
        //BasicVirtualDatabaseMetadata
    public final static String BVDBMD_0001 = "ERR.008.005.0032";
    public final static String BVDBMD_0002 = "ERR.008.005.0033";
    public final static String BVDBMD_0003 = "ERR.008.005.0034";
    public final static String BVDBMD_0004 = "ERR.008.005.0035";
    public final static String BVDBMD_0005 = "ERR.008.005.0036";
        //MetadataCache
    public final static String MDC_0001 = "ERR.008.005.0037";
    public final static String MDC_0002 = "ERR.008.005.0038";
    public final static String MDC_0003 = "ERR.008.005.0039";
    public final static String MDC_0004 = "ERR.008.005.0040";
    public final static String MDC_0005 = "ERR.008.005.0041";
    public final static String MDC_0006 = "ERR.008.005.0042";
    public final static String MDC_0007 = "ERR.008.005.0043";
    public final static String MDC_0008 = "ERR.008.005.0044";
    public final static String MDC_0009 = "ERR.008.005.0045";
    public final static String MDC_0010 = "ERR.008.005.0046";
    public final static String MDC_0011 = "ERR.008.005.0047";
    public final static String MDC_0012 = "ERR.008.005.0048";
    public final static String MDC_0013 = "ERR.008.005.0049";
    public final static String MDC_0014 = "ERR.008.005.0050";
    public final static String MDC_0015 = "ERR.008.005.0051";
    public final static String MDC_0016 = "ERR.008.005.0052";
    public final static String MDC_0017 = "ERR.008.005.0053";
    public final static String MDC_0018 = "ERR.008.005.0054";
    public final static String MDC_0019 = "ERR.008.005.0055";
    public final static String MDC_0020 = "ERR.008.005.0056";
    public final static String MDC_0021 = "ERR.008.005.0057";
    public final static String MDC_0022 = "ERR.008.005.0058";
    public final static String MDC_0023 = "ERR.008.005.0059";
    public final static String MDC_0024 = "ERR.008.005.0060";
    public final static String MDC_0025 = "ERR.008.005.0061";
    public final static String MDC_0026 = "ERR.008.005.0062";
    public final static String MDC_0027 = "ERR.008.005.0063";
        //MetadataLoadingCache
//    public final static String MLC_0001 = "ERR.008.005.0087";
//    public final static String MLC_0002 = "ERR.008.005.0088";
//    public final static String MLC_0003 = "ERR.008.005.0089";
//    public final static String MLC_0004 = "ERR.008.005.0090";
//    public final static String MLC_0005 = "ERR.008.005.0091";
//    public final static String MLC_0006 = "ERR.008.005.0092";
//    public final static String MLC_0007 = "ERR.008.005.0093";
//        //RuntimeMetadataBuilder
//    public final static String RMB_0001 = "ERR.008.005.0094"; 
//    public final static String RMB_0002 = "ERR.008.005.0095"; 
//    public final static String RMB_0003 = "ERR.008.005.0096"; 
//    public final static String RMB_0004 = "ERR.008.005.0097";
//    public final static String RMB_0005 = "ERR.008.005.0098";  
//    public final static String RMB_0006 = "ERR.008.005.0099"; 
//    public final static String RMB_0007 = "ERR.008.005.0100"; 
//    public final static String RMB_0008 = "ERR.008.005.0101"; 
//    public final static String RMB_0009 = "ERR.008.005.0102"; 
//    public final static String RMB_0010 = "ERR.008.005.0103"; 
//    public final static String RMB_0011 = "ERR.008.005.0104";
//    public final static String RMB_0012 = "ERR.008.005.0105";
//    public final static String RMB_0013 = "ERR.008.005.0106";     
//    public final static String RMB_0014 = "ERR.008.005.0107";
//    public final static String RMB_0015 = "ERR.008.005.0108";
//    public final static String RMB_0016 = "ERR.008.005.0109";
//    public final static String RMB_0017 = "ERR.008.005.0110";
//    public final static String RMB_0018 = "ERR.008.005.0111";
//    public final static String RMB_0019 = "ERR.008.005.0112";
//    public final static String RMB_0020 = "ERR.008.005.0113";
//    public final static String RMB_0021 = "ERR.008.005.0114";
//    public final static String RMB_0022 = "ERR.008.005.0115";
//    public final static String RMB_0023 = "ERR.008.005.0116";
//    public final static String RMB_0024 = "ERR.008.005.0117";
//    public final static String RMB_0025 = "ERR.008.005.0118";
//    public final static String RMB_0026 = "ERR.008.005.0119";
//    public final static String RMB_0027 = "ERR.008.005.0120";
//    public final static String RMB_0028 = "ERR.008.005.0121";
//    public final static String RMB_0029 = "ERR.008.005.0122";
        //UpdateController
    public final static String UC_0001 = "ERR.008.005.0064";
    public final static String UC_0002 = "ERR.008.005.0065";
    public final static String UC_0003 = "ERR.008.005.0066";
    public final static String UC_0004 = "ERR.008.005.0067";
    public final static String UC_0005 = "ERR.008.005.0068";
    public final static String UC_0006 = "ERR.008.005.0069";
    public final static String UC_0007 = "ERR.008.005.0070";
    public final static String UC_0008 = "ERR.008.005.0071";
    public final static String UC_0009 = "ERR.008.005.0072";
    public final static String UC_0010 = "ERR.008.005.0073";
    public final static String UC_0011 = "ERR.008.005.0074";
    public final static String UC_0012 = "ERR.008.005.0075";
    public final static String UC_0013 = "ERR.008.005.0076";
    public final static String UC_0014 = "ERR.008.005.0077";
    public final static String UC_0015 = "ERR.008.005.0078";
    public final static String UC_0016 = "ERR.008.005.0079";
    public final static String UC_0017 = "ERR.008.005.0080";
    public final static String UC_0018 = "ERR.008.005.0081";
    public final static String UC_0019 = "ERR.008.005.0082";
    public final static String UC_0020 = "ERR.008.005.0083";
    public final static String UC_0021 = "ERR.008.005.0084";
    public final static String UC_0022 = "ERR.008.005.0085";
    public final static String UC_0023 = "ERR.008.005.0086";

    // runtime.spi (006)
        //JDBCConnector
    public final static String JDBCC_0001 = "ERR.008.006.0001";
    public final static String JDBCC_0002 = "ERR.008.006.0002";
    public final static String JDBCC_0003 = "ERR.008.006.0003";
    public final static String JDBCC_0004 = "ERR.008.006.0004";
    public final static String JDBCC_0005 = "ERR.008.006.0005";
    public final static String JDBCC_0006 = "ERR.008.006.0006";
    public final static String JDBCC_0007 = "ERR.008.006.0007";
    public final static String JDBCC_0008 = "ERR.008.006.0008";
    public final static String JDBCC_0009 = "ERR.008.006.0009";
    public final static String JDBCC_0010 = "ERR.008.006.0010";
    public final static String JDBCC_0011 = "ERR.008.006.0011";
    public final static String JDBCC_0012 = "ERR.008.006.0012";
    public final static String JDBCC_0013 = "ERR.008.006.0013";
    public final static String JDBCC_0014 = "ERR.008.006.0014";
    public final static String JDBCC_0015 = "ERR.008.006.0015";
    public final static String JDBCC_0016 = "ERR.008.006.0016";
    public final static String JDBCC_0017 = "ERR.008.006.0017";
    public final static String JDBCC_0018 = "ERR.008.006.0018";
    public final static String JDBCC_0019 = "ERR.008.006.0019";
    public final static String JDBCC_0020 = "ERR.008.006.0020";
    public final static String JDBCC_0021 = "ERR.008.006.0021";
    public final static String JDBCC_0022 = "ERR.008.006.0022";
    public final static String JDBCC_0023 = "ERR.008.006.0023";
    public final static String JDBCC_0024 = "ERR.008.006.0024";
    public final static String JDBCC_0025 = "ERR.008.006.0025";
    public final static String JDBCC_0026 = "ERR.008.006.0026";
    public final static String JDBCC_0027 = "ERR.008.006.0027";
    public final static String JDBCC_0028 = "ERR.008.006.0028";
    public final static String JDBCC_0029 = "ERR.008.006.0029";
    public final static String JDBCC_0030 = "ERR.008.006.0030";
    public final static String JDBCC_0031 = "ERR.008.006.0031";
    public final static String JDBCC_0032 = "ERR.008.006.0032";
    public final static String JDBCC_0033 = "ERR.008.006.0033";
    public final static String JDBCC_0034 = "ERR.008.006.0034";
        //JDBCRuntimeMetadataReader
    public final static String JDBCR_0001 = "ERR.008.006.0035";
    public final static String JDBCR_0002 = "ERR.008.006.0036";
    public final static String JDBCR_0003 = "ERR.008.006.0037";
        //JDBCRuntimeMetadataWriter
    public final static String JDBCW_0001 = "ERR.008.006.0003";
    public final static String JDBCW_0002 = "ERR.008.006.0035";
    public final static String JDBCW_0003 = "ERR.008.006.0038";
    public final static String JDBCW_0004 = "ERR.008.006.0039";
    public final static String JDBCW_0005 = "ERR.008.006.0040";
    public final static String JDBCW_0006 = "ERR.008.006.0041";
    public final static String JDBCW_0007 = "ERR.008.006.0042";
    public final static String JDBCW_0008 = "ERR.008.006.0043";
    public final static String JDBCW_0009 = "ERR.008.006.0044";
    public final static String JDBCW_0010 = "ERR.008.006.0030";
    public final static String JDBCW_0011 = "ERR.008.006.0064";
    public final static String JDBCW_0012 = "ERR.008.006.0065";
        //JDBCTranslator
    public final static String JDBCT_0001 = "ERR.008.006.0045";
    public final static String JDBCT_0002 = "ERR.008.006.0046";
    public final static String JDBCT_0003 = "ERR.008.006.0047";

    
    // runtime.util (007)
        //RuntimeMetadataUtilities
    public final static String RMU_0001 = "ERR.008.007.0001";
        //StreamHashMap
    public final static String SHM_0001 = "ERR.008.007.0002";
    public final static String SHM_0002 = "ERR.008.007.0003";
    
   
    // server.api.event (022)
    
    // server.api.exception (023)
        //FaultMessageConstants
    public final static String FMC_0001 = "ERR.008.023.0001";
    public final static String FMC_0002 = "ERR.008.023.0002";
    public final static String FMC_0003 = "ERR.008.023.0003";
    public final static String FMC_0004 = "ERR.008.023.0004";
    public final static String FMC_0005 = "ERR.008.023.0005";
    public final static String FMC_0006 = "ERR.008.023.0006";
    public final static String FMC_0007 = "ERR.008.023.0007";
    public final static String FMC_0008 = "ERR.008.023.0008";
    public final static String FMC_0009 = "ERR.008.023.0009";
    public final static String FMC_0010 = "ERR.008.023.0010";
    public final static String FMC_0011 = "ERR.008.023.0011";
    public final static String FMC_0012 = "ERR.008.023.0012";
    public final static String FMC_0013 = "ERR.008.023.0013";
    public final static String FMC_0014 = "ERR.008.023.0014";
    

        //ObjectConverter
    public final static String OC_0001 = "ERR.008.040.0005";
    public final static String OC_0002 = "ERR.008.040.0006";
    
//  *********************** Metadata Transform ***********************
    // transform (041)
    public final static String TRANSFORM_ERR_0007 = "ERR.008.041.0007";
    public final static String TRANSFORM_ERR_0008 = "ERR.008.041.0008";

    // runtime metadata admin helper
    public static final String admin_0003 = "ERR.018.001.0003"; //$NON-NLS-1$
	public static final String admin_0004 = "ERR.018.001.0004"; //$NON-NLS-1$    
	public static final String admin_0005 = "ERR.018.001.0005"; //$NON-NLS-1$
	public static final String admin_0006 = "ERR.018.001.0006"; //$NON-NLS-1$
	public static final String admin_0008 = "ERR.018.001.0008"; //$NON-NLS-1$
	public static final String admin_0010 = "ERR.018.001.0010"; //$NON-NLS-1$
	public static final String admin_0013 = "ERR.018.001.0013"; //$NON-NLS-1$
	
	public static final String admin_0051 = "ERR.018.001.0051"; //$NON-NLS-1$	
	public static final String admin_0052 = "ERR.018.001.0052"; //$NON-NLS-1$
	public static final String admin_0053 = "ERR.018.001.0053"; //$NON-NLS-1$
	public static final String admin_0054 = "ERR.018.001.0054"; //$NON-NLS-1$
	public static final String admin_0055 = "ERR.018.001.0055"; //$NON-NLS-1$
	public static final String admin_0063 = "ERR.018.001.0063"; //$NON-NLS-1$
	
}
