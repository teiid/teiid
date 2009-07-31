/*
 * Copyright © 2000-2008 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.rhq;



/** 
 * @since 4.3
 */
public interface StartingEnvironmentConstants {

    public static final String INSTALL_DIR = "D:/apps/MetaMatrix/enterpriseserver/5.5.3/073008"; //$NON-NLS-1$
    public static final String PORT = "31000"; //$NON-NLS-1$
    public static final String USERNAME = "metamatrixadmin"; //$NON-NLS-1$
    public static final String PASSWORD = "mm"; //$NON-NLS-1$
    
    public static final String INSTALL_DIR2 = "D:/metamatrix/5.5.3/server_0721b"; //$NON-NLS-1$
    public static final String PORT2 = "32000"; //$NON-NLS-1$
    
    
    
    
    public static final String SINGLE_SYSTEM_PARM =  
                INSTALL_DIR + "," + //$NON-NLS-1$
                PORT + "," + //$NON-NLS-1$
                USERNAME + "," + //$NON-NLS-1$
                PASSWORD;
                
    public static final String TWO_SYSTEM_PARM = SINGLE_SYSTEM_PARM + ";" + //$NON-NLS-1$
                INSTALL_DIR2 + "," + //$NON-NLS-1$
                PORT2 + "," + //$NON-NLS-1$
                USERNAME + "," + //$NON-NLS-1$
                PASSWORD;
            
    

}
