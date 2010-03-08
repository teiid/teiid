package org.teiid.connector.xml.file;

import org.teiid.connector.xmlsource.file.FileManagedConnectionFactory;

import com.metamatrix.core.util.UnitTestUtil;

public class FakeFileManagedConnectionfactory {
	
    public static FileManagedConnectionFactory getDefaultFileProps() {
    	FileManagedConnectionFactory env = new FileManagedConnectionFactory();
    	env.setCapabilitiesClass("com.metamatrix.connector.xml.base.XMLCapabilities");
    	env.setDirectoryLocation(UnitTestUtil.getTestDataPath()+"/documents");
    	env.setFileName("state_college2.xml");
        
        return env;
    }
    public static String getDocumentsFolder() {
		return UnitTestUtil.getTestDataPath()+"/documents";
    }    
}
