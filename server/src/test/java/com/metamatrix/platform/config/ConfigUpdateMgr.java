/*
 * Copyright � 2000-2005 MetaMatrix, Inc.  All rights reserved.
 */
package com.metamatrix.platform.config;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.ComponentCryptoUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationConnector;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationMgr;
import com.metamatrix.platform.config.util.CurrentConfigHelper;

/**
 * This class provides a means to updating the configuration stored on the filesystem
 * 
 * 
 * @author vhalbert
 * Date Nov 20, 2002
 *
 */
public class ConfigUpdateMgr {
	
	private final static String path = UnitTestUtil.getTestScratchPath()
			+ File.separator + "config"; //$NON-NLS-1$

	protected static final String CONFIG_FILE = "config.xml"; //$NON-NLS-1$
	
	private XMLConfigurationConnector writer;
	
	private BasicConfigurationObjectEditor editor=new BasicConfigurationObjectEditor(true);

    public ConfigUpdateMgr() {
        super();
    	initData();
    	

    }
	

    public BasicConfigurationObjectEditor getEditor() {
        return this.editor;
    }

    public ConfigurationModelContainer getConfigModel() throws ConfigurationException {
        ConfigurationModelContainer config = CurrentConfiguration.getInstance().getConfigurationModel();
        return config;

    }
    
	protected String getPath() {
		return path;
	}
    
	public void initTransactions(Properties props) throws ConfigurationException {

		writer = XMLConfigurationMgr.getInstance().getTransaction("test"); //$NON-NLS-1$

	}
	
	public Set commit(List actions) throws ConfigurationException {
			Set commits = writer.executeActions(actions);
			writer.commit();

			writer = XMLConfigurationMgr.getInstance().getTransaction("test"); //$NON-NLS-1$	
			return commits;
   }


    public Set commit() throws ConfigurationException {
    	
    	return this.commit(editor.getDestination().popActions());
    }
    
	protected XMLConfigurationConnector getWriter() {
		return this.writer;
	}

    /**
     * Check whether the encrypted properties for the specified ConnectorBindings can be decrypted.
     * If any fail, log a warning message.
     * @param bindings Collection<ConnectorBinding>
     * @since 4.3
     */
    public void checkDecryptable(Collection bindings) throws Exception {
        
        ConfigurationModelContainer cmc = getConfigModel();
        
        //for each ConnectorBinding, check whether it can be decrypted
        List nonDecryptableBindings = new ArrayList();
        for (Iterator iter = bindings.iterator(); iter.hasNext();) {
            ConnectorBinding binding = (ConnectorBinding)iter.next();

            Collection componentTypeDefns = cmc.getAllComponentTypeDefinitions(binding.getComponentTypeID());
            boolean result = ComponentCryptoUtil.checkPropertiesDecryptable(binding, componentTypeDefns);
            
            if (! result) {
                nonDecryptableBindings.add(binding);
            }
        }
        if (nonDecryptableBindings.size() == 0) {
            return;
        }
        
        

        //build up message and log it
        StringBuffer messageBuffer = new StringBuffer();
        messageBuffer.append("The following connector bindings were added, but the passwords could not be decrypted: \n");

        for (Iterator iter = nonDecryptableBindings.iterator(); iter.hasNext();) {
            ConnectorBinding binding = (ConnectorBinding)iter.next();

            messageBuffer.append("  ");
            messageBuffer.append(binding.getName());
            messageBuffer.append("\n");
        }

        messageBuffer.append("\n\nThese bindings may have been exported from a system with a different keystore.");
        messageBuffer.append("\nYou must manually re-enter the passwords via the Console 'Properties' tab, or convert the file with the 'convertpasswords' utility and re-import.");


    }   
    
	public static void createSystemProperties(String fileName) throws Exception {
		initData();
		CurrentConfigHelper.initXMLConfig(fileName, path, "ConfigUpdateHelper");
		CurrentConfiguration.reset();
	}
	
	private static void initData() {
		File scratch = new File(path);
		if (scratch.exists()) {
			FileUtils.removeDirectoryAndChildren(scratch);
		}
		scratch.mkdir();
		try {
			FileUtils.copyDirectoryContentsRecursively(UnitTestUtil
					.getTestDataFile("config"), scratch); //$NON-NLS-1$
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}

}
