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

package com.metamatrix.common.config.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import junit.framework.TestCase;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.BasicConnectorArchive;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.common.config.model.BasicExtensionModule;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.config.util.ConfigurationImportExportUtility;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.core.util.UnitTestUtil;


/**
 * Test Class for CAF files. For the unknown its "Connector Archive File". 
 * @since 4.3.2
 */
public class TestXMLConfigurationImportExportUtility extends TestCase {

    public void testImportConnectorArchive() throws Exception {
        
        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();     
        
        FileInputStream stream = new FileInputStream(UnitTestUtil.getTestDataPath()+"/configutil/sample_connector_archive.caf"); //$NON-NLS-1$
        ConnectorArchive archive = util.importConnectorArchive(stream, new BasicConfigurationObjectEditor()); 
        
        assertNotNull(archive);
        
        ConnectorBindingType type = archive.getConnectorTypes()[0]; 
        assertNotNull(type);
        assertEquals("Sample Connector", type.getName()); //$NON-NLS-1$

        ExtensionModule[] extModules = archive.getExtensionModules(type);
        assertEquals(2, extModules.length);
        
        assertEquals("jdbcconn.jar", extModules[0].getFullName());  //$NON-NLS-1$
        assertEquals("sampleconn.jar", extModules[1].getFullName()); //$NON-NLS-1$
         
    }
    
    public void testExportConnectorArchive() throws Exception{
        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();        
        ConnectorBindingType type = (ConnectorBindingType)util.importComponentType(new FileInputStream(UnitTestUtil.getTestDataPath()+"/configutil/sample.cdk"), new BasicConfigurationObjectEditor(), "sample"); //$NON-NLS-1$ //$NON-NLS-2$
        
        BasicExtensionModule ext1 = new BasicExtensionModule("jdbcconn.jar", ExtensionModule.JAR_FILE_TYPE, "Foo", ByteArrayHelper.toByteArray(new File(UnitTestUtil.getTestDataPath()+"/configutil/jdbcconn.jar"))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        BasicExtensionModule ext2 = new BasicExtensionModule("sampleconn.jar", ExtensionModule.JAR_FILE_TYPE, "Foo", ByteArrayHelper.toByteArray(new File(UnitTestUtil.getTestDataPath()+"/configutil/jdbcconn.jar"))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        
        BasicConnectorArchive ca = new BasicConnectorArchive();
        ca.addConnectorType(type);
        ca.addExtensionModule(type, ext1);
        ca.addExtensionModule(type, ext2);
 
        File f = new File(UnitTestUtil.getTestDataPath()+"/configutil/exported_sample.caf"); //$NON-NLS-1$
        FileOutputStream out = new FileOutputStream(f);
        util.exportConnectorArchive(out, ca, new Properties());
        out.close();
        
        ZipFile zip = new ZipFile(UnitTestUtil.getTestDataPath()+"/configutil/exported_sample.caf"); //$NON-NLS-1$
        ZipEntry entry = zip.getEntry("ConnectorTypes/"); //$NON-NLS-1$
        assertNotNull(entry);

        entry = zip.getEntry("Manifest.xml"); //$NON-NLS-1$
        assertNotNull(entry);
        
        entry = zip.getEntry("ConnectorTypes/sample"); //$NON-NLS-1$
        assertNotNull(entry);
        
        entry = zip.getEntry("ConnectorTypes/sample/sample.cdk");//$NON-NLS-1$
        assertNotNull(entry);
        
        entry = zip.getEntry("ConnectorTypes/sample/jdbcconn.jar");//$NON-NLS-1$
        assertNotNull(entry);
        
        entry = zip.getEntry("ConnectorTypes/sample/sampleconn.jar");//$NON-NLS-1$
        assertNotNull(entry);   
        zip.close();
        
        f.delete();
    }
    
    public void testImportSelfExportedConnectorArchive() throws Exception{
        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
        
        ConnectorBindingType type = (ConnectorBindingType)util.importComponentType(new FileInputStream(UnitTestUtil.getTestDataPath()+"/configutil/sample.cdk"), new BasicConfigurationObjectEditor(), "myexported"); //$NON-NLS-1$ //$NON-NLS-2$
        
        BasicExtensionModule ext1 = new BasicExtensionModule("jdbcconn.jar", ExtensionModule.JAR_FILE_TYPE, "Foo", ByteArrayHelper.toByteArray(new File(UnitTestUtil.getTestDataPath()+"/configutil/jdbcconn.jar"))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        BasicExtensionModule ext2 = new BasicExtensionModule("sampleconn.jar", ExtensionModule.JAR_FILE_TYPE, "Foo", ByteArrayHelper.toByteArray(new File(UnitTestUtil.getTestDataPath()+"/configutil/sampleconn.jar"))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        
        BasicConnectorArchive ca = new BasicConnectorArchive();
        ca.addConnectorType(type);
        ca.addExtensionModule(type, ext1);
        ca.addExtensionModule(type, ext2);
        
        File f = new File(UnitTestUtil.getTestDataPath()+"/configutil/exported_sample.caf"); //$NON-NLS-1$
        FileOutputStream out = new FileOutputStream(f);
        util.exportConnectorArchive(out, ca, new Properties());
        out.close();
        
        
        FileInputStream stream = new FileInputStream(f); 
                
        ConnectorArchive archive = util.importConnectorArchive(stream, new BasicConfigurationObjectEditor()); 
        stream.close();
        
        assertNotNull(archive);
        
        ConnectorBindingType type2 = archive.getConnectorTypes()[0]; 
        assertNotNull(type2);
        assertEquals("myexported", type2.getName()); //$NON-NLS-1$

        ExtensionModule[] extModules = archive.getExtensionModules(type2);
        assertEquals(2, extModules.length);
        
        assertEquals("jdbcconn.jar", extModules[0].getFullName());  //$NON-NLS-1$
        assertEquals("sampleconn.jar", extModules[1].getFullName()); //$NON-NLS-1$
        f.delete();        
    }
    
    /*
     * This test is testing a 3 connector types with 5 extension modules and some of them
     * shared. this is the layout dependencies.
     * 
     * TypeOne
     *  - ext1.jar
     *  - ext4.jar
     *  TypeTwo
     *  - ext2.jar
     *  -ext1.jar
     *  TypeThree
     *  - ext1.jar
     *  - ext2.jar
     *  - ext3.jar
     *  - ext5.jar
     *  
     *  Based on the above they should be like
     *  Manifest.xml
     *  /ConnectorTypes
     *  /ConnectorTypes/TypeOne/TypeOne.cdk
     *  /ConnectorTypes/TypeOne/ext4.jar
     *  /ConnectorTypes/TypeOne/TypeTwo.cdk
     *  /ConnectorTypes/TypeOne/TypeThree.cdk
     *  /ConnectorTypes/TypeOne/ext3.jar
     *  /ConnectorTypes/TypeOne/ext5.jar
     *  /ConnectorTypes/Shared/
     *  /ConnectorTypes/Shared/ext1.jar
     *  /ConnectorTypes/Shared/ext2.jar
     *  
     */
    public void testMultipleTypesInSingleCAFExport() throws Exception{
        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
        try {
            FileUtils.copy(UnitTestUtil.getTestDataPath()+"/configutil/sampleconn.jar", UnitTestUtil.getTestDataPath()+"/configutil/ext1.jar",  true); //$NON-NLS-1$ //$NON-NLS-2$
            FileUtils.copy(UnitTestUtil.getTestDataPath()+"/configutil/sampleconn.jar", UnitTestUtil.getTestDataPath()+"/configutil/ext2.jar",  true); //$NON-NLS-1$ //$NON-NLS-2$
            FileUtils.copy(UnitTestUtil.getTestDataPath()+"/configutil/sampleconn.jar", UnitTestUtil.getTestDataPath()+"/configutil/ext3.jar",  true); //$NON-NLS-1$ //$NON-NLS-2$
            FileUtils.copy(UnitTestUtil.getTestDataPath()+"/configutil/sampleconn.jar", UnitTestUtil.getTestDataPath()+"/configutil/ext4.jar",  true); //$NON-NLS-1$ //$NON-NLS-2$
            FileUtils.copy(UnitTestUtil.getTestDataPath()+"/configutil/sampleconn.jar", UnitTestUtil.getTestDataPath()+"/configutil/ext5.jar",  true); //$NON-NLS-1$ //$NON-NLS-2$
    
            BasicConnectorArchive ca = new BasicConnectorArchive();
            
            for (int i = 1; i <= 3; i++) {
                ConnectorBindingType type = (ConnectorBindingType)util.importComponentType(new FileInputStream(UnitTestUtil.getTestDataPath()+"/configutil/type"+i+".cdk"), new BasicConfigurationObjectEditor(), null); //$NON-NLS-1$ //$NON-NLS-2$
                ca.addConnectorType(type);
                
                String[] extModules = type.getExtensionModules();
                for (int m = 0; m < extModules.length; m++) {
                    BasicExtensionModule ext = new BasicExtensionModule(extModules[m], "Foo", ByteArrayHelper.toByteArray(new File(UnitTestUtil.getTestDataPath()+"/configutil/"+extModules[m]))); //$NON-NLS-1$ //$NON-NLS-2$ 
                    ca.addExtensionModule(type, ext);
                }
            }
        
            File f = new File(UnitTestUtil.getTestDataPath()+"/configutil/exported_multiple_types.caf"); //$NON-NLS-1$
            FileOutputStream out = new FileOutputStream(f);
            util.exportConnectorArchive(out, ca, new Properties());
            out.close();

            assertTrue("Exported File must exist", f.exists()); //$NON-NLS-1$
            
            ZipFile zip = new ZipFile(f);
            assertNotNull("1 null", zip.getEntry("ConnectorTypes/TypeOne/TypeOne.cdk")); //$NON-NLS-1$ //$NON-NLS-2$
            assertNotNull("2 null", zip.getEntry("ConnectorTypes/TypeOne/ext4.jar")); //$NON-NLS-1$ //$NON-NLS-2$
            assertNotNull("3 null", zip.getEntry("ConnectorTypes/TypeTwo/TypeTwo.cdk")); //$NON-NLS-1$ //$NON-NLS-2$
            assertNotNull("4 null", zip.getEntry("ConnectorTypes/TypeThree/TypeThree.cdk")); //$NON-NLS-1$ //$NON-NLS-2$
            assertNotNull("5 null", zip.getEntry("ConnectorTypes/TypeThree/ext3.jar")); //$NON-NLS-1$ //$NON-NLS-2$
            assertNotNull("6 null", zip.getEntry("ConnectorTypes/TypeThree/ext5.jar")); //$NON-NLS-1$ //$NON-NLS-2$
            assertNotNull("7 null", zip.getEntry("ConnectorTypes/Shared/ext1.jar")); //$NON-NLS-1$ //$NON-NLS-2$
            assertNotNull("8 null", zip.getEntry("ConnectorTypes/Shared/ext2.jar")); //$NON-NLS-1$ //$NON-NLS-2$
            assertNotNull("9 null", zip.getEntry("Manifest.xml")); //$NON-NLS-1$ //$NON-NLS-2$
            
            zip.close();             
            
            
            // now import
            FileInputStream stream = new FileInputStream(f); 
            
            ConnectorArchive archive = util.importConnectorArchive(stream, new BasicConfigurationObjectEditor()); 
            stream.close();
            
            assertNotNull(archive);
            
            ConnectorBindingType[] types = archive.getConnectorTypes();
            assertEquals(3, types.length);
            for (int i = 0; i< types.length; i++) {
                ConnectorBindingType type = types[1];
                if (type.getName().equals("TypeOne")) { //$NON-NLS-1$
                    ExtensionModule[] modules = archive.getExtensionModules(type);
                    assertEquals(2, modules.length);
                    for (int m = 0; m < modules.length; m++) {
                        assertTrue(modules[m].getFullName().equals("ext1.jar") || modules[m].getFullName().equals("ext4.jar")); //$NON-NLS-1$ //$NON-NLS-2$  
                    }
                }
                else if (type.getName().equals("TypeTwo")) { //$NON-NLS-1$
                    ExtensionModule[] modules = archive.getExtensionModules(type);
                    assertEquals(2, modules.length);
                    for (int m = 0; m < modules.length; m++) {
                        assertTrue(modules[m].getFullName().equals("ext1.jar") || modules[m].getFullName().equals("ext2.jar")); //$NON-NLS-1$ //$NON-NLS-2$  
                    }                    
                }
                else if (type.getName().equals("TypeThree")) { //$NON-NLS-1$
                    ExtensionModule[] modules = archive.getExtensionModules(type);
                    assertEquals(4, modules.length);
                    for (int m = 0; m < modules.length; m++) {
                        assertTrue(modules[m].getFullName().equals("ext1.jar") || modules[m].getFullName().equals("ext2.jar") || modules[m].getFullName().equals("ext3.jar") || modules[m].getFullName().equals("ext5.jar")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$  
                    }                                        
                }
                else {
                    fail("Unknown type"); //$NON-NLS-1$
                }                
            }
            String nl = "\n";//System.getProperty("line.separator"); //$NON-NLS-1$
            
            String manifest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+nl //$NON-NLS-1$
                    +"<DeploymentManifest>"+nl //$NON-NLS-1$
                    +"    <ConnectorTypes>"+nl //$NON-NLS-1$
                    +"        <ConnectorType Name=\"TypeOne\" File=\"ConnectorTypes/TypeOne/TypeOne.cdk\">"+nl //$NON-NLS-1$
                    +"            <ExtensionModules>"+nl //$NON-NLS-1$
                    +"                <ExtensionModule Type=\"JAR File\" File=\"ConnectorTypes/Shared/ext1.jar\" />"+nl //$NON-NLS-1$
                    +"                <ExtensionModule Type=\"JAR File\" File=\"ConnectorTypes/TypeOne/ext4.jar\" />"+nl //$NON-NLS-1$
                    +"            </ExtensionModules>"+nl //$NON-NLS-1$
                    +"        </ConnectorType>"+nl //$NON-NLS-1$
                    +"        <ConnectorType Name=\"TypeTwo\" File=\"ConnectorTypes/TypeTwo/TypeTwo.cdk\">"+nl //$NON-NLS-1$
                    +"            <ExtensionModules>"+nl //$NON-NLS-1$
                    +"                <ExtensionModule Type=\"JAR File\" File=\"ConnectorTypes/Shared/ext2.jar\" />"+nl //$NON-NLS-1$
                    +"                <ExtensionModule Type=\"JAR File\" File=\"ConnectorTypes/Shared/ext1.jar\" />"+nl //$NON-NLS-1$
                    +"            </ExtensionModules>"+nl //$NON-NLS-1$
                    +"        </ConnectorType>"+nl //$NON-NLS-1$
                    +"        <ConnectorType Name=\"TypeThree\" File=\"ConnectorTypes/TypeThree/TypeThree.cdk\">"+nl //$NON-NLS-1$
                    +"            <ExtensionModules>"+nl //$NON-NLS-1$
                    +"                <ExtensionModule Type=\"JAR File\" File=\"ConnectorTypes/Shared/ext1.jar\" />"+nl //$NON-NLS-1$
                    +"                <ExtensionModule Type=\"JAR File\" File=\"ConnectorTypes/Shared/ext2.jar\" />"+nl //$NON-NLS-1$
                    +"                <ExtensionModule Type=\"JAR File\" File=\"ConnectorTypes/TypeThree/ext3.jar\" />"+nl //$NON-NLS-1$
                    +"                <ExtensionModule Type=\"JAR File\" File=\"ConnectorTypes/TypeThree/ext5.jar\" />"+nl //$NON-NLS-1$
                    +"            </ExtensionModules>"+nl //$NON-NLS-1$
                    +"        </ConnectorType>"+nl //$NON-NLS-1$
                    +"    </ConnectorTypes>"+nl //$NON-NLS-1$
                    +"</DeploymentManifest>"+nl //$NON-NLS-1$
                    +""+nl; //$NON-NLS-1$
                
            String actualManifest = new String(archive.getManifestContents());
                        
            assertEquals(manifest, actualManifest);
            
            f.delete();
                        
        }finally {                
            FileUtils.remove(UnitTestUtil.getTestDataPath()+"/configutil/ext1.jar"); //$NON-NLS-1$
            FileUtils.remove(UnitTestUtil.getTestDataPath()+"/configutil/ext2.jar"); //$NON-NLS-1$
            FileUtils.remove(UnitTestUtil.getTestDataPath()+"/configutil/ext3.jar"); //$NON-NLS-1$
            FileUtils.remove(UnitTestUtil.getTestDataPath()+"/configutil/ext4.jar"); //$NON-NLS-1$
            FileUtils.remove(UnitTestUtil.getTestDataPath()+"/configutil/ext5.jar"); //$NON-NLS-1$
        }
    }
    
    // in this write missing extension modules
    public void testExportBadType() throws Exception {
        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
        BasicConnectorArchive ca = new BasicConnectorArchive();
        
        ConnectorBindingType type = (ConnectorBindingType)util.importComponentType(new FileInputStream(UnitTestUtil.getTestDataPath()+"/configutil/type1.cdk"), new BasicConfigurationObjectEditor(), null); //$NON-NLS-1$ 
        ca.addConnectorType(type);
        
        // should fail in this block as ext modules ext1.jar and ext4.jar not found
        BasicExtensionModule ext1 = new BasicExtensionModule("jdbcconn.jar", ExtensionModule.JAR_FILE_TYPE, "Foo", ByteArrayHelper.toByteArray(new File(UnitTestUtil.getTestDataPath()+"/configutil/jdbcconn.jar"))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        BasicExtensionModule ext2 = new BasicExtensionModule("sampleconn.jar", ExtensionModule.JAR_FILE_TYPE, "Foo", ByteArrayHelper.toByteArray(new File(UnitTestUtil.getTestDataPath()+"/configutil/sampleconn.jar"))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 

        ca.addExtensionModule(type, ext2);
        ca.addExtensionModule(type, ext1);

        try {
            File f = new File(UnitTestUtil.getTestDataPath()+"/configutil/exported_multiple_types.caf"); //$NON-NLS-1$
            FileOutputStream out = new FileOutputStream(f);
            util.exportConnectorArchive(out, ca, new Properties());
            out.close();        
            f.delete();

            // Now we want allow the incomplete caf files..
            //fail("must have failed during the export because of the invalid configuration"); //$NON-NLS-1$
        }catch(Exception e) {
         // pass   
        }
    }
    
    public void testloadConnectorBindingAndType() throws Exception {
    	    ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();
    	    ConfigurationImportExportUtility utility=  new XMLConfigurationImportExportUtility();
    	        
    	    String filename = UnitTestUtil.getTestDataPath() + File.separator+ "properties.cdk";
    	    
    	       InputStream in = new FileInputStream(filename);   	        

    	       Object[] bandt = utility.importConnectorBindingAndType(in, editor, new String[]{});
    	       
    	       if (bandt == null || bandt.length != 2) {
    	    	   fail("didnt import both, binding and type");
    	       }
    	       if ( bandt[1] instanceof ConnectorBinding) {
    	    	  
    	       } else {
    	    	   fail("Not connector binding instance");
    	       }
    	       if ( bandt[0] instanceof ConnectorBindingType) {
    	    	   
    	       } else {
    	    	   fail("No connector type instance");
    	       }
    }
    
    public void testloadConnectorBinding() throws Exception {
	    ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();
	    ConfigurationImportExportUtility utility=  new XMLConfigurationImportExportUtility();
	        
	    String filename = UnitTestUtil.getTestDataPath() + File.separator+ "properties.cdk";
	    
	       InputStream in = new FileInputStream(filename);   	        

	       ConnectorBinding cb = utility.importConnectorBinding(in, editor, null);
	       
	       if (cb == null) {
	    	   fail("didnt import binding");
	       }
	       
	       InputStream is = ObjectConverterUtil.convertToInputStream(new File(filename));
	        
	        XMLConfigurationImportExportUtility ciu = new XMLConfigurationImportExportUtility();
	       
	        ConnectorBinding cb2 = ciu.importConnectorBinding(is, new BasicConfigurationObjectEditor(false), "");

    }   
    
    public void testImportExportConfig() throws Exception {
  
            String fileToImport = UnitTestUtil.getTestDataPath()+"/config-original.xml"; //$NON-NLS-1$
            
            ConfigurationModelContainerAdapter cma = new ConfigurationModelContainerAdapter();
            
            ConfigurationModelContainer configModel = cma.readConfigurationModel(fileToImport, Configuration.NEXT_STARTUP_ID);
 
            if (configModel.getHosts().size() != 1) {
            	fail("Didnt find 1 hosts");
            }
            
            if (configModel.getConfiguration().getVMComponentDefns().size() != 1) {
            	fail("Didnt find 1 vm");
            }
            
            VMComponentDefn vm = (VMComponentDefn) configModel.getConfiguration().getVMComponentDefns().iterator().next();
            
            Collection depsvcs = configModel.getConfiguration().getDeployedServicesForVM(vm);
            
            if (depsvcs == null || depsvcs.size() != 6) {
            	fail("Didnt find 6 deployed services for VM " + vm.getName());
            }
            
            
            boolean foundprops = false;
            boolean isenabled = false;
            for (Iterator<DeployedComponent> it=depsvcs.iterator(); it.hasNext();) {
            	DeployedComponent dc = it.next();
            	if (!dc.isEnabled()) {
            		if (dc.getName().equalsIgnoreCase("QueryService")) {
            			isenabled = true;
            			BasicDeployedComponent bdc = (BasicDeployedComponent) dc;
            			bdc.setIsEnabled(true);
            		} 
            		
            	}
            	
            	Properties props = configModel.getConfiguration().getAllPropertiesForComponent(dc.getDeployedComponentDefnID());
            	
            	if (dc.getServiceComponentDefnID().getName().equalsIgnoreCase("runtimemetadataservice")) {
            		String propvalue = props.getProperty("ExceptionOnMaxRows") ;
            		if (propvalue != null && propvalue.equals("true") ) {
            			foundprops = true;
            		}
            	}
            }
            if (! isenabled) {
            	fail("Did not find the QueryService deployed service that wase enabled");
            }
            
            if (! foundprops) {
            	fail("Did not find the dependent property from deployed runtimemetadataservice");
            }
            
            String fileToExport = UnitTestUtil.getTestScratchPath() + ("/exported_config.xml");
            
            cma.writeConfigurationModel(fileToExport, configModel, "TestCase");
            
            
            // try reloading what was written to confirm
            configModel = cma.readConfigurationModel(fileToExport, Configuration.NEXT_STARTUP_ID);

            depsvcs = configModel.getConfiguration().getDeployedServicesForVM(vm);
            
            if (depsvcs == null || depsvcs.size() != 6) {
            	fail("Didnt find 6 deployed services for VM " + vm.getName());
            }

            isenabled = false;
            for (Iterator<DeployedComponent> it=depsvcs.iterator(); it.hasNext();) {
            	DeployedComponent dc = it.next();
           		if ( dc.getName().equalsIgnoreCase("QueryService")) {
           			if (!dc.isEnabled()) {
           				fail("Update to change enabled status to true did not work");
           			}
        		} 

            }
           
    	
    }
}
