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

package com.metamatrix.dqp.embedded.services;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.model.BasicConnectorBinding;
import com.metamatrix.common.config.model.BasicConnectorBindingType;
import com.metamatrix.common.config.model.BasicExtensionModule;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.EmbeddedTestUtil;
import com.metamatrix.dqp.embedded.configuration.VDBConfigurationReader;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;


/** 
 * Test class for FileConfigurationService
 * @since 4.3
 */
public class TestEmbeddedConfigurationService extends TestCase {
    EmbeddedConfigurationService service =  null;
       
    protected void setUp() throws Exception {
    	EmbeddedTestUtil.createTestDirectory();
        service = new EmbeddedConfigurationService();  
        ApplicationEnvironment env = new ApplicationEnvironment();
        service.start(env);
        env.bindService(DQPServiceNames.CONFIGURATION_SERVICE, service);
    }

    protected void tearDown() throws Exception {
        service.stop();
        FileUtils.removeDirectoryAndChildren(service.getDeployDir());
    }

    public void testUseExtensionPath() throws Exception {
        service.setUserPreferences(EmbeddedTestUtil.getProperties()); 
        assertTrue(service.useExtensionClasspath());
    }
    
    public void testUseExtensionPathFalse()  throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(); 
        p.remove("dqp.extensions"); //$NON-NLS-1$        
        service.setUserPreferences(p);        
        assertFalse(service.useExtensionClasspath());
    }
    
    public void testGetAvailableVDBFiles() throws Exception {
        service.setUserPreferences(EmbeddedTestUtil.getProperties());
        HashMap vdbFiles = VDBConfigurationReader.loadVDBS(service.getVDBLocations(), service.getDeployDir());
        int count = vdbFiles.keySet().size();
        assertEquals(2, count);   
        // admin.vdb is ignored because it did not have any models
        for (Iterator i = vdbFiles.keySet().iterator(); i.hasNext();){                                           
            URL vdbURL = (URL)i.next();            
            String path = vdbURL.getPath();
            if (!path.endsWith("QT_Ora9DS.vdb") && !path.endsWith("Admin.vdb")) { //$NON-NLS-1$ //$NON-NLS-2$
                fail("wrong file found"); //$NON-NLS-1$
            }
        }        
    }
    
    public void testGetConfigFileURL() throws Exception {
        service.setUserPreferences(EmbeddedTestUtil.getProperties()); 
        assertTrue(service.getConfigFile().toString().endsWith("dqp/config/ServerConfig.xml"));            //$NON-NLS-1$
    }
        
    public void testGetAlternateBinding2() throws Exception {
        BasicConnectorBinding binding = new BasicConnectorBinding(new ConfigurationID("foo"), new ConnectorBindingID(new ConfigurationID("foo"), "foo"), new ComponentTypeID("foo type")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        BasicConnectorBinding alternatebinding = new BasicConnectorBinding(new ConfigurationID("foo"), new ConnectorBindingID(new ConfigurationID("foo"), "foo"), new ComponentTypeID("foo type")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        Properties p = EmbeddedTestUtil.getProperties();    
        service.setUserPreferences(p);
        service.loadedConnectorBindings.put("foo", alternatebinding); //$NON-NLS-1$
                
        // we did not set use alternate.
        assertTrue(service.getConnectorBinding(binding.getFullName())==alternatebinding);
    }    
    
    public void testGetDefaultExtensionPath()  throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(); 
        p.remove(DQPEmbeddedProperties.DQP_EXTENSIONS);
        service.setUserPreferences(p);        
        assertTrue(service.getExtensionPath()[0].toString().endsWith("dqp/extensions/")); //$NON-NLS-1$
    }
    
    public void testGetDirectoryToStoreVDBS() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);  
        assertTrue(service.getVDBSaveLocation().toString().endsWith("dqp/config/QT_Ora9DS.vdb")); //$NON-NLS-1$
    }

    public void testGetDirectoryToStoreVDBSByVDBName() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(); 
        p.setProperty(DQPEmbeddedProperties.VDB_DEFINITION, "./config/QT_Ora9DS.vdb"); //$NON-NLS-1$
        service.setUserPreferences(p);  
        service.initializeService(p);
        
        VDBArchive vdb = service.getVDB("QT_Ora9DS", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        vdb.getConfigurationDef().setName("Foo"); //$NON-NLS-1$
        vdb.getConfigurationDef().setVersion("2"); //$NON-NLS-1$
        assertTrue(service.getNewVDBLocation(vdb).toString().endsWith("dqp/config/Foo_2.vdb")); //$NON-NLS-1$
    }
    
    public void testGetFileToSaveNewFile() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        p.setProperty(DQPEmbeddedProperties.VDB_DEFINITION, "./config/QT_Ora9DS.vdb"); //$NON-NLS-1$
        service.initializeService(p);
        
        VDBArchive vdb = service.getVDB("QT_Ora9DS", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        vdb.getConfigurationDef().setName("Foo"); //$NON-NLS-1$
        vdb.getConfigurationDef().setVersion("2"); //$NON-NLS-1$
        URL f = service.getNewVDBLocation(vdb);
        assertTrue(f.toString().endsWith("dqp/config/Foo_2.vdb")); //$NON-NLS-1$
    }
    
    public void testGetFullyQualifiedPath() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);       
        assertTrue(service.getFullyQualifiedPath("http://lib/foo.txt").toString().endsWith("http://lib/foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(service.getFullyQualifiedPath("file:/c:/lib/foo.txt").toString().endsWith("file:/c:/lib/foo.txt"));//$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(service.getFullyQualifiedPath("/lib/foo.txt").toString().endsWith("mmfile:/lib/foo.txt"));//$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(service.getFullyQualifiedPath("../lib/foo.txt").toString().endsWith("mmfile:"+UnitTestUtil.getTestScratchPath()+"/lib/foo.txt"));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertTrue(service.getFullyQualifiedPath("lib/foo.txt").toString().endsWith("mmfile:"+UnitTestUtil.getTestScratchPath()+"/dqp/lib/foo.txt"));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertTrue(service.getFullyQualifiedPath("./lib/foo.txt").toString().endsWith("mmfile:"+UnitTestUtil.getTestScratchPath()+"/dqp/lib/foo.txt"));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertTrue(service.getFullyQualifiedPath("c:\\lib\\foo.txt").toString().endsWith("mmfile:/c:/lib/foo.txt"));//$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(service.getFullyQualifiedPath("c:/lib/foo.txt").toString().endsWith("mmfile:/c:/lib/foo.txt"));//$NON-NLS-1$ //$NON-NLS-2$              
    }
    
    public void testGetNextVdbVersion() throws Exception{        
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);        
        assertEquals(2, service.getNextVdbVersion("QT_Ora9DS")); //$NON-NLS-1$
    }    
    
    public void testDeleteInUseConnectorBinding() throws Exception{        
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);         
        try {
            service.deleteConnectorBinding("BQT2 Oracle 9i Simple Cap"); //$NON-NLS-1$
            fail("Should not delete a binding in use.."); //$NON-NLS-1$
        }catch(MetaMatrixComponentException e) {
            // pass
        }        
    }

    public void testDeleteNonExistingConnectorBinding() throws Exception{        
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);         
        try {
            service.deleteConnectorBinding("UNKNOWN"); //$NON-NLS-1$
            fail("Should not delete a binding UNKNOWN"); //$NON-NLS-1$
        }catch(MetaMatrixComponentException e) {
            // pass
        }        
    }    
    
    public void testDeleteConnectorBinding() throws Exception{        
        Properties p = EmbeddedTestUtil.getProperties();
        service.setUserPreferences(p);
        service.initializeService(p);         
        
        service.deleteVDB(service.getVDB("QT_Ora9DS", "1")); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            service.deleteConnectorBinding("QT_ORA9DS_1.BQT2 Oracle 9i Simple Cap"); //$NON-NLS-1$
            fail("must have failed because, delete of VDB should delete the binding too"); //$NON-NLS-1$
        }catch(Exception e) {
            // pass
        }
        assertNull(service.getConnectorBinding("QT_ORA9DS_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$
    }    
    
    
    public void testDeleteConnectorType() throws Exception{        
        Properties p = EmbeddedTestUtil.getProperties();
        service.setUserPreferences(p);
        service.initializeService(p);         
        
        try {
            service.deleteConnectorType("UNKNOWN"); //$NON-NLS-1$
        }catch(MetaMatrixComponentException e){
            //pass
        }
        
        assertNotNull(service.getConnectorType("File XMLSource Connector"));//$NON-NLS-1$
        service.deleteConnectorType("File XMLSource Connector"); //$NON-NLS-1$
        
        try {
            service.deleteConnectorType("File XMLSource Connector"); //$NON-NLS-1$
        }catch(MetaMatrixComponentException e){
            //pass
        }               
    }     
    
    public void testDeleteConnectorTypeInUse() throws Exception{        
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);         
        
        assertNotNull(service.getConnectorType("Oracle ANSI JDBC Connector"));//$NON-NLS-1$
        
        try {
            service.deleteConnectorType("Oracle ANSI JDBC Connector"); //$NON-NLS-1$
            fail("Must have failed in deleteing the in use connector type"); //$NON-NLS-1$
        }catch(MetaMatrixComponentException e){
            //pass
        }               
    }     
    
    public void testDeleteVDB() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);         
        
        VDBArchive vdb = service.getVDB("QT_Ora9DS", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        assertNotNull(vdb); 
        
        service.deleteVDB(vdb);
        
        assertNull(service.getVDB("QT_Ora9DS", "1")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetSystemProperties() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);         

        Properties sp = service.getSystemProperties();
        
        assertEquals("50", sp.getProperty("MaxCodeTables")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("100", sp.getProperty("MaxPlanCacheSize")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("20000", sp.getProperty("MaxFetchSize")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetSystemConfiguration() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties();       
        service.setUserPreferences(p);         
        assertNull(service.configurationModel);
        
        service.getSystemConfiguration();
        assertNotNull(service.configurationModel);
    }
    
    public void testGetSystemVDB() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);  
        assertNotNull(service.getSystemVdb());
    }
    
    public void testUDF() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties(); 
        p.setProperty(DQPEmbeddedProperties.USER_DEFINED_FUNCTIONS, "extensionjar:foo.xmi"); //$NON-NLS-1$
        p.setProperty("dqp.extensions", "./foo/;./bar/"); //$NON-NLS-1$ //$NON-NLS-2$
        service.setUserPreferences(p);
        
        assertNull(service.getUDFFile());
        
        File f = new File("target/scratch/dqp/bar"); //$NON-NLS-1$
        f.mkdirs();
        File xmiFile = new File("target/scratch/dqp/bar/foo.xmi"); //$NON-NLS-1$
        FileWriter fw = new FileWriter(xmiFile); 
        fw.write("testing extension modules"); //$NON-NLS-1$
        fw.flush();
        fw.close();
        
        
        assertTrue(service.getUDFFile().toString().endsWith(UnitTestUtil.getTestScratchPath()+"/dqp/bar/foo.xmi")); //$NON-NLS-1$
        
        p.setProperty(DQPEmbeddedProperties.USER_DEFINED_FUNCTIONS, xmiFile.toURL().toString());
        assertEquals(xmiFile.toURL().toString(), service.getUDFFile().toString()); 
        
        xmiFile.delete();
        
        // now look for the default
        p.remove(DQPEmbeddedProperties.USER_DEFINED_FUNCTIONS);
        assertNull(service.getUDFFile());

        service.saveExtensionModule(new BasicExtensionModule(ConfigurationService.USER_DEFINED_FUNCTION_MODEL, "adding extension module", "xmi", "testing extension modules".getBytes())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$        
        
        assertTrue(service.getUDFFile().toString().endsWith("/dqp/foo/"+ConfigurationService.USER_DEFINED_FUNCTION_MODEL)); //$NON-NLS-1$
        
        f.delete();
    }
    
    public void testGetVDBs() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);
        assertTrue(service.getVDBs().size() == 2); 
    }
    
    public void testSaveConnectorBinding() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties();      
        service.setUserPreferences(p);
        service.initializeService(p);
        
        String msg = "Test update of the connector binding"; //$NON-NLS-1$
        BasicConnectorBinding binding = (BasicConnectorBinding)service.getConnectorBinding("QT_ORA9DS_1.BQT2 Oracle 9i Simple Cap"); //$NON-NLS-1$
        binding.setDescription(msg);
        
        service.saveConnectorBinding(binding.getFullName(), binding, true);
        
        // Test and make sure the VDB updated
        VDBArchive vdb = service.getVDB("QT_Ora9DS", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        VDBDefn def = vdb.getConfigurationDef();
        
        Map bindings = def.getConnectorBindings();
        ConnectorBinding savedBinding = (ConnectorBinding)bindings.get("BQT2 Oracle 9i Simple Cap"); //$NON-NLS-1$
        assertEquals(msg, savedBinding.getDescription());
        
        // Test and make sure the server config updated        
        Collection configBindings = service.getSystemConfiguration().getConfiguration().getConnectorBindings();
        Iterator it = configBindings.iterator();
        while(it.hasNext()) {
            ConnectorBinding b = (ConnectorBinding)it.next();
            if (b.getFullName().equals("BQT2 Oracle 9i Simple Cap")) { //$NON-NLS-1$
                assertEquals(msg, b.getDescription());
            }
        }         
    }
    

    public void testSaveConnectorType() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties();    
        service.setUserPreferences(p);
        service.initializeService(p);
        
        String msg = "Test update of the connector type"; //$NON-NLS-1$
        
        BasicConnectorBindingType type = (BasicConnectorBindingType)service.getConnectorType("File XMLSource Connector"); //$NON-NLS-1$
        type.setDescription(msg);
        service.saveConnectorType(type);
        
        assertEquals(msg, service.getConnectorType("File XMLSource Connector").getDescription()); //$NON-NLS-1$
        
        Collection componentTypes = service.getSystemConfiguration().getComponentTypes().values();
        for (Iterator i = componentTypes.iterator(); i.hasNext();) {
            ComponentType t = (ComponentType)i.next();
            if (t.getComponentTypeCode() == ComponentType.CONNECTOR_COMPONENT_TYPE_CODE) {
                if (t.getFullName().equals("File XMLSource Connector")) { //$NON-NLS-1$
                    assertEquals(msg, t.getDescription()); 
                }
            }
        }
    }

    
    public void testSaveVDB() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);
        
        VDBArchive vdb = service.getVDB("QT_Ora9DS", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        service.saveVDB(vdb, ConfigurationService.NEXT_VDB_VERSION);
        assertEquals("QT_Ora9DS", vdb.getName()); //$NON-NLS-1$
        assertEquals("2", vdb.getVersion()); //$NON-NLS-1$
        
        assertNotNull(service.getVDB("QT_Ora9DS", "2")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetProcessorBatchSize() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties();    
        service.setUserPreferences(p);
        service.initializeService(p);
        
        assertEquals("3867", service.getProcessorBatchSize()); //$NON-NLS-1$
    }
    
    public void testGetConnectorBatchSize() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties();    
        service.setUserPreferences(p);
        service.initializeService(p);
        
        assertEquals("3868", service.getConnectorBatchSize()); //$NON-NLS-1$
    }
    
    public void testLoadedConnectorBindings() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);
        
        assertEquals("There shlould be only three connectors", 3, service.getConnectorBindings().size()); //$NON-NLS-1$

        for (Iterator i = service.getConnectorBindings().iterator(); i.hasNext();) {
            ConnectorBinding binding = (ConnectorBinding)i.next();
            assertTrue("QT_ORA9DS_1.BQT1 Oracle 9i Simple Cap|QT_ORA9DS_1.BQT2 Oracle 9i Simple Cap".indexOf(binding.getDeployedName()) != -1); //$NON-NLS-1$
        }            
        
        int count = 0;
        for (Iterator i = service.loadedConnectorBindings.keySet().iterator(); i.hasNext();) {
            String binding = (String)i.next();
            if (binding.startsWith("QT_ORA9DS_1.")) //$NON-NLS-1$
                count++;
        }
        // thers should be at least oneof the connectors started with vdb name
        assertEquals(2, count);
    }    
    
    public void testAddConnectorBinding() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(); 
        service.setUserPreferences(p);
        service.initializeService(p);
        
        ConnectorBinding binding = service.getConnectorBinding("QT_ORA9DS_1.BQT1 Oracle 9i Simple Cap"); //$NON-NLS-1$
        assertNotNull(binding);
        List<VDBArchive> vdbs = service.getVdbsThatUseConnectorBinding(binding.getDeployedName());
        assertEquals(1, vdbs.size());
        assertEquals("QT_Ora9DS", vdbs.get(0).getName()); //$NON-NLS-1$
    }
    
    public void testGetConnectorTypes() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties();      
        service.setUserPreferences(p);
        service.initializeService(p);
        
        assertEquals(23, service.loadedConnectorTypes.size());
    }
    
    public void testUseMultipleExtensionPath()  throws Exception {
        Properties p = EmbeddedTestUtil.getProperties();
        p.setProperty("dqp.extensions", "/foo/;../x/bar/"); //$NON-NLS-1$ //$NON-NLS-2$
        service.setUserPreferences(p);        
        assertEquals("mmfile:/foo/", service.getExtensionPath()[0].toExternalForm()); //$NON-NLS-1$
        assertEquals("mmfile:target/scratch/x/bar/", service.getExtensionPath()[1].toExternalForm()); //$NON-NLS-1$
    }    
    
    public void testGetExtensionModule()  throws Exception {
        Properties p = EmbeddedTestUtil.getProperties();
        
        File f = new File("target/scratch/dqp/bar"); //$NON-NLS-1$
        f.mkdirs();
        FileWriter fw = new FileWriter("target/scratch/dqp/bar/extfile.jar"); //$NON-NLS-1$
        fw.write("testing extension modules"); //$NON-NLS-1$
        fw.flush();
        fw.close();
        
        p.setProperty("dqp.extensions", "./foo/;./bar/"); //$NON-NLS-1$ //$NON-NLS-2$
        service.setUserPreferences(p);
        
        // get all the modules in the system.
        List<ExtensionModule> modules = service.getExtensionModules();
        assertEquals("extfile.jar", modules.get(0).getID().getFullName()); //$NON-NLS-1$
        assertEquals("testing extension modules", new String(modules.get(0).getFileContents())); //$NON-NLS-1$
        
        // get individual module
        ExtensionModule m = service.getExtensionModule("extfile.jar"); //$NON-NLS-1$
        assertEquals("testing extension modules", new String(m.getFileContents())); //$NON-NLS-1$
        
        // test adding of the extension module
        service.saveExtensionModule(new BasicExtensionModule("added-ext.jar", "adding extension module", "jar", "testing extension modules".getBytes())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        modules = service.getExtensionModules();
        assertEquals(2, modules.size());
        
        m = service.getExtensionModule("added-ext.jar"); //$NON-NLS-1$
        assertEquals("testing extension modules", new String(m.getFileContents())); //$NON-NLS-1$
        
        modules = service.getExtensionModules();
        assertEquals(2,  modules.size()); 
        
        // test common class path; also makes sure that the connect in position (1) has the newly added module
        service.getUserPreferences().setProperty("dqp.extension.CommonClasspath", "extensionjar:added-ext.jar;extensionjar:extfile.jar"); //$NON-NLS-1$ //$NON-NLS-2$
        URL[] urls  = service.getClassLoaderManager().parseURLs(service.getClassLoaderManager().getCommonExtensionClassPath());
        assertEquals("mmfile:target/scratch/dqp/foo/added-ext.jar", urls[0].toString()); //$NON-NLS-1$
        assertEquals("mmfile:target/scratch/dqp/bar/extfile.jar", urls[1].toString()); //$NON-NLS-1$
        
        // test delete 
        service.deleteExtensionModule("added-ext.jar"); //$NON-NLS-1$
        modules = service.getExtensionModules();
        assertEquals(1,  modules.size()); 
        
        // test for non-existent module
        try {
			m = service.getExtensionModule("added-ext.jar"); //$NON-NLS-1$
			fail("must have failed to find"); //$NON-NLS-1$
		} catch (MetaMatrixComponentException e) {
		}
		
		f.delete();
    }      
    
    public void testCommonExtensionPath() throws Exception{
        Properties p = EmbeddedTestUtil.getProperties();
        File f = new File("target/scratch/dqp/bar"); //$NON-NLS-1$
        f.mkdirs();
        FileWriter fw = new FileWriter("target/scratch/dqp/bar/extfile.jar"); //$NON-NLS-1$
        fw.write("testing extension modules"); //$NON-NLS-1$
        fw.flush();
        fw.close();
        
        fw = new FileWriter("target/scratch/dqp/bar/driver.jar"); //$NON-NLS-1$
        fw.write("testing extension modules"); //$NON-NLS-1$
        fw.flush();
        fw.close();        
        
        p.setProperty("dqp.extensions", "./foo/;./bar/"); //$NON-NLS-1$ //$NON-NLS-2$
        service.setUserPreferences(p);
        
        assertEquals("", service.getClassLoaderManager().getCommonExtensionClassPath()); //$NON-NLS-1$
        
        p.setProperty(DQPEmbeddedProperties.COMMON_EXTENSION_CLASPATH, "extensionjar:extfile.jar"); //$NON-NLS-1$
               
        URL[] urls  = service.getClassLoaderManager().parseURLs(service.getClassLoaderManager().getCommonExtensionClassPath());
        assertEquals("mmfile:target/scratch/dqp/bar/extfile.jar", urls[0].toString()); //$NON-NLS-1$
    }
}
