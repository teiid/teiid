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

package com.metamatrix.cdk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.metamatrix.core.commandshell.CommandShell;
import com.metamatrix.core.commandshell.CommandShellTest;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.util.StringUtilities;
import com.metamatrix.core.util.UnitTestUtil;

public class TestConnectorShell extends CommandShellTest {
    private ConnectorShell connectorShell;
    private ConnectorShellCommandTarget commandTarget;
    
    public TestConnectorShell(String name) {
        super(name);
    }

    /* 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        commandTarget = new ConnectorShellCommandTarget();
        connectorShell = new ConnectorShell(commandTarget);
        connectorShell.setSilent(true);
        connectorShell.setPrintStackTraceOnException(false);
        connectorShell.setDefaultFilePath(UnitTestUtil.getTestDataPath() + File.separator); 
    }

    public void testLoad() {
        String result = loadLoopBackConnector();
        assertNull(result);
    }
    
    private String loadLoopBackConnector() {
        return connectorShell.execute("load com.metamatrix.connector.loopback.LoopbackConnector partssupplier/PartsSupplier.vdb"); //$NON-NLS-1$  
    }
    
    private void start() {
        connectorShell.execute("start"); //$NON-NLS-1$
    }
    
    public void testSelect() {
        loadLoopBackConnector();
        start();
        String result = connectorShell.execute("select * from partssupplier.partssupplier.parts;"); //$NON-NLS-1$
        assertEquals("PART_ID\tPART_NAME\tPART_COLOR\tPART_WEIGHT" + StringUtil.LINE_SEPARATOR + "ABCDEFGHIJ\tABCDEFGHIJ\tABCDEFGHIJ\tABCDEFGHIJ" + StringUtil.LINE_SEPARATOR, result); //$NON-NLS-1$  //$NON-NLS-2$
    }
    
    
    public void testRun() {
        loadLoopBackConnector();
        start();
        setScriptFile();
        connectorShell.turnOffExceptionHandling();
        String result = connectorShell.execute("run test1"); //$NON-NLS-1$
        assertEquals("Test " + UnitTestUtil.getTestDataPath() + File.separator + "script.txt.test1 failed.  CompareResults Error: Expected 1 records but received 2\n", result); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void setScriptFile() {
        setScriptFile( "script.txt" ); //$NON-NLS-1$
    }
    
    private void setScriptFile(String fileName) {
        connectorShell.execute("setScriptFile " + UnitTestUtil.getTestDataPath() + File.separator + fileName); //$NON-NLS-1$ 
    }
    
    public void testCallingScriptFromAnotherScriptFile() {
        commandTarget.setFailOnError(true);
        connectorShell.execute( "setscriptfile " + UnitTestUtil.getTestDataPath() + File.separator + "/cdk/s1.txt" ); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals( "", connectorShell.execute( "s3" ) ); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals( "", connectorShell.execute( "s1" ) ); //$NON-NLS-1$ //$NON-NLS-2$
        connectorShell.execute( "s4" ); //$NON-NLS-1$
        connectorShell.execute( "s2" ); //$NON-NLS-1$
    }
    
    public void testCallingScriptFromAnotherScriptFileThatCallsAScript() {
        commandTarget.setFailOnError(true);
        connectorShell.setDefaultFilePath(""); //$NON-NLS-1$
        connectorShell.execute( "setscriptfile " + UnitTestUtil.getTestDataPath() + File.separator + "/cdk/s3.txt" ); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals( "", connectorShell.execute( "s3_3" ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRunAllTests() {
        loadLoopBackConnector();
        start();
        setScriptFile();
        String result = connectorShell.execute("runAll"); //$NON-NLS-1$
        assertEquals("Test " + UnitTestUtil.getTestDataPath() + File.separator + "script.txt.test1 failed.  CompareResults Error: Expected 1 records but received 2\nTest " + UnitTestUtil.getTestDataPath() + File.separator +"script.txt.testBadData failed.  CompareResults Error: Value mismatch at row 1 and column 2: expected = XX, actual = ABCDEFGHIJ\nTests run: 16 test failures: 2", result); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testSetProperty() {
        connectorShell.setDefaultFilePath(UnitTestUtil.getTestDataPath() + File.separator); 
        setScriptFile();
        String result = connectorShell.execute("run testSetProperty"); //$NON-NLS-1$
        assertEquals("", result); //$NON-NLS-1$ 
    }
    
    public void testSetPropertyAfterConnectorHostStart() {
        connectorShell.setDefaultFilePath(UnitTestUtil.getTestDataPath() + File.separator); 
        setScriptFile();
        start();
        String result = connectorShell.execute("run testSetPropertyAfterConnectorHostStart"); //$NON-NLS-1$
        assertEquals("", result); //$NON-NLS-1$ 
    }
    
    public void testLoadingClearsProperties() {
        connectorShell.setDefaultFilePath(UnitTestUtil.getTestDataPath() + File.separator); 
        setScriptFile();
        String result = connectorShell.execute("run testLoadingClearsProperties"); //$NON-NLS-1$
        assertEquals("", result); //$NON-NLS-1$ 
    }
    
    public void testRunAllTestsWithoutSetup() {
        loadLoopBackConnector();
        start();
        setScriptFile("scriptWithoutSetup.txt"); //$NON-NLS-1$
        String result = connectorShell.execute("runAll"); //$NON-NLS-1$
        assertEquals("Test " + UnitTestUtil.getTestDataPath() + File.separator + "scriptWithoutSetup.txt.test1 failed.  CompareResults Error: Expected 1 records but received 2\nTests run: 3 test failures: 1", result); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCaseInsensitiveCommands() {
        loadLoopBackConnector();
        setScriptFile();
        String result = connectorShell.execute("run testCaseInsensitiveCommands"); //$NON-NLS-1$
        assertEquals("", result); //$NON-NLS-1$
    }
    
    public void testInvokingAsCommandLine() {
        new ConnectorShell(new ConnectorShellCommandTarget()).run(new String[] {"quit"}, UnitTestUtil.getTestScratchPath() + "/connector_shell.log"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    
    /* 
     * @see com.metamatrix.core.commandshell.CommandShellTest#getCommandShell()
     */
    protected CommandShell getCommandShell() {
        return connectorShell;
    }
    
    public void testTabsInResults() {
        runTest("testTabsInResults"); //$NON-NLS-1$
    }

    public void testLoadArchive() {
        runTest("testLoadArchive"); //$NON-NLS-1$
    }

    public void testCreateArchive() {        
        File f = UnitTestUtil.getTestScratchFile("foo.caf");
        f.delete();
        connectorShell.execute("createArchive "+UnitTestUtil.getTestScratchPath()+"/foo.caf "+UnitTestUtil.getTestDataPath()+"/sample/sample2.cdk "+UnitTestUtil.getTestDataPath()+"/sample");
        assertTrue("failed to create archive file", f.exists());
    }
    
    public void testBracketsInResult() {
        runTest("testBracketsInResult"); //$NON-NLS-1$
    }

    public void testHelp() {
        runTest("testHelp"); //$NON-NLS-1$
    }

    public void testNotLoadingConnector() {
        runTest("testNotLoadingConnector"); //$NON-NLS-1$
    }     

    public void testStart() {
        loadLoopBackConnector();
        connectorShell.execute("start"); //$NON-NLS-1$
    }
    
    public void testStop() {
        loadLoopBackConnector();
        start();
        connectorShell.execute("stop"); //$NON-NLS-1$
    }
    
    public void testLoadXMLProperties() {
        loadLoopBackConnector();
        connectorShell.execute("loadProperties " + UnitTestUtil.getTestDataPath() + File.separator+ "properties.cdk"); //$NON-NLS-1$ //$NON-NLS-2$
        start();
        String loadedProperties = connectorShell.execute("getProperties"); //$NON-NLS-1$
        String expected = new StringBuffer("property1=PropVal1") //$NON-NLS-1$
                            .append(StringUtilities.LINE_SEPARATOR)
                            .append("property2=PropVal2") //$NON-NLS-1$
                            .append(StringUtilities.LINE_SEPARATOR)
                            .append("property3=com.metamatrix.cdk.propertyVal") //$NON-NLS-1$
                            .append(StringUtilities.LINE_SEPARATOR)
                            .toString();
        assertEquals(expected, loadedProperties);
    }
    
    public void testLoadPropertiesFromPropertiesFile() {
        loadLoopBackConnector();
        connectorShell.execute("loadProperties " + UnitTestUtil.getTestDataPath() + File.separator+ "/cdk/connector.properties"); //$NON-NLS-1$ //$NON-NLS-2$
        start();
        String loadedProperties = connectorShell.execute("getProperties"); //$NON-NLS-1$
        assertNotNull(loadedProperties);
        String expected = new StringBuffer("property1=PropVal1") //$NON-NLS-1$
                            .append(StringUtilities.LINE_SEPARATOR)
                            .append("property2=PropVal2") //$NON-NLS-1$
                            .append(StringUtilities.LINE_SEPARATOR)
                            .append("property3=com.metamatrix.cdk.propertyVal") //$NON-NLS-1$
                            .append(StringUtilities.LINE_SEPARATOR)
                            .toString();
        assertEquals(expected, loadedProperties);
    }
    
    
    public void testCreateTemplateFileDoesNotExistNoPath() throws FileNotFoundException, IOException {
        //Create a template file.  The file does not exist yet.  The filename does not include a path.
        runTemplateTestAndDeleteFile("testTemplate1");  //$NON-NLS-1$
    }
    
    public void testCreateTemplateFileDoesNotExist() throws FileNotFoundException, IOException {
        //Create a template file.  The file does not exist yet.  The filename does not include a path.
        runTemplateTestAndDeleteFile("testTemplate2");  //$NON-NLS-1$
    }
    
    private void runTemplateTestAndDeleteFile(String fileName) throws FileNotFoundException, IOException {
    	fileName = UnitTestUtil.getTestScratchPath() + File.separator + fileName;
        new File(fileName).delete();
        runTemplateTest(fileName);
    }
    
    private void runTemplateTest(String tempFileName) throws FileNotFoundException, IOException {
        connectorShell.execute("setFailOnError true");  //$NON-NLS-1$
        connectorShell.execute("createTemplate " + tempFileName); //$NON-NLS-1$

        FileInputStream template = new FileInputStream(new File(tempFileName));
        InputStream original = getClass().getResourceAsStream("Template.cdk"); //$NON-NLS-1$
        int originalByte= 0, tempByte = 0;
        int bytesCompared = 0;
        while((originalByte = original.read()) != -1 && (tempByte = template.read()) != -1) {            
            assertEquals(originalByte, tempByte);
            bytesCompared++;
        }
        assertTrue(bytesCompared>0);
        // Ensure that the end of the master template has been reached        
        assertEquals(-1, originalByte);
        template.close();
        original.close();
    }
}
