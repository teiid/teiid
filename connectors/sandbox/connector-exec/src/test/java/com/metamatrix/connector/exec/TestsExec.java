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
package com.metamatrix.connector.exec;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ResultSetExecution;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestsExec extends TestCase {
    
    private List expectedResults;
    private String command;
    private FakeExecConnector connector;
    
    private String vdbFile = UnitTestUtil.getTestDataPath() + File.separator +"Exec.vdb"; //$NON-NLS-1$ 
        
    
    public TestsExec(String name) {
        super(name);
        
    }
    
    public void tearDown(){
        if(this.connector != null){   
            this.connector.stop();
            this.connector = null;
        }
        expectedResults = null;
    }

    private void executeCommand(Properties props) throws ConnectorException{
        ConnectorHost host = new ConnectorHost(new FakeExecConnector(), props, vdbFile, false);
        List results = host.executeCommand(command); //$NON-NLS-1$
        if (results != null && !results.isEmpty()) {
            for (Iterator it=results.iterator(); it.hasNext();) {
                it.next();
            }
        }
        
        //     assertEquals(expectedResults, results);
    }

    private void executeCommand() throws ConnectorException{
        executeCommand(getConnectorProperties());
    }
    
    private Properties getConnectorProperties()
    {
        Properties props = new Properties();
        props.setProperty("win.executable", "cmd.exe");//$NON-NLS-1$ //$NON-NLS-2$
        
        props.setProperty("CapabilitiesClass", "com.metamatrix.connector.exec.ExecCapabilities"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("ConnectorClass", "com.metamatrix.connector.exec.ExecConnnector"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("ServiceClassName", "com.metamatrix.server.connector.service.ConnectorService"); //$NON-NLS-1$ //$NON-NLS-2$
        
         
        return props;
    }

    //actual tests
    public void testDIRCommand() throws Exception{
        command = "Select returnvalue From ExecModel.Exec where command='dir'"; //$NON-NLS-1$
        expectedResults = new ArrayList();
        List row1 = new ArrayList();
        row1.add("filename"); //$NON-NLS-1$
        expectedResults.add(row1);
        executeCommand();
    }  
    
    public void testExclusion() throws Exception{
        command = "Select returnvalue From ExecModel.Exec where command='rm'"; //$NON-NLS-1$
        expectedResults = new ArrayList();
        List row1 = new ArrayList();
        row1.add("filename"); //$NON-NLS-1$
        expectedResults.add(row1);
        Properties prop = getConnectorProperties();
        String f = UnitTestUtil.getTestDataPath() + File.separator+ "exclusionFile.properties"; //$NON-NLS-1$
        prop.setProperty("exclusionFile", f);//$NON-NLS-1$
        prop.setProperty("ConnectorClass", "com.metamatrix.connector.exec.TestExecConnnector"); //$NON-NLS-1$ //$NON-NLS-2$

        try {
            executeCommand(prop);
            fail("rm was in the exclusion file, should not have run"); //$NON-NLS-1$
        } catch (ConnectorException ce) {
            //System.out.print("Passed: " + ce.getMessage()); //$NON-NLS-1$
        }
    }      
    
    

    class CancelThread extends Thread{
        private ResultSetExecution execution;

        CancelThread(ResultSetExecution execution){
            this.execution = execution;
        }
        public void run(){
            try {
                Thread.sleep(500);
                execution.cancel();
            } catch (ConnectorException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
