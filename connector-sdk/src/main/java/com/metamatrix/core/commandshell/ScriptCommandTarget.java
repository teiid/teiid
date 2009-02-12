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

package com.metamatrix.core.commandshell;

import java.util.Arrays;
import java.util.List;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.util.StringUtil;

/**
 * Base command target class with scripting support.
 */
public class ScriptCommandTarget extends CommandTarget implements StringUtil.Constants {
    protected static final String TEST_PREFIX = "test"; //$NON-NLS-1$

    protected int testFailureCount = 0;

    protected int testCount = 0;

    private ScriptFileNameStack scriptFileNames = new ScriptFileNameStack();
    
    private boolean runningSetUp = false;
    
    public void setScriptFile(String scriptFileName) {
        scriptFileNames.setDefaultScriptFileName(scriptFileName);
    }

    protected static final String SETUP_SCRIPT = "setUp"; //$NON-NLS-1$

    protected boolean hasSetupScript(String fileName) {
        String[] scriptNames = shell.getScriptNames(fileName);
        List scriptNameList = Arrays.asList(scriptNames);
        return scriptNameList.contains(SETUP_SCRIPT);
    }

    protected StringBuffer testFailureMessage;

    protected void runningScript(String fileName) {
        scriptFileNames.usingScriptFile(fileName);
    }
        
    public String run(String scriptName) {
        if (scriptFileNames.hasDefaultScriptFileBeenSet()) {
            return runScriptDirect(scriptFileNames.getUnexpandedCurrentScriptFileName(), scriptName);
        }
        throw new NoScriptFileException();
    }

    public String runScript(String fileName, String scriptName) {
        return runScriptDirect( fileName, scriptName );
    }

    private String runScriptDirect(String originalFileName, String scriptName) {
        String fileName = scriptFileNames.expandScriptFileName(originalFileName);
        testFailureMessage = new StringBuffer();
        if (!runningSetUp) {
            if (hasSetupScript(fileName)) {
                try {
                    runningSetUp = true;
                    runScript(fileName, SETUP_SCRIPT, null, null); 
                } finally {
                    runningSetUp = false;
                }
            }
        }
        runScript(fileName, scriptName, null, getScriptResultListener());
        return testFailureMessage.toString();
    }

    void runScript(String fileName, String scriptName, StringBuffer transcript, ScriptResultListener listener) {
        scriptFileNames.startingScriptFromFile(fileName);
        try {
            shell.runScript(fileName, scriptName, transcript, listener);
        } finally {
            scriptFileNames.finishedScript();
        }
    }

    private ScriptResultListener getScriptResultListener() {
        return new ScriptResultListener() {
            public void scriptResults(String scriptFileName, String scriptName, String expected, String actual) {
                ScriptCommandTarget.this.scriptResults(scriptFileName, scriptName, expected, actual);
            }
        };
    }

    private void scriptResults(String scriptFileName, String scriptName, String expected, String actual) {
        if(expected.equals(actual)) {
        } else {
            testFailureCount++;
            String diffString = ConnectorResultUtility.compareResultsStrings(expected, actual);
            testFailureMessage.append(CorePlugin.Util.getString("ScriptCommandTarget.Test_{0}.{1}_failed.{2}_2", new Object[] {scriptFileName, scriptName, diffString}) ); //$NON-NLS-1$
            testFailureMessage.append(NEW_LINE);
        }
    }

    protected void resetTestStatistics() {
        testCount = 0;
        testFailureCount = 0;
        testFailureMessage = new StringBuffer();
    }

    public String runAll() {
        resetTestStatistics();
        String fileName = scriptFileNames.expandScriptFileName(scriptFileNames.getUnexpandedCurrentScriptFileName());
        String[] scriptNames = shell.getScriptNames( fileName );
        boolean runSetup = hasSetupScript( fileName );
        for (int i=0; i<scriptNames.length; i++) {
            if (scriptNames[i].startsWith(TEST_PREFIX)){
                testCount++;
                if (runSetup) {
                    runScript(fileName, SETUP_SCRIPT, null, null); 
                    runSetup = false;
                }
                runScript(fileName, scriptNames[i], null, getScriptResultListener());
            }
        }   
        return getTestSummary();
    }

    protected String getTestSummary() {
        return testFailureMessage.toString() + CorePlugin.Util.getString("ScriptCommandTarget.Tests_run__{0}_test_failures__{1}_1", new Object[] {new Integer(testCount), new Integer(testFailureCount)}); //$NON-NLS-1$
    }

    public void runRep(int repCount, String scriptName) {
        for (int i=0; i<repCount; i++) {
            run(scriptName);
        }
    }

    public void setSilent(boolean silent) {
        shell.setSilent(silent);
    }
    
    protected Object clone() throws CloneNotSupportedException {
        ScriptCommandTarget result = (ScriptCommandTarget) super.clone();
        result.scriptFileNames = (ScriptFileNameStack) scriptFileNames.clone();
        return result;
    } 
        
}
