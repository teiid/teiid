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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.UnitTestUtil;

/**
 * Base class expected to be extended by classes which test command shells.
 * Knows how to run test scripts and check their results.
 * Also keeps a transcript of the test suite execution.
 */
public abstract class CommandShellTest extends TestCase implements ScriptResultListener {

	private static final String TEST_PREFIX = "test"; //$NON-NLS-1$
    private static String defaultTestFileName = "/script.txt"; //$NON-NLS-1$
    private static String defaultTranscriptFileName = "/transcript.txt"; //$NON-NLS-1$
    private static StringBuffer testTranscript = new StringBuffer();
    private static int testCount = 0;

    private String testFileName;
    private String transcriptFileName;

    private CommandShellTest(String name, String testFileName, String transcriptFileName) {
        super(name);
        this.testFileName = testFileName;
        this.transcriptFileName = transcriptFileName;
    }

    public CommandShellTest(String name) {
        this(name, UnitTestUtil.getTestDataPath() + defaultTestFileName, UnitTestUtil.getTestScratchPath() + defaultTranscriptFileName);
    }
    
    public CommandShellTest(String name, String testFile) {
        this(name, getTestFileName(testFile), UnitTestUtil.getTestScratchPath() + File.separator + testFile + "_transcript.txt");
    }

    protected void runTest(String testName) {
        runTest(testName, true, this);
    }

    public void scriptResults(String scriptFileName, String scriptName, String expected, String actual) {
        assertEquals(expected, actual);
    }

    abstract protected CommandShell getCommandShell();

    protected void runTest(String testName, boolean runSetup, ScriptResultListener listener) {
        CommandShell shell = getInitilizedShell();
        StringBuffer testTranscript = new StringBuffer();
        if (runSetup) {
            runSetupScript(shell, testTranscript);
        }
        runTestScript(testName, listener, shell, testTranscript);
    }

    private CommandShell getInitilizedShell() {
        CommandShell shell = getCommandShell();
        shell.setDefaultFilePath(UnitTestUtil.getTestDataPath() + "/"); //$NON-NLS-1$
        return shell;
    }

    private void runSetupScript(CommandShell shell, StringBuffer testTranscript) {
        StringBuffer setupTranscript = null;
        if (testCount == 0) {
            setupTranscript = testTranscript;
        }
        shell.runScript(testFileName, "setUp", setupTranscript, null); //$NON-NLS-1$
    }

    private void runTestScript(
        String testName,
        ScriptResultListener listener,
        CommandShell shell,
        StringBuffer testTranscript) {
        try {
            shell.runScript(testFileName, testName, testTranscript, listener);
        } finally {
            testCount++;
            if (transcriptFileName != null) {
            	if (transcriptFileName.lastIndexOf('/') != -1) {
	            	File tranFile = new File(transcriptFileName.substring(0, transcriptFileName.lastIndexOf('/')));
	            	tranFile.mkdirs();
            	}
                FileUtil transcriptFile = new FileUtil(transcriptFileName);
                if (testCount == 1) {
                    transcriptFile.delete();
                }
                transcriptFile.append(testTranscript.toString());
            }
        }
    }

    public static String[] getScriptNames(String testFileName) {
        String[] scriptNames = new ScriptReader(new FileUtil(testFileName).read()).getScriptNames();
        List result = new ArrayList();
        for (int i = 0; i < scriptNames.length; i++) {
            if (scriptNames[i].toLowerCase().startsWith(TEST_PREFIX.toLowerCase())) {
                result.add(scriptNames[i]);
            }
        }
        return (String[]) result.toArray(new String[result.size()]);
    }

    protected static String getTestTranscript() {
        return testTranscript.toString();
    }

	public static String getTestFileName(String testFile) {
		return UnitTestUtil.getTestDataPath() + File.separator + testFile + ".txt";
	}
}
