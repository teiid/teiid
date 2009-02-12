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

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.util.StringUtilities;

/**
 * Understands how to read specific command line scripts from a String containing multiple scripts.
 */
public class ScriptReader {
    public static String RESULT_KEYWORD = "result"; //$NON-NLS-1$
    
    private static final String LEFT_BRACE = "{"; //$NON-NLS-1$
    private static final String RIGHT_BRACE = "}"; //$NON-NLS-1$
    
    private String script;
    private String testName;
    private int testScriptIndex;
    private String testScript;
    private String[] testLines;

    /**
     * 
     * @param scripts A String containing many script definitions.
     */
    public ScriptReader(String scripts) {
        this.script = scripts;
    }

    /**
     * Indicates which specific script is currently being referenced.
     * @param scriptName The name of the script to go to.  This name should correspond to a name in the scripts String.
     */
    public void gotoScript(String scriptName) {
        testScriptIndex = 0;
        this.testName = scriptName;
        testScript = getScriptContents();
        testLines = StringUtilities.getLines(testScript);
    }

    /**
     * Returns whether or not their are additional commands for the current script.
     */
    public boolean hasMore() {
        checkGotoCalled();
        skipBlankLines();
        return testScriptIndex < testLines.length;
    }

    /**
     * Retrieve the next command line for the current script.
     */
    public String nextCommandLine() {
        checkGotoCalled();
        skipBlankLines();
        if (checkResults()) {
            getExpectedResults();
        }
        if (testScriptIndex < testLines.length) {
            String result = testLines[testScriptIndex].trim();
            testScriptIndex++;
            return result;
        }
        return null;

    }

    private void checkGotoCalled() {
        if (testName == null) {
            String message = CorePlugin.Util.getString("ScriptReader.Call_goto_first"); //$NON-NLS-1$
            throw new MetaMatrixRuntimeException(message);
        }
    }

    private void skipBlankLines() {
        while (testScriptIndex < testLines.length && testLines[testScriptIndex].trim().length() == 0) {
            testScriptIndex++;
        }
    }

    /**
     * Returns whether the last command line includes a specification of what the expected results are.
     */
    public boolean checkResults() {
        checkGotoCalled();
        skipBlankLines();
        if (testScriptIndex < testLines.length && testLines[testScriptIndex].trim().startsWith(RESULT_KEYWORD)) {
            return true;
        }
        return false;
    }

    /**
     * If the last command line includes a specification of what the expected results are, then this method returns
     * the expected value from the script.
     */
    public String getExpectedResults() {
        checkGotoCalled();
        if (!checkResults()) {
            return null;
        }
        testScriptIndex++;
        StringBuffer result = new StringBuffer();

        while (!testLines[testScriptIndex].trim().equals("]")) { //$NON-NLS-1$
            result.append(testLines[testScriptIndex]);
            result.append(StringUtil.LINE_SEPARATOR);
            testScriptIndex++;
        }
        testScriptIndex++;
        return result.toString();

    }

    private String getScriptContents() {
        boolean readyForNewScript = true;
        String[] scriptLines = StringUtilities.getLines(script);
        for (int i=0; i<scriptLines.length; i++) {
            if (readyForNewScript) {
                boolean openingBraceFound = false;
                if (scriptLines[i].indexOf(LEFT_BRACE)>=0) {
                    openingBraceFound = true;
                }
                List subStrings = StringUtil.split(scriptLines[i], LEFT_BRACE);
                    if (subStrings.size() >= 1) {
                        String name = (String) subStrings.get(0);
                        name = name.trim();
                        if (name.length() > 0) {
                            readyForNewScript = false;
                            if (name.equals(testName)) {
                                if (!openingBraceFound) {
                                    for (; i<scriptLines.length; i++) {
                                        if (scriptLines[i].indexOf(LEFT_BRACE)>=0) {
                                            break;
                                        }
                                    }
                                }
                                return readToEndOfScript(scriptLines, i + 1);
                            }
                        }
                    }
                
            } else {
                if (scriptLines[i].trim().equals(RIGHT_BRACE)) {
                    readyForNewScript = true;
                }
            }
        }

        Object[] params = new Object[] {testName};
        String message = CorePlugin.Util.getString("ScriptReader.Could_not_find_test_{0}", params); //$NON-NLS-1$
        throw new MetaMatrixRuntimeException(message);
    }
    
    private String readToEndOfScript(String[] lines, int index) {
        StringBuffer result = new StringBuffer();
        for (int i=index; i<lines.length; i++) {
            if (lines[i].trim().equals("}")) { //$NON-NLS-1$
                break;    
            }
            result.append(lines[i]);
            result.append(StringUtil.LINE_SEPARATOR);
        }
        return result.toString();
    }
    
    public String[] getScriptNames() {
        List result = new ArrayList();
        List subStrings = StringUtil.split(script, "{"); //$NON-NLS-1$
        for (int i = 0; i < subStrings.size()-1; i++) {
            String fragment = ((String) subStrings.get(i)).trim();
            String[] lines = StringUtilities.getLines(fragment);
            String testName = lines[lines.length-1];
            if (!containsWhitespace(testName)) {
                if (testName.length() > 0) {
                    result.add(testName);
                }
            }                 
        }
        return (String[]) result.toArray(new String[result.size()]);
    }

    private boolean containsWhitespace(String text) {
        char[] chars = text.toCharArray();
        for (int j=0; j<chars.length; j++) {
            if (Character.isWhitespace(chars[j])) {
                return true;                        
            }
        }
        return false;
    }
}
