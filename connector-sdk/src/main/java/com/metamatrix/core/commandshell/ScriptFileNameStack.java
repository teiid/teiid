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
import java.util.Stack;

/**
 * Keeps track of script file names and script directories as scripts are run.
 * Knows how to interpret relative file names in the context of other file names.
 */
class ScriptFileNameStack implements Cloneable {
    private String defaultScriptFileName;

    private Stack executingScriptFileName = new Stack();

    /**
     * @param scriptFileName The default script file name to use when none is specified and when no scripts are executing.
     */
    public void setDefaultScriptFileName(String scriptFileName) {
        log("setDefaultScriptFileName", scriptFileName); //$NON-NLS-1$
        defaultScriptFileName = scriptFileName;
    }
    

    /**
     * Record that a script file name is being used.  This will become the default if no default has been set.
     * @param scriptFileName
     */
    public void usingScriptFile(String scriptFileName) {
        log("usingScriptFile", scriptFileName); //$NON-NLS-1$
        if (!hasDefaultScriptFileBeenSet()) {
            setDefaultScriptFileName(scriptFileName);
        }
    }
    
    private void log(String method, String value) {
        //System.out.println("ScriptFileNames." + method + ": " + value);
    }

    /**
     * Expands relative file names to include path information from currently running file names.
     * @param scriptFileName
     * @return
     */
    public String expandScriptFileName(String scriptFileName) {
        log("expandScriptFileName", scriptFileName); //$NON-NLS-1$
        String result = scriptFileName;
        String parent = getParent();
        if (isAbsolute(scriptFileName) || parent == null) {
        } else {
            result = parent + File.separator + scriptFileName;
        }
        log("expandScriptFileName", "result=" + result); //$NON-NLS-1$ //$NON-NLS-2$
        return result;
    }
   
    private String getParent() {
        String parent = null;
        if (!executingScriptFileName.isEmpty()) {
            parent = new File(peek()).getParent();
        }
        return parent;
    }
   

    public boolean hasDefaultScriptFileBeenSet() {
        return defaultScriptFileName != null;
    }

    private String peek() {
        return (String) executingScriptFileName.peek();
    }
    
    private boolean isAbsolute(String fileName) {
        File file = new File(fileName);
        return file.isAbsolute() || fileName.startsWith("\\") || fileName.startsWith("/"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Retrieves the default script file name.
     * If a script is currently executing this will return the name of the current executing script file rather than the default.
     * @return
     */
    public String getUnexpandedCurrentScriptFileName() {
        String result = getCurrentScriptFileNameDirect();
        log("getCurrentScriptFileName", result); //$NON-NLS-1$
        return result;
    }

    private String getCurrentScriptFileNameDirect() {
        if (executingScriptFileName.isEmpty()) {
            return defaultScriptFileName;
        }
        return new File(peek()).getName();
    }

    /**
     * Indicate that a script is about to be executed from the given file.
     * This must be called in order to maintain the stack tracking the current script file.
     * @param fileName
     */
    public void startingScriptFromFile(String fileName) {
        log("startingScriptFromFile", fileName); //$NON-NLS-1$
        executingScriptFileName.push(fileName);
    }
    
    /**
     * Indicate that a script has completed execution.
     * This must be called in order to maintain the stack tracking the current script file.
     */
    public void finishedScript() {
        log("finishedScript", ""); //$NON-NLS-1$ //$NON-NLS-2$
        executingScriptFileName.pop();
    }
    
    protected Object clone() throws CloneNotSupportedException {
        ScriptFileNameStack result = (ScriptFileNameStack) super.clone();
        executingScriptFileName = (Stack) executingScriptFileName.clone();        
        return result;
    } 

}
