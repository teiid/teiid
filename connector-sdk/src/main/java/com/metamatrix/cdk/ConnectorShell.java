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

import com.metamatrix.core.commandshell.CommandShell;


/**
 * Command line utility to execute queries on a connector.
 */
public class ConnectorShell extends CommandShell {
	
	

    public ConnectorShell(IConnectorHost host) {
        super(new ConnectorShellCommandTarget(host));
    }
    
    public ConnectorShell(ConnectorShellCommandTarget target) {
        super(target);
    }

    public static void main(String[] args) {
        System.out.println("Starting"); //$NON-NLS-1$
        
        new ConnectorShell(new ConnectorShellCommandTarget()).run(args, DEFAULT_LOG_FILE);
    }
    
    protected boolean showHelpFor(String methodName) {
        return getParameterNamesDirect(methodName) != null;
    }
    
    protected String[] getParameterNames(String methodName) {
        String[] result = getParameterNamesDirect(methodName);
        if (result == null) {
            result = new String[] {};
        }
        return result;
    }

    private String[] getParameterNamesDirect(String methodName) {
        if (methodName.equals("select")) { //$NON-NLS-1$
            return new String[] { "query" }; //$NON-NLS-1$
        } else if (methodName.equals("run")) { //$NON-NLS-1$
            return new String[] { "scriptName" }; //$NON-NLS-1$
        } else if (methodName.equals("setProperty")) { //$NON-NLS-1$
            return new String[] { "propertyName", "propertyValue" }; //$NON-NLS-1$ //$NON-NLS-2$
        } else if (methodName.equals("load")) { //$NON-NLS-1$
            return new String[] { "fullyQualifiedConnectorClassName", "pathToVdbFile" }; //$NON-NLS-1$ //$NON-NLS-2$
        } else if (methodName.equals("getProperties")) { //$NON-NLS-1$
            return new String[] {};
        } else if (methodName.equals("delete")) { //$NON-NLS-1$
            return new String[] { "multilineSqlTerminatedWith;" }; //$NON-NLS-1$
        } else if (methodName.equals("insert")) { //$NON-NLS-1$
            return new String[] { "multilineSqlTerminatedWith;" }; //$NON-NLS-1$
        } else if (methodName.equals("help")) { //$NON-NLS-1$
            return new String[] { "" }; //$NON-NLS-1$
        } else if (methodName.equals("update")) { //$NON-NLS-1$
            return new String[] { "multilineSqlTerminatedWith;" }; //$NON-NLS-1$
        } else if (methodName.equals("select")) { //$NON-NLS-1$
            return new String[] { "multilineSqlTerminatedWith;" }; //$NON-NLS-1$
        } else if (methodName.equals("quit")) { //$NON-NLS-1$
            return new String[] { "" }; //$NON-NLS-1$
        } else if (methodName.equals("runScript")) { //$NON-NLS-1$
            return new String[] { "pathToScriptFile", "scriptNameWithinFile" }; //$NON-NLS-1$ //$NON-NLS-2$
        } else if (methodName.equals("loadFromScript")) { //$NON-NLS-1$
            return new String[] { "pathToConfiguratonScript" };         //$NON-NLS-1$
        } else if (methodName.equals("setFailOnError")) { //$NON-NLS-1$
            return new String[] { "boolean" }; //$NON-NLS-1$
        } else if (methodName.equals("setPrintStackOnError")) { //$NON-NLS-1$
            return new String[] { "boolean" }; //$NON-NLS-1$
        } else if (methodName.equals("setSecurityContext")) { //$NON-NLS-1$
            return new String[] { "vdbName", "vdbVersion", "userName" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if (methodName.equals("setScriptFile")) { //$NON-NLS-1$
            return new String[] { "pathToScriptFile" }; //$NON-NLS-1$
        } else if (methodName.equals("runAll")) { //$NON-NLS-1$
            return new String[] {};
        } else if (methodName.equals("setBatchSize")) { //$NON-NLS-1$
            return new String[] { "batchSize" }; //$NON-NLS-1$
        } else if (methodName.equalsIgnoreCase("start")) { //$NON-NLS-1$
            return new String[] {};
        } else if (methodName.equalsIgnoreCase("stop")) { //$NON-NLS-1$
            return new String[] {};
        } else if (methodName.equalsIgnoreCase("loadProperties")) { //$NON-NLS-1$
            return new String[] {"pathToPropertyFile"}; //$NON-NLS-1$
        } else if (methodName.equalsIgnoreCase("createTemplate")) { //$NON-NLS-1$
            return new String[] {"pathToTemplateFile"}; //$NON-NLS-1$
        } else if (methodName.equalsIgnoreCase("createArchive")) { //$NON-NLS-1$
            return new String[] {"pathToArchiveFileName", "pathToCDKFileName", "pathToDirectoryForExtenstionModules"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if (methodName.equalsIgnoreCase("loadArchive")) { //$NON-NLS-1$
            return new String[] {"pathToArchiveFileName", "newConnectorTypeName"}; //$NON-NLS-1$//$NON-NLS-2$
        } else if (methodName.equalsIgnoreCase("exec")) { //$NON-NLS-1$
            return new String[] {"fullyQualifiedProcedureName"}; //$NON-NLS-1$
        }
        return null;
    }
}
