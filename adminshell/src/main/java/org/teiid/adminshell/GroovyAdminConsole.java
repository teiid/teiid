/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General public static
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General public static License for more details.
 * 
 * You should have received a copy of the GNU Lesser General public static
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.adminshell;

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import groovy.ui.Console;

import java.io.File;

import javax.swing.UIManager;

import org.codehaus.groovy.runtime.StackTraceUtils;
import org.teiid.adminapi.Admin;

public class GroovyAdminConsole {
	
	public static final String IMPORTS = "import static " + AdminShell.class.getName() + ".*;\n" +  //$NON-NLS-1$ //$NON-NLS-2$
			"import static " + GroovySqlExtensions.class.getName() + ".*;\n" + //$NON-NLS-1$ //$NON-NLS-2$
			"import " + Admin.class.getPackage().getName() + ".*;\n"; //$NON-NLS-1$ //$NON-NLS-2$
	
	public static void main(String[] args) throws Exception {
        // allow the full stack traces to bubble up to the root logger
        java.util.logging.Logger.getLogger(StackTraceUtils.STACK_LOG_NAME).setUseParentHandlers(true);

        //when starting via main set the look and feel to system
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName()); 

        final Console console = new Console(Console.class.getClassLoader());
        console.setBeforeExecution(new Closure(null) {
        	public void doCall() {
        		console.setShell(new GroovyShell(Console.class.getClassLoader(), new Binding()) {
        			public Object run(String scriptText, String fileName, String[] args) throws org.codehaus.groovy.control.CompilationFailedException {
        				return super.run(IMPORTS + scriptText, fileName, args);
        			};
        		});
        	}
		});
        console.run();
        if (args.length == 1) {
        	console.loadScriptFile(new File(args[0]));
        }
	}

}
