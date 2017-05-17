/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
