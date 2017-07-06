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

import java.util.ArrayList;
import java.util.List;

import org.codehaus.groovy.tools.shell.Command;
import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;

public class AdminHelpCommand extends CommandSupport {

	protected AdminHelpCommand(Groovysh shell) {
		super(shell, "adminhelp", "\\ah");  //$NON-NLS-1$ //$NON-NLS-2$
		
		//hook to introduce default imports
		final String[] imports = GroovyAdminConsole.IMPORTS.split("\n"); //$NON-NLS-1$
		for(String aimport : imports){
			shell.execute(aimport);
		}
		//for backwards compatibility add aliases to 1.7 Groovy commands
		for (Command cmd :new ArrayList<Command>(shell.getRegistry().commands())) {
			if (cmd.getHidden()) {
				continue;
			}
			String name = cmd.getName();
			if (name.startsWith(":")) { //$NON-NLS-1$
				shell.execute(":alias " + name.substring(1) + " " + name); //$NON-NLS-1$ //$NON-NLS-2$
				String shortCut = cmd.getShortcut();
				if (shortCut.startsWith(":")) { //$NON-NLS-1$
					shell.execute(":alias \\" + shortCut.substring(1) + " " + name); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
		}
	}

	@Override
	public Object execute(List args) {
		if (args.size() > 1) {
            fail(messages.format("error.unexpected_args", new Object[] {args.toString()}));  //$NON-NLS-1$
        }
		if (args.isEmpty()) {
			AdminShell.adminHelp();
		} else {
			AdminShell.adminHelp(args.get(0).toString());
		}
		return null;
	}

}
