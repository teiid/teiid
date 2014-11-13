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
