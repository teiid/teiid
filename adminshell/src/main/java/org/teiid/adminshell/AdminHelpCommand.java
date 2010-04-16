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

import java.util.List;

import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Shell;

public class AdminHelpCommand extends CommandSupport {

	protected AdminHelpCommand(Shell shell) {
		super(shell, "adminhelp", "\\ah");  //$NON-NLS-1$ //$NON-NLS-2$
		
		//hook to introduce default imports
		shell.execute(GroovyAdminConsole.IMPORTS);
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
