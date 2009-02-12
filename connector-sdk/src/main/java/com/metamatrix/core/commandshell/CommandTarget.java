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

import java.util.Collections;
import java.util.Set;

/**
 * Base class for command-line targets used by the CommandShell class.
 * This class should be extended with custom commands for each command-line tool.
 */
public class CommandTarget {
    protected CommandShell shell;

    protected void setShell(CommandShell shell) {
        this.shell = shell;
    }
    
    protected CommandShell getShell() {
        return shell;
    }

    public String help() {
        return shell.getHelp();
    }

    public void quit() {
        shell.quit();
    }
    
    public void exit() {
        shell.quit();
    }
            
    protected void runningScript(String fileName) {
    }
    
    protected Set getMethodsToIgnore() {
        return Collections.EMPTY_SET;
    }
}
