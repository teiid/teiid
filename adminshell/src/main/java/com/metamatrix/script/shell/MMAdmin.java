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

package com.metamatrix.script.shell;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import bsh.Capabilities;
import bsh.EvalError;
import bsh.Interpreter;


/** 
 * Invokes the BeanShell window, specifically designed for the metamatrix purposes.
 * The difference with this shell is, it will plug in a customer parser on top the
 * BeanShell, and load up all the MetaMatrix commands.
 */
public class MMAdmin {
    
    public static void main( String args[] ) throws IOException {
        
        boolean gui = Boolean.getBoolean("gui"); //$NON-NLS-1$
        System.setProperty("metamatrix.config.none", "true"); //$NON-NLS-1$ //$NON-NLS-2$
        
        if (args.length == 0) {
            if ( !Capabilities.classExists( "bsh.util.Util" ) ) //$NON-NLS-1$
                System.out.println("Can't find the BeanShell utilities..."); //$NON-NLS-1$
        
            FileWriter logger = new FileWriter("adminscript.txt", true); //$NON-NLS-1$
            PrintStream out = new FilePrintStream(System.out, "mmadmin.log"); //$NON-NLS-1$
            
            try {
                SimpleParser p = new SimpleParser();
                Interpreter interpreter = new Interpreter(new ReaderInterceptor(p, logger), out, out, true); 
                interpreter.eval("importCommands(\"commands\")"); //$NON-NLS-1$
                interpreter.eval("load(\"server\")"); //$NON-NLS-1$
                
                p.setInterpreter(interpreter);
                
                if (Capabilities.haveSwing() && gui) {
                    //bsh.util.Util.startSplashScreen();
                    interpreter.eval("desktop()"); //$NON-NLS-1$                    
                } else {
                    interpreter.run();
                }
            } catch ( EvalError e ) {
               System.err.println("Couldn't start desktop: "+e); //$NON-NLS-1$
            } finally {
                logger.close();
                out.close();
            }
        }
        else {
            // If we running a script file run as it is
            Interpreter.main(args);
        }
    }
    
}
