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

package org.teiid;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import bsh.Capabilities;
import bsh.EvalError;
import bsh.Interpreter;

import com.metamatrix.script.shell.FilePrintStream;
import com.metamatrix.script.shell.ReaderInterceptor;
import com.metamatrix.script.shell.SimpleParser;


/** 
 * Invokes the BeanShell window, specifically designed for the metamatrix purposes.
 * The difference with this shell is, it will plug in a customer parser on top the
 * BeanShell, and load up all the MetaMatrix commands.
 */
public class AdminShell {
    
    public static void main( String args[] ) throws IOException {
        
        boolean gui = Boolean.getBoolean("gui"); //$NON-NLS-1$
        
        if (args.length == 0) {
            if ( !Capabilities.classExists( "bsh.util.Util" ) ) //$NON-NLS-1$
                System.out.println("Can't find the BeanShell utilities..."); //$NON-NLS-1$
        
            String teiidHome = System.getenv("TEIID_HOME"); //$NON-NLS-1$
            if (teiidHome == null) {
            	teiidHome = System.getProperty("user.dir");  //$NON-NLS-1$
            }
            File logDir = new File(teiidHome, "log"); //$NON-NLS-1$
            if (!logDir.exists()) {
            	logDir.mkdirs();
            }
            
            FileWriter logger = new FileWriter(teiidHome+"/log/adminscript.txt", true); //$NON-NLS-1$
            PrintStream out = new FilePrintStream(System.out, teiidHome+"/log/adminshell.log"); //$NON-NLS-1$
            
            try {
                SimpleParser p = new SimpleParser();
                Interpreter interpreter = new Interpreter(new ReaderInterceptor(p, logger), out, out, true); 
                interpreter.eval("importCommands(\"commands\")"); //$NON-NLS-1$
                interpreter.eval("load(\"server\")"); //$NON-NLS-1$
                
                p.setInterpreter(interpreter);
                
                if (Capabilities.haveSwing() && gui) {
                    //bsh.util.Util.startSplashScreen();
                    interpreter.eval("desktop()"); //$NON-NLS-1$
                    interpreter.getOut().flush();
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
