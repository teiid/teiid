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

package com.metamatrix.query.processor.program;

/**
 * Utility to print out a {@link Program Program}.
 */
public final class ProgramUtil {

    private ProgramUtil() {
    }

    public static final String programToString(Program program) {
        
        StringBuffer str = new StringBuffer();   
            
        int counter = 1;
        int tabs = 0;
        
        programToString(program, counter, tabs, str);
        
        return str.toString();
    }

    /**
     * This method calls itself recursively if either a While or If instruction is encountered. 
     * The sub program(s) from those kinds of instructions are passed, recursively, into this
     * method.
     */
    private static final int programToString(Program program, int counter, int tabs, StringBuffer str) {

        int instructionIndex = 0;
        ProgramInstruction inst = program.getInstructionAt(instructionIndex);
//        Program subprogram = null;
    
        while(inst != null) {
            
            printLine(counter++, tabs, inst.toString(), str);

			if(counter > 1000) { 
			    printLine(counter, tabs, "[OUTPUT TRUNCATED...]", str); //$NON-NLS-1$
			    break;
			}

            instructionIndex++;
            inst = program.getInstructionAt(instructionIndex);
        
        }

        return counter;
    }


    private static final void printLine(int counter, int tabs, String line, StringBuffer buffer) {
        // Pad counter with spaces
        String counterStr = "" + counter + ": "; //$NON-NLS-1$ //$NON-NLS-2$
        if(counter < 10) { 
            counterStr += " ";     //$NON-NLS-1$
        }
        if(counterStr.length() == 1) { 
            counterStr += "  "; //$NON-NLS-1$
        } else if(counterStr.length() == 2) { 
            counterStr += " ";     //$NON-NLS-1$
        } 
        
        buffer.append(counterStr + getTab(tabs) + line + "\n"); //$NON-NLS-1$
    }
        
    private static final String getTab(int tabs) { 
        if(tabs == 0) { 
            return ""; //$NON-NLS-1$
        }
        StringBuffer str = new StringBuffer();
        for(int i=0; i<tabs; i++) { 
            str.append("    "); //$NON-NLS-1$
        }    
        return str.toString();    
    }
      
}
