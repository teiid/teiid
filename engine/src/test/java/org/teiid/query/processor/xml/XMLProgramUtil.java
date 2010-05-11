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

package org.teiid.query.processor.xml;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.query.processor.xml.Condition;
import org.teiid.query.processor.xml.IfInstruction;
import org.teiid.query.processor.xml.ProcessorInstruction;
import org.teiid.query.processor.xml.Program;
import org.teiid.query.processor.xml.RecurseProgramCondition;
import org.teiid.query.processor.xml.WhileInstruction;


/** 
 * 
 */
public class XMLProgramUtil {
    
    public static Map getProgramStats(Program program) {
        HashMap map = new HashMap();
        getProgramStats(program, map);
        return map;
    }

    private static Map getProgramStats(Program program, Map map) {
        Program childProgram = null;
        
        if (program == null) {
            return map;
        }
        
        for (int i = 0; i < program.getProcessorInstructions().size(); i++) {
            ProcessorInstruction inst = program.getInstructionAt(i);
            
            if (inst instanceof WhileInstruction) {
                WhileInstruction whileInst = (WhileInstruction)inst;
                childProgram = whileInst.getBlockProgram();
                getProgramStats(childProgram, map);
            }
            else if (inst instanceof IfInstruction) {
                IfInstruction ifInst = (IfInstruction)inst;
                
                getProgramStats(ifInst.getElseProgram(), map);
                
                for (int then = 0; then < ifInst.getThenCount(); then++) {
                    childProgram = ifInst.getThenProgram(then);
                    Condition condition = ifInst.getThenCondition(then);
                    if (!(condition instanceof RecurseProgramCondition)) {
                        getProgramStats(childProgram, map);    
                    }
                }
            }
            
            List instrs = (List)map.get(inst.getClass());
            if (instrs == null) {
                instrs = new LinkedList();
                map.put(inst.getClass(), instrs);
            }
            instrs.add(inst);
        }
        return map;
    }
}

