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

package com.metamatrix.query.processor.xml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.util.LogConstants;

/**
 * <p>This instruction holds a List of Criteria, and for each Criteria there is a
 * {@link Program} that will be executed.  Therefore, this ProcessorInstruction
 * implements an arbitrarily deep if-else if-....else block.</p>
 * 
 * <p>During processing, the Criteria are iterated through, in order.  When one 
 * evaluates to true, that Criteria's Program is placed on to the 
 * Program {@link ProcessorEnvironment#getProgramStack stack}, and the loop
 * is broken - this simulates the if-else if-... behavior.  An optional
 * "else" or "default" Program can be added via the 
 * {@link #addDefaultChoiceSubProgram addDefaultChoiceSubProgram} 
 * method - this is the optional "else"
 * block at the end of an if-else if-... construct.</p>
 */
public class IfInstruction extends ProcessorInstruction {

    private List thenBlocks;
    private DefaultCondition defaultCondition;

    // State if condition evaluation blocked
    private boolean blockedOnCondition = false;
    private int blockedConditionIndex = 0;

    /**
     * Constructor for IfInstruction.
     * @param endIf see {@link #getEndIf}
     */
    public IfInstruction() {
        super();
        this.thenBlocks = new ArrayList();
    }
    
    public void addCondition(Condition condition) { 
        thenBlocks.add(condition);
    }

    public void setDefaultCondition(DefaultCondition defaultCondition) { 
        this.defaultCondition = defaultCondition;
    }

    /**
     * Return the number of "if/else if" clauses and 
     * corresponding "then" sub programs in this If Instruction.
     * (Does not include the optional 
     * {@link #getElseProgram "else" sub program}).
     * @return number of "then" sub programs
     */
    int getThenCount(){
        return this.thenBlocks.size();
    }

    /**
     * Return the Criteria object representing the "if" or
     * "else if" condition, at the indicated "then" index.
     * Use {@link #getThenCount getThenCount} method to get the
     * number of then sub programs.
     * @param thenCount index into the ordered list of "then" sub programs
     * @return Criteria for the sub program, or null if it is an invalid index
     */
    Condition getThenCondition(int thenCount){
        if (thenCount >=0 && thenCount < this.thenBlocks.size()){
            return (Condition)this.thenBlocks.get(thenCount);
        }
        return null;
    }

    /**
     * Return the sub Program object representing a "then" clause
     * of this "if then else if..." block, at the indicated "then" index.
     * Use {@link #getThenCount getThenCount} method to get the
     * number of then sub programs.
     * @param thenCount index into the ordered list of "then" sub programs
     * @return the "thenCount"th "then" sub program, or null if it is an invalid index
     */
    Program getThenProgram(int thenCount){
        if (thenCount >=0 && thenCount < this.thenBlocks.size()){
            Condition condition = (Condition)this.thenBlocks.get(thenCount);
            return condition.getThenProgram();
        }
        return null;
    }    
    
    /**
     * Returns the optional "else" sub program, which has no criteria
     * but is executed if no other "if/else if" condition evaluates to
     * true.
     * @return else Program, may be null
     */
    Program getElseProgram(){
        return this.defaultCondition.getThenProgram();
    }
  
    /**
     * This instruction will evaluate it's criteria, one by one.  If any evaluate
     * to true, it will push the corresponding sub-program on to the top of the
     * program stack, and break from the loop.  Regardless if whether any criteria
     * evaluate to true, this instruction will increment the program counter of the
     * current program.
     * @see ProcessorInstruction#process(ProcessorEnvironment)
     */
    public XMLContext process(XMLProcessorEnvironment env, XMLContext context)
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException{

        List thens = this.thenBlocks;
        if (this.defaultCondition != null){
	        //add default choice to the end of the List - this will be
	        //the "else" at the end of the "if-then-else-if..." structure,
	        //and its rsNames will be null

            //the original List must not be modified, so make a copy
            thens = new ArrayList(thenBlocks);
	        thens.add(defaultCondition);
        }
        
        int conditionIndex = this.blockedConditionIndex;
        if(blockedOnCondition) {
            // Remove state - we have recovered and will reset if necessary
            this.blockedOnCondition = false;
            this.blockedConditionIndex = 0;            
        } else{
            conditionIndex = 0;
        }
        
        Condition condition = null;
        boolean foundTrueCondition = false;

        for(; conditionIndex < thens.size(); conditionIndex++){            
            condition = (Condition)thens.get(conditionIndex);
                             
             // evaluate may block if criteria evaluation blocks
             try {
                 if(condition.evaluate(env, context)) {
                     foundTrueCondition = true;
                     //break from the loop; only the first "then" Program
                     //whose criteria evaluates to true will be executed 
                     break;
                 }
             } catch(BlockedException e) {
                 // Save state and rethrow
                 this.blockedOnCondition = true;
                 this.blockedConditionIndex = conditionIndex; 
                 throw e;
             }
            
        }

        // This IF instruction should be processed exactly once, so the 
        // program containing the IF instruction needs to have it's
        // counter incremented.
        // NOTE: At this point, if any of the conditions (above) evaluated
        // to true, then a sub program will be pushed (below) onto the 
        // stack above this current program.  That's okay.  Still need to
        // increment program counter so this IF instruction is only
        // executed once
        env.incrementCurrentProgramCounter();

        if (foundTrueCondition) {

            //push the "then" Program onto the stack            
            Program thenProgram = condition.getThenProgram();
            
            env.pushProgram(thenProgram, condition.isProgramRecursive());
            LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"IF: true condition", condition, "- then program:", thenProgram}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return context;
    }

    public String toString() {
        return "IF BLOCK:"; //$NON-NLS-1$
    }
    
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "CHOICE"); //$NON-NLS-1$

        List conditions = new ArrayList(thenBlocks.size());
        List programs = new ArrayList(thenBlocks.size());
        
        Iterator iter = this.thenBlocks.iterator();
        while(iter.hasNext()) {
            Condition condition = (Condition) iter.next();
            conditions.add(condition.toString());
            if(condition instanceof RecurseProgramCondition) {
                programs.add(new HashMap());
            } else {
                programs.add(condition.getThenProgram().getDescriptionProperties());
            }
        }
        
        props.put(PROP_CONDITIONS, conditions);
        props.put(PROP_PROGRAMS, programs);
        if (defaultCondition != null && defaultCondition.getThenProgram() != null){
            props.put(PROP_DEFAULT_PROGRAM, defaultCondition.getThenProgram().getDescriptionProperties());
        }
                
        return props;
    }
}