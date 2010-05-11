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

package org.teiid.query.optimizer.relational;

import java.util.LinkedList;

public class RuleStack {

    private LinkedList<OptimizerRule> rules = new LinkedList<OptimizerRule>();
    
    public void push(OptimizerRule rule) { 
        rules.addFirst(rule);
    }
    
    public void addLast(OptimizerRule rule) {
    	rules.addLast(rule);
    }
    
    public OptimizerRule pop() { 
        if(rules.isEmpty()) { 
            return null;
        }    
        return rules.removeFirst();
    }
    
    public boolean isEmpty() { 
        return rules.isEmpty();
    }
    
    public int size() { 
        return rules.size();
    }
    
    /**
     * Remove all occurrences of this rule in the stack 
     * @param rule The rule to remove
     * @since 4.2
     */
    public void remove(OptimizerRule rule) {
        while(rules.remove(rule)) {}            
    }
    
    public boolean contains(OptimizerRule rule) {
        return rules.contains(rule);
    }
    
}