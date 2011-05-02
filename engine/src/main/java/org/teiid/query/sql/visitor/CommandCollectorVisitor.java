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

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.List;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.symbol.ScalarSubquery;


/**
 * <p>This visitor class will traverse a language object tree and collect all sub-commands 
 * it finds.  It uses a List to collect the sub-commands in the order they're found.</p>
 * 
 * <p>The easiest way to use this visitor is to call the static methods which create 
 * the visitor, run the visitor, and get the collection. 
 * The public visit() methods should NOT be called directly.</p>
 */
public class CommandCollectorVisitor extends LanguageVisitor {

    private List<Command> commands = new ArrayList<Command>();

    /**
     * Get the commands collected by the visitor.  This should best be called 
     * after the visitor has been run on the language object tree.
     * @return List of {@link org.teiid.query.sql.lang.Command}
     */
    public List<Command> getCommands() { 
        return this.commands;
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.ExistsCriteria)
     */
    public void visit(ExistsCriteria obj) {
        this.commands.add(obj.getCommand());
    }

    public void visit(ScalarSubquery obj) {
        this.commands.add(obj.getCommand());
    }

    public void visit(SubqueryCompareCriteria obj) {
        this.commands.add(obj.getCommand());
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(SubqueryFromClause obj) {
        this.commands.add(obj.getCommand());
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(SubquerySetCriteria obj) {
        this.commands.add(obj.getCommand());
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(CommandStatement obj) {
        this.commands.add(obj.getCommand());
    }    

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(LoopStatement obj) {
        this.commands.add(obj.getCommand());
    }
    
    public void visit(BatchedUpdateCommand obj) {
        this.commands.addAll(obj.getUpdateCommands());
    }
    
    @Override
    public void visit(AlterProcedure alterProcedure) {
    	this.commands.add(alterProcedure.getDefinition());
    }
    
    @Override
    public void visit(AlterTrigger alterTrigger) {
    	if (alterTrigger.getDefinition() != null) {
    		this.commands.add(alterTrigger.getDefinition());
    	}
    }
    
    @Override
    public void visit(AlterView alterView) {
    	this.commands.add(alterView.getDefinition());
    }
    
    /**
     * Helper to quickly get the commands from obj
     * @param obj Language object
     * @param elements Collection to collect commands in
     */
    public static final List<Command> getCommands(Command command) {
        CommandCollectorVisitor visitor = new CommandCollectorVisitor();
        final boolean visitCommands = command instanceof SetQuery;
        PreOrderNavigator navigator = new PreOrderNavigator(visitor) {

        	@Override
        	protected void visitNode(LanguageObject obj) {
        		if (!visitCommands && obj instanceof Command) {
    				return;
        		}
        		super.visitNode(obj);
        	}
        	
        };
        command.acceptVisitor(navigator);
        return visitor.getCommands();
    }
    
}
