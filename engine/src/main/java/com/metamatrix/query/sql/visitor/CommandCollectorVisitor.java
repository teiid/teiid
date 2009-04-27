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

package com.metamatrix.query.sql.visitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.CommandStatement;
import com.metamatrix.query.sql.proc.LoopStatement;
import com.metamatrix.query.sql.symbol.ScalarSubquery;

/**
 * <p>This visitor class will traverse a language object tree and collect all sub-commands 
 * it finds.  It uses a List to collect the sub-commands in the order they're found.</p>
 * 
 * <p>The easiest way to use this visitor is to call the static methods which create 
 * the visitor, run the visitor, and get the collection. 
 * The public visit() methods should NOT be called directly.</p>
 */
public class CommandCollectorVisitor extends LanguageVisitor {

	private enum Mode {
		EMBEDDED,
		NON_EMBEDDED
	}
	
    private List<Command> commands = new ArrayList<Command>();
    private Set<Mode> modes;

    /**
     * Construct a new visitor with the default collection type, which is a 
     * {@link java.util.HashSet}.  
     */
    public CommandCollectorVisitor(Set<Mode> modes) { 
        this.modes = modes;
    }   

    /**
     * Get the commands collected by the visitor.  This should best be called 
     * after the visitor has been run on the language object tree.
     * @return List of {@link com.metamatrix.query.sql.lang.Command}
     */
    public List<Command> getCommands() { 
        return this.commands;
    }

    /**
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.ExistsCriteria)
     */
    public void visit(ExistsCriteria obj) {
        if (modes.contains(Mode.EMBEDDED)) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.ScalarSubquery)
     */
    public void visit(ScalarSubquery obj) {
    	if (modes.contains(Mode.EMBEDDED)) {
            this.commands.add(obj.getCommand());
    	}
    }

    /**
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.SubqueryCompareCriteria)
     */
    public void visit(SubqueryCompareCriteria obj) {
    	if (modes.contains(Mode.EMBEDDED)) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(SubqueryFromClause obj) {
    	if (modes.contains(Mode.EMBEDDED)) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(SubquerySetCriteria obj) {
    	if (modes.contains(Mode.EMBEDDED)) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(CommandStatement obj) {
    	if (modes.contains(Mode.EMBEDDED)) {
            this.commands.add(obj.getCommand());
        }
    }    

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(AssignmentStatement obj) {
    	if(modes.contains(Mode.EMBEDDED) && obj.hasCommand()) {
	        this.commands.add(obj.getCommand());
    	}
    }
    
    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(LoopStatement obj) {
        if (modes.contains(Mode.EMBEDDED)) {
            this.commands.add(obj.getCommand());
        }
    }
    
    public void visit(UnaryFromClause obj) {
        if (modes.contains(Mode.NON_EMBEDDED) && obj.getExpandedCommand() != null) {
            this.commands.add(obj.getExpandedCommand());
        }
    }
    
    public void visit(BatchedUpdateCommand obj) {
        if (modes.contains(Mode.NON_EMBEDDED)) {
            this.commands.addAll(obj.getUpdateCommands());
        }
    }
    
    public void visit(ProcedureContainer obj) {
        if (modes.contains(Mode.NON_EMBEDDED) && obj.getSubCommand() != null) {
            this.commands.add(obj.getSubCommand());
        }
    }
    
    /**
     * Helper to quickly get the commands from obj
     * @param obj Language object
     * @param elements Collection to collect commands in
     */
    public static final List<Command> getCommands(Command obj) {
        return getCommands(obj, true, true);
    }

    public static final List<Command> getCommands(Command obj, boolean embeddedOnly) {
        return getCommands(obj, true, !embeddedOnly);
    }
    
    private static final List<Command> getCommands(Command command, boolean embedded, boolean nonEmbedded) {
    	HashSet<Mode> modes = new HashSet<Mode>();
    	if (embedded) {
    		modes.add(Mode.EMBEDDED);
    	}
    	if (nonEmbedded) {
    		modes.add(Mode.NON_EMBEDDED);
    	}
        CommandCollectorVisitor visitor = new CommandCollectorVisitor(modes);
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
    
    public static final List<Command> getNonEmbeddedCommands(Command obj) {
        return getCommands(obj, false, true);
    }

}
