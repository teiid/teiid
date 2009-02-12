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
import java.util.List;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.CommandStatement;
import com.metamatrix.query.sql.proc.DeclareStatement;
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

    private List commands;
    private boolean embeddedOnly;
    private boolean nonEmbeddedOnly;

    /**
     * Construct a new visitor with the default collection type, which is a 
     * {@link java.util.HashSet}.  
     */
    public CommandCollectorVisitor(boolean embeddedOnly, boolean nonEmbeddedOnly) { 
        this.commands = new ArrayList();
        this.embeddedOnly = embeddedOnly;
        this.nonEmbeddedOnly = nonEmbeddedOnly;
    }   

    /**
     * Get the commands collected by the visitor.  This should best be called 
     * after the visitor has been run on the language object tree.
     * @return List of {@link com.metamatrix.query.sql.lang.Command}
     */
    public List getCommands() { 
        return this.commands;
    }

    /**
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.ExistsCriteria)
     */
    public void visit(ExistsCriteria obj) {
        if (!nonEmbeddedOnly) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.ScalarSubquery)
     */
    public void visit(ScalarSubquery obj) {
        if (!nonEmbeddedOnly) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.SubqueryCompareCriteria)
     */
    public void visit(SubqueryCompareCriteria obj) {
        if (!nonEmbeddedOnly) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(SubqueryFromClause obj) {
        if (!nonEmbeddedOnly) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(SubquerySetCriteria obj) {
        if (!nonEmbeddedOnly) {
            this.commands.add(obj.getCommand());
        }
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(CommandStatement obj) {
        if (!nonEmbeddedOnly) {
            this.commands.add(obj.getCommand());
        }
    }    

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(AssignmentStatement obj) {
    	if(!nonEmbeddedOnly && obj.hasCommand()) {
	        this.commands.add(obj.getCommand());
    	}
    }
    
    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(LoopStatement obj) {
        if (!nonEmbeddedOnly) {
            this.commands.add(obj.getCommand());
        }
    }
    
    public void visit(UnaryFromClause obj) {
        if (!embeddedOnly && obj.getExpandedCommand() != null) {
            this.commands.add(obj.getExpandedCommand());
        }
    }
    
    public void visit(SetQuery obj) {
        if (!nonEmbeddedOnly) {
            
            for (QueryCommand command : obj.getQueryCommands()) {
                if (command instanceof SetQuery) {
                    visit((SetQuery)command);
                } else {
                    this.commands.addAll(command.getSubCommands());
                }
            }
        }
    }
    
    public void visit(BatchedUpdateCommand obj) {
        if (!embeddedOnly) {
            this.commands.addAll(obj.getUpdateCommands());
        }
    }
    
    public void visit(ProcedureContainer obj) {
        if (!embeddedOnly && obj.getSubCommand() != null) {
            this.commands.add(obj.getSubCommand());
        }
    }
    
    /**
     * Helper to quickly get the commands from obj
     * @param obj Language object
     * @param elements Collection to collect commands in
     */
    public static final List getCommands(LanguageObject obj) {
        return getCommands(obj, false, false);
    }

    public static final List getCommands(LanguageObject obj, boolean embeddedOnly) {
        return getCommands(obj, embeddedOnly, false);
    }
    
    private static final List getCommands(LanguageObject obj, boolean embeddedOnly, boolean nonEmbeddedOnly) {
        CommandCollectorVisitor visitor = new CommandCollectorVisitor(embeddedOnly, nonEmbeddedOnly);
        
        //we need a special navigator here to prevent subcommands in statements from being picked up
        //by the wrong parent
        PreOrderNavigator navigator = new PreOrderNavigator(visitor) {
            public void visit(LoopStatement obj) {
                preVisitVisitor(obj);
                visitNode(obj.getBlock());
            }
            
            public void visit(CommandStatement obj) {
                preVisitVisitor(obj);
            }
            
            public void visit(AssignmentStatement obj) {
                preVisitVisitor(obj);
                if (obj.hasExpression()) {
                    visitNode(obj.getExpression());
                }
            }

            public void visit(DeclareStatement obj) {
            	preVisitVisitor(obj);
            	if (obj.hasExpression()) {
            		visitNode(obj.getExpression());
            	}
            }
            
            public void visit(SetQuery obj) {
            	preVisitVisitor(obj);
                visitNode(obj.getOrderBy());
                visitNode(obj.getLimit());
                visitNode(obj.getOption());
            }
            
        };
        obj.acceptVisitor(navigator);
        return visitor.getCommands();
    }
    
    public static final List getNonEmbeddedCommands(LanguageObject obj) {
        return getCommands(obj, false, true);
    }

}
