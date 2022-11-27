/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.List;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.symbol.ScalarSubquery;


/**
 * <p>This visitor class will traverse a language object tree and collect all sub-commands
 * it finds.  It uses a List to collect the sub-commands in the order they're found.
 *
 * <p>The easiest way to use this visitor is to call the static methods which create
 * the visitor, run the visitor, and get the collection.
 * The public visit() methods should NOT be called directly.
 */
public class CommandCollectorVisitor extends LanguageVisitor {

    private List<Command> commands = new ArrayList<Command>();
    private boolean collectExpanded;

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
        if (obj.getCommand() != null) {
            this.commands.add(obj.getCommand());
        }
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
    public void visit(WithQueryCommand obj) {
        this.commands.add(obj.getCommand());
    }

    @Override
    public void visit(Insert obj) {
        if (obj.getQueryExpression() != null) {
            this.commands.add(obj.getQueryExpression());
        }
    }

    @Override
    public void visit(UnaryFromClause obj) {
        if (collectExpanded && obj.getExpandedCommand() != null && !obj.getGroup().isProcedure()) {
            this.commands.add(obj.getExpandedCommand());
        }
    }

    /**
     * Helper to quickly get the commands from obj
     * @param command Language object
     */
    public static final List<Command> getCommands(Command command) {
        return getCommands(command, false);
    }

    public static final List<Command> getCommands(Command command, boolean includeExpanded) {
        CommandCollectorVisitor visitor = new CommandCollectorVisitor();
        visitor.collectExpanded = includeExpanded;
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
