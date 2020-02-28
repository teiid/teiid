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

package org.teiid.query.sql.lang;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * A Command is an interface for all the language objects that are at the root
 * of a language object tree representing a SQL statement.  For instance, a
 * Query command represents a SQL select query, an Update command represents a
 * SQL update statement, etc.
 */
public abstract class Command implements LanguageObject {

    /**
     * Represents an unknown type of command
     */
    public static final int TYPE_UNKNOWN = 0;

    /**
     * Represents a SQL SELECT statement
     */
    public static final int TYPE_QUERY = 1;

    /**
     * Represents a SQL INSERT statement
     */
    public static final int TYPE_INSERT = 2;

    /**
     * Represents a SQL UPDATE statement
     */
    public static final int TYPE_UPDATE = 3;

    /**
     * Represents a SQL DELETE statement
     */
    public static final int TYPE_DELETE = 4;

    /**
     * Represents a stored procedure command
     */
    public static final int TYPE_STORED_PROCEDURE = 6;

    /**
     * Represents a update stored procedure command
     */
    public static final int TYPE_UPDATE_PROCEDURE = 7;

    /**
     * Represents a batched sequence of UPDATE statements
     */
    public static final int TYPE_BATCHED_UPDATE = 9;

    public static final int TYPE_DYNAMIC = 10;

    public static final int TYPE_CREATE = 11;

    public static final int TYPE_DROP = 12;

    public static final int TYPE_TRIGGER_ACTION = 13;

    public static final int TYPE_ALTER_VIEW = 14;

    public static final int TYPE_ALTER_PROC = 15;

    public static final int TYPE_ALTER_TRIGGER = 16;

    public static final int TYPE_EXPLAIN = 17;

    public static final int TYPE_SOURCE_EVENT = -1;

    private static List<Expression> updateCommandSymbol;

    /**
     * All temporary group IDs discovered while resolving this
     * command.  The key is a TempMetadataID and the value is an
     * ordered List of TempMetadataID representing the elements.
     */
    protected TempMetadataStore tempGroupIDs;

    private transient GroupContext externalGroups;

    /** The option clause */
    private Option option;

    private ProcessorPlan plan;

    private SymbolMap correlatedReferences;

    private CacheHint cacheHint;
    private SourceHint sourceHint;

    /**
     * Return type of command to make it easier to build switch statements by command type.
     * @return Type from TYPE constants
     */
    public abstract int getType();

    /**
     * Get the correlated references to the containing scope only
     * @return
     */
    public SymbolMap getCorrelatedReferences() {
        return correlatedReferences;
    }

    public void setCorrelatedReferences(SymbolMap correlatedReferences) {
        this.correlatedReferences = correlatedReferences;
    }

    public void setTemporaryMetadata(TempMetadataStore metadata) {
        this.tempGroupIDs = metadata;
    }

    public TempMetadataStore getTemporaryMetadata() {
        return this.tempGroupIDs;
    }

    public void addExternalGroupToContext(GroupSymbol group) {
        getExternalGroupContexts().addGroup(group);
    }

    public void setExternalGroupContexts(GroupContext root) {
        if (root == null) {
            this.externalGroups = null;
        } else {
            this.externalGroups = root.clone();
        }
    }

    public void pushNewResolvingContext(Collection<GroupSymbol> groups) {
        externalGroups = new GroupContext(externalGroups, new LinkedList<GroupSymbol>(groups));
    }

    public GroupContext getExternalGroupContexts() {
        if (externalGroups == null) {
            this.externalGroups = new GroupContext();
        }
        return this.externalGroups;
    }

    public List<GroupSymbol> getAllExternalGroups() {
        if (externalGroups == null) {
            return Collections.emptyList();
        }

        return externalGroups.getAllGroups();
    }

    public abstract Object clone();

    protected void copyMetadataState(Command copy) {
        if(this.getExternalGroupContexts() != null) {
            copy.externalGroups = this.externalGroups.clone();
        }
        if(this.tempGroupIDs != null) {
            copy.setTemporaryMetadata(this.tempGroupIDs.clone());
        }

        copy.plan = this.plan;
        if (this.correlatedReferences != null) {
            copy.correlatedReferences = this.correlatedReferences.clone();
        }
        if(this.getOption() != null) {
            copy.setOption( (Option) this.getOption().clone() );
        }
        copy.cacheHint = this.cacheHint;
        copy.sourceHint = this.sourceHint;
    }

    /**
     * Print the full tree of commands with indentation - useful for debugging
     * @return String String representation of command tree
     */
    public String printCommandTree() {
        StringBuffer str = new StringBuffer();
        printCommandTree(str, 0);
        return str.toString();
    }

    /**
     * Helper method to print command tree at given tab level
     * @param str String buffer to add command sub tree to
     * @param tabLevel Number of tabs to print this command at
     */
    protected void printCommandTree(StringBuffer str, int tabLevel) {
        // Add tabs
        for(int i=0; i<tabLevel; i++) {
            str.append("\t"); //$NON-NLS-1$
        }

        // Add this command
        str.append(toString());
        str.append("\n"); //$NON-NLS-1$

        // Add children recursively
        tabLevel++;
        for (Command subCommand : CommandCollectorVisitor.getCommands(this)) {
            subCommand.printCommandTree(str, tabLevel);
        }
    }

    // =========================================================================
    //                     O P T I O N      M E T H O D S
    // =========================================================================

    /**
     * Get the option clause for the query.
     * @return option clause
     */
    public Option getOption() {
        return option;
    }

    /**
     * Set the option clause for the query.
     * @param option New option clause
     */
    public void setOption(Option option) {
        this.option = option;
    }

    /**
     * Get the ordered list of all elements returned by this query.  These elements
     * may be ElementSymbols or ExpressionSymbols but in all cases each represents a
     * single column.
     * @return Ordered list of SingleElementSymbol
     */
    public abstract List<Expression> getProjectedSymbols();

    /**
     * Whether the results are cachable.
     * @return True if the results are cachable; false otherwise.
     */
    public abstract boolean areResultsCachable();

    public static List<Expression> getUpdateCommandSymbol() {
        if (updateCommandSymbol == null ) {
            ElementSymbol symbol = new ElementSymbol("Count"); //$NON-NLS-1$
            symbol.setType(DataTypeManager.DefaultDataClasses.INTEGER);
            updateCommandSymbol = Arrays.asList((Expression)symbol);
        }
        return updateCommandSymbol;
    }

    public ProcessorPlan getProcessorPlan() {
        return this.plan;
    }

    public void setProcessorPlan(ProcessorPlan plan) {
        this.plan = plan;
    }

    public CacheHint getCacheHint() {
        return cacheHint;
    }

    public void setCacheHint(CacheHint cacheHint) {
        this.cacheHint = cacheHint;
    }

    public SourceHint getSourceHint() {
        return sourceHint;
    }

    public void setSourceHint(SourceHint sourceHint) {
        this.sourceHint = sourceHint;
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    protected boolean sameOptionAndHint(Command cmd) {
        return EquivalenceUtil.areEqual(this.cacheHint, cmd.cacheHint) &&
        EquivalenceUtil.areEqual(this.option, cmd.option);
    }

    public boolean returnsResultSet() {
        return false;
    }

    /**
     * @return null if unknown, empty if results are not returned, or the resultset columns
     */
    public List<? extends Expression> getResultSetColumns() {
        if (returnsResultSet()) {
            return getProjectedSymbols();
        }
        return Collections.emptyList();
    }

    //TODO: replace with enum
    public static String getCommandToken(int commandType) {
        switch (commandType) {
        case Command.TYPE_INSERT:
            return "I"; //$NON-NLS-1$
        case Command.TYPE_UPDATE:
            return "U"; //$NON-NLS-1$
        case Command.TYPE_DELETE:
            return "D"; //$NON-NLS-1$
        case Command.TYPE_DROP:
            return "DT"; //$NON-NLS-1$
        case Command.TYPE_ALTER_PROC:
            return "AP"; //$NON-NLS-1$
        case Command.TYPE_ALTER_TRIGGER:
            return "AT"; //$NON-NLS-1$
        case Command.TYPE_ALTER_VIEW:
            return "AV"; //$NON-NLS-1$
        case Command.TYPE_CREATE:
            return "CT"; //$NON-NLS-1$
        case Command.TYPE_DYNAMIC:
            return "Dy"; //$NON-NLS-1$
        case Command.TYPE_QUERY:
            return "S"; //$NON-NLS-1$
        case Command.TYPE_STORED_PROCEDURE:
            return "Sp"; //$NON-NLS-1$
        case Command.TYPE_UPDATE_PROCEDURE:
            return "Up"; //$NON-NLS-1$
        }
        return "?"; //$NON-NLS-1$
    }

    /**
     * For a statement such as explain, obtain the actual command
     * @return
     */
    public Command getActualCommand() {
        return this;
    }

}
