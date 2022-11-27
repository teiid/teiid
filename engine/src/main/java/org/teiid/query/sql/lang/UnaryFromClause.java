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

import java.util.Collection;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 * A FROM subpart that represents a single group.  For example, the FROM clause:
 * "FROM a, b" will have two UnaryFromClause objects, each holding a reference to
 * a GroupSymbol (for a and b).
 */
public class UnaryFromClause extends FromClause {

    private GroupSymbol group;

    private Command expandedCommand;

    /**
     * Construct default object
     */
    public UnaryFromClause() {
    }

    /**
     * Construct object with specified group
     * @param group Group being held
     */
    public UnaryFromClause(GroupSymbol group) {
        this.group = group;
    }

    /**
     * Set the group held by the clause
     * @param group Group to hold
     */
    public void setGroup(GroupSymbol group) {
        this.group = group;
    }

    /**
     * Get group held by clause
     * @return Group held by clause
     */
    public GroupSymbol getGroup() {
        return this.group;
    }

    /**
     * Collect all GroupSymbols for this from clause.
     * @param groups Groups to add to
     */
    public void collectGroups(Collection<GroupSymbol> groups) {
        groups.add(this.group);
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Check whether objects are equal
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        if(! (obj instanceof UnaryFromClause)) {
            return false;
        }

        UnaryFromClause other = (UnaryFromClause)obj;

        if( EquivalenceUtil.areEqual(group.getDefinition(), other.getGroup().getDefinition()) ) {
            return EquivalenceUtil.areEqual(getGroup().getNonCorrelationName(), other.getGroup().getNonCorrelationName()) &&
            other.isOptional() == this.isOptional() &&
            EquivalenceUtil.areEqual(expandedCommand, other.expandedCommand);
        }
        return false;
    }

    /**
     * Get hash code of object
     * @return Hash code
     */
    public int hashCode() {
        if(this.group == null) {
            return 0;
        }
        return this.group.hashCode();
    }

    /**
     * Get deep clone of object
     * @return Deep copy of the object
     */
    public FromClause cloneDirect() {
        GroupSymbol copyGroup = null;
        if(this.group != null) {
            copyGroup = this.group.clone();
        }
        UnaryFromClause clonedUnaryFromClause = new UnaryFromClause(copyGroup);
        if (this.expandedCommand != null) {
            clonedUnaryFromClause.setExpandedCommand((Command)this.expandedCommand.clone());
        }
        return clonedUnaryFromClause;
    }

    /**
     * @return Returns the expandedCommand.
     */
    public Command getExpandedCommand() {
        return this.expandedCommand;
    }

    /**
     * @param expandedCommand The expandedCommand to set.
     */
    public void setExpandedCommand(Command expandedCommand) {
        this.expandedCommand = expandedCommand;
    }

}
