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

import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents a SQL Update statement of the form:
 * "UPDATE &lt;group&gt; SET &lt;element&gt; = &lt;expression&gt;, ... [WHERE &lt;criteria&gt;]".
 */
public class Update extends ProcedureContainer implements FilteredCommand {

    /** Identifies the group to be updated. */
    private GroupSymbol group;

    private SetClauseList changeList = new SetClauseList();

    /** optional criteria defining which row get updated. */
    private Criteria criteria;

    private Criteria constraint;

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    /**
     * Constructs a default instance of this class.
     */
    public Update() {
    }

    /**
     * Return type of command.
     * @return TYPE_UPDATE
     */
    public int getType() {
        return Command.TYPE_UPDATE;
    }

    /**
     * Construct with group and change list
     * @param group Group to by updated
     * @param changeList List of CompareCriteria that represent Element and expression updates
     */
    public Update(GroupSymbol group, SetClauseList changeList) {
        this.group = group;
        this.changeList = changeList;
    }

    /**
     * Construct with group, change list, and criteria
     * @param group DataGroupID that represents the group being updated
     * @param changeList of changeCriteria that represent Element and value pairings
     * @param criteria Criteria that defines what rows get updated
     */
    public Update(GroupSymbol group, SetClauseList changeList, Criteria criteria) {
        this(group, changeList);
        this.criteria = criteria;
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================

    /**
     * Returns the group being updated
     * @return Group being updated
     */
    public GroupSymbol getGroup() {
        return group;
    }

    /**
     * Set the group being updated
     * @param group Group being updated
     */
    public void setGroup(GroupSymbol group) {
        this.group = group;
    }

    /**
     * Set the list of CompareCriteria representing updates being made
     * @param changeList List of CompareCriteria
     */
    public void setChangeList(SetClauseList changeList) {
        this.changeList = changeList;
    }

    /**
     * Return the list of CompareCriteria representing updates being made
     * @return List of CompareCriteria
     */
    public SetClauseList getChangeList() {
        return this.changeList;
    }

    /**
     * Add change to change list - a change is represented by a CompareCriteria
     * internally but can be added here as an element and an expression
     * @param id Element to be changed
     * @param value Expression, often a value, being set
     */
    public void addChange(ElementSymbol id, Expression value) {
        changeList.addClause(id, value);
    }

    /**
     * Returns the criteria object for this command, may be null
     * @return Criteria, may be null
     */
    public Criteria getCriteria() {
        return this.criteria;
    }

    /**
     * Set the criteria for this Update command
     * @param criteria Criteria to be associated with this command
     */
    public void setCriteria(Criteria criteria) {
        this.criteria = criteria;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    // =========================================================================
    //                  P A R S E R     M E T H O D S
    // =========================================================================


    /**
     * Get hashcode for command.  WARNING: This hash code relies on the hash codes of the
     * Group, changeList and Criteria clause.  If the command changes, it's hash code will change and
     * it can be lost from collections.  Hash code is only valid after command has been
     * completely constructed.
     * @return Hash code
     */
    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.group);
        myHash = HashCodeUtil.hashCode(myHash, this.changeList);
        if (this.criteria != null) {
            myHash = HashCodeUtil.hashCode(myHash, this.criteria);
        }
        return myHash;
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    /**
     * Compare two update commands for equality.  Will only evaluate to equal if
     * they are IDENTICAL: group is equal, changeList contains same compareCriteria, criteria are in
     * the same exact structure.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof Update)) {
            return false;
        }

        Update other = (Update) obj;

        return
            EquivalenceUtil.areEqual(getGroup(), other.getGroup()) &&
            getChangeList().equals(other.getChangeList()) &&
            sameOptionAndHint(other) &&
            EquivalenceUtil.areEqual(getCriteria(), other.getCriteria());
    }

    /**
     * Return a copy of this Update.
     * @return Deep clone
     */
    public Object clone() {
        Update copy = new Update();

        if(group != null) {
            copy.setGroup(group.clone());
        }

        copy.setChangeList((SetClauseList)this.changeList.clone());

        if(criteria != null) {
            copy.setCriteria((Criteria) criteria.clone());
        }

        this.copyMetadataState(copy);
        if (this.constraint != null) {
            copy.constraint = (Criteria) this.constraint.clone();
        }
        return copy;
    }

    /**
     * Get the ordered list of all elements returned by this query.  These elements
     * may be ElementSymbols or ExpressionSymbols but in all cases each represents a
     * single column.
     * @return Ordered list of SingleElementSymbol
     */
    public List getProjectedSymbols(){
        return Command.getUpdateCommandSymbol();
    }

    /**
     * @see org.teiid.query.sql.lang.Command#areResultsCachable()
     */
    public boolean areResultsCachable(){
        return false;
    }

    public Criteria getConstraint() {
        return constraint;
    }

    public void setConstraint(Criteria constraint) {
        this.constraint = constraint;
    }

}


