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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents a FROM clause in a SELECT query.  The from clause holds a set of
 * FROM subclauses.  Each FROM subclause can be either a single group
 * ({@link UnaryFromClause}) or a join predicate ({@link JoinPredicate}).
 */
public class From implements LanguageObject {

    private List<FromClause> clauses;

    /**
     * Constructs a default instance of this class.
     */
    public From() {
        clauses = new ArrayList<FromClause>();
    }

    /**
     * Constructs an instance of this class from an ordered set of from clauses
     * @param parameters The ordered list of from clauses
     */
    public From( List<? extends FromClause> parameters ) {
        clauses = new ArrayList<FromClause>( parameters );
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================

    /**
     * Add a clause to the FROM
     * @param clause Add a clause to the FROM
     */
    public void addClause(FromClause clause) {
        this.clauses.add(clause);
    }

    /**
     * Add clauses to the FROM
     * @param toAdd Collection of {@link FromClause}s
     */
    public void addClauses(Collection<? extends FromClause> toAdd) {
        this.clauses.addAll(toAdd);
    }

    /**
     * Get all the clauses in FROM
     * @return List of {@link FromClause}
     */
    public List<FromClause> getClauses() {
        return this.clauses;
    }

    /**
     * Set all the clauses
     * @param clauses List of {@link FromClause}
     */
    public void setClauses(List<FromClause> clauses) {
        this.clauses = clauses;
    }


    /**
     * Adds a new group to the list (it will be wrapped in a UnaryFromClause)
     * @param group Group to add
     */
    public void addGroup( GroupSymbol group ) {
        if( group != null ) {
            clauses.add(new UnaryFromClause(group));
        }
    }

    /**
     * Adds a new collection of groups to the list
     * @param groups Collection of {@link GroupSymbol}
     */
    public void addGroups( Collection<GroupSymbol> groups ) {
        if(groups != null) {
            for (GroupSymbol groupSymbol : groups) {
                clauses.add(new UnaryFromClause(groupSymbol));
            }
        }
    }

    /**
     * Returns an ordered list of the groups in all sub-clauses.
     * @return List of {@link GroupSymbol}
     */
    public List<GroupSymbol> getGroups() {
        List<GroupSymbol> groups = new ArrayList<GroupSymbol>();
        if(clauses != null) {
            for(int i=0; i<clauses.size(); i++) {
                FromClause clause = clauses.get(i);
                clause.collectGroups(groups);
            }
        }

        return groups;
    }

    /**
     * Checks if a group is in the From
     * @param group Group to check for
     * @return True if the From contains the group
     */
    public boolean containsGroup( GroupSymbol group ) {
        return getGroups().contains(group);
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    // =========================================================================
    //          O V E R R I D D E N     O B J E C T     M E T H O D S
    // =========================================================================

    /**
     * Return copy of this From clause.
     */
    public Object clone() {
        return new From(LanguageObject.Util.deepClone(clauses, FromClause.class));
    }

    /**
     * Compare two Froms for equality.  Order is not important in the from, so
     * this is a set comparison.
     */
    public boolean equals(Object obj) {

        if(obj == this) {
            return true;
        }

        if(!(obj instanceof From)) {
            return false;
        }

        return EquivalenceUtil.areEqual(getClauses(), ((From)obj).getClauses());
       }

    /**
     * Get hashcode for From.  WARNING: The hash code relies on the variables
     * in the select, so changing the variables will change the hash code, causing
     * a select to be lost in a hash structure.  Do not hash a From if you plan
     * to change it.
     */
    public int hashCode() {
        return HashCodeUtil.hashCode(0, getGroups());
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
