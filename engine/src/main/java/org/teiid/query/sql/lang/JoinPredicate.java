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
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 * Represents a subpart of the FROM clause specifying a join within the FROM.  It may
 * optionally specify criteria for the join, depending on the join type.  For example,
 * these are some representations of JoinPredicates:
 * <OL>
 * <LI>GroupA CROSS JOIN GroupB</LI>
 * <LI>GroupA JOIN GroupB ON GroupA.E1=GroupB.E1 AND GroupA.E2=GroupB.E2</LI>
 * <LI>GroupA RIGHT OUTER JOIN (GroupB JOIN GroupC ON GroupB.E1=GroupC.E1) ON GroupA.E1=GroupB.E1</LI>
 * </OL>
 */
public class JoinPredicate extends FromClause {

    private FromClause leftClause;
    private FromClause rightClause;
    private JoinType joinType = JoinType.JOIN_INNER;
    private List joinCriteria;

    /**
     * Construct a JoinPredicate
     */
    public JoinPredicate() {
        this.joinCriteria = new ArrayList();
    }

    /**
     * Construct a JoinPredicate between two clauses of the specified type.
     * @param leftClause Left from clause
     * @param rightClause Right from clause
     * @param type Type of join
     */
    public JoinPredicate(FromClause leftClause, FromClause rightClause, JoinType type) {
        this.leftClause = leftClause;
        this.rightClause = rightClause;
        this.joinType = type;
         this.joinCriteria = new ArrayList();
    }

    /**
     * Construct a JoinPredicate between two clauses of the specified type.
     * @param leftClause Left from clause
     * @param rightClause Right from clause
     * @param type Type of join
     * @param criteria List of Criteria for this join predicate
     */
    public JoinPredicate(FromClause leftClause, FromClause rightClause, JoinType type, List criteria) {
        this.leftClause = leftClause;
        this.rightClause = rightClause;
        this.joinType = type;
        this.joinCriteria = criteria;
    }

    /**
     * Construct a JoinPredicate between two clauses of the specified type.
     * @param leftClause Left from clause
     * @param rightClause Right from clause
     * @param type Type of join
     * @param criteria List of Criteria for this join predicate
     */
    public JoinPredicate(FromClause leftClause, FromClause rightClause, JoinType type, Criteria criteria) {
        this.leftClause = leftClause;
        this.rightClause = rightClause;
        this.joinType = type;
        this.joinCriteria = Criteria.separateCriteriaByAnd(criteria);
    }

    /**
     * Set left clause
     * @param predicate Left clause to set
     */
    public void setLeftClause(FromClause predicate) {
        this.leftClause = predicate;
    }

    /**
     * Get left clause
     * @return Left clause
     */
    public FromClause getLeftClause() {
        return this.leftClause;
    }

    /**
     * Set right clause
     * @param predicate Right clause to set
     */
    public void setRightClause(FromClause predicate) {
        this.rightClause = predicate;
    }

    /**
     * Get right clause
     * @return Right clause
     */
    public FromClause getRightClause() {
        return this.rightClause;
    }

    /**
     * Set join type for this predicate
     * @param type Type of join
     */
    public void setJoinType(JoinType type) {
        this.joinType = type;
    }

    /**
     * Get join type for this predicate
     * @return Type of join
     */
    public JoinType getJoinType() {
        return this.joinType;
    }

    /**
     * Set join criteria for this predicate
     * @param criteria List of {@link Criteria} set on this predicate
     */
    public void setJoinCriteria(List criteria) {
        this.joinCriteria = criteria;
    }

    /**
     * Get join criteria for this predicate
     * @return List of {@link Criteria}
     */
    public List getJoinCriteria() {
        return this.joinCriteria;
    }

    /**
     * Collect all GroupSymbols for this from clause.
     * @param groups Groups to add to
     */
    public void collectGroups(Collection<GroupSymbol> groups) {
        if(this.leftClause != null) {
            this.leftClause.collectGroups(groups);
        }
        if(this.rightClause != null) {
            this.rightClause.collectGroups(groups);
        }
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Compare this object to another
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        if(!(obj instanceof JoinPredicate)) {
            return false;
        }
        JoinPredicate other = (JoinPredicate) obj;

        List thisCrit = this.getJoinCriteria();
        if(thisCrit != null && thisCrit.size() == 0) {
            thisCrit = null;
        }
        List otherCrit = other.getJoinCriteria();
        if(otherCrit != null && otherCrit.size() == 0) {
            otherCrit = null;
        }

        return EquivalenceUtil.areEqual(other.getJoinType(), this.getJoinType()) &&
               EquivalenceUtil.areEqual(other.getLeftClause(), this.getLeftClause()) &&
               EquivalenceUtil.areEqual(other.getRightClause(), this.getRightClause()) &&
               EquivalenceUtil.areEqual(otherCrit, thisCrit);
    }

    /**
     * Get hash code for object
     * @return Hash code
     */
    public int hashCode() {
        int hash = HashCodeUtil.hashCode(0, getLeftClause());
        hash = HashCodeUtil.hashCode(hash, getJoinType().getTypeCode());
        hash = HashCodeUtil.hashCode(hash, getRightClause());
        return hash;
    }

    /**
     * Return deep clone for object
     * @return Deep clone
     */
    protected FromClause cloneDirect() {
        FromClause copyLeft = null;
        if(this.leftClause != null) {
            copyLeft = this.leftClause.clone();
        }

        FromClause copyRight = null;
        if(this.rightClause != null) {
            copyRight = this.rightClause.clone();
        }

        List copyCrits = null;
        if(this.joinCriteria != null) {
            copyCrits = new ArrayList(joinCriteria.size());
            Iterator iter = this.joinCriteria.iterator();
            while(iter.hasNext()) {
                Criteria crit = (Criteria) iter.next();
                copyCrits.add(crit.clone());
            }
        }

        JoinPredicate clonedJoinPredicate = new JoinPredicate(copyLeft, copyRight, this.joinType, copyCrits);
        return clonedJoinPredicate;
    }

}
