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

import java.util.Collections;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


public class DynamicCommand extends Command {

    private Expression sql;

    private List asColumns;

    private GroupSymbol intoGroup;

    private int updatingModelCount;

    private SetClauseList using;

    private boolean asClauseSet;

    public DynamicCommand() {
        super();
    }

    public DynamicCommand(Expression sql, List columns, GroupSymbol intoGroup, SetClauseList using) {
        super();
        this.sql = sql;
        this.asColumns = columns;
        this.intoGroup = intoGroup;
        this.using = using;
    }

    /**
     * @see org.teiid.query.sql.lang.QueryCommand#clone()
     */
    public Object clone() {
        DynamicCommand clone = new DynamicCommand();

        clone.setSql((Expression)getSql().clone());
        if (asColumns != null) {
            List<ElementSymbol> cloneColumns = LanguageObject.Util.deepClone(asColumns, ElementSymbol.class);
            clone.setAsColumns(cloneColumns);
        }

        if (intoGroup != null) {
            clone.setIntoGroup(intoGroup.clone());
        }

        if (using != null) {
            clone.setUsing((SetClauseList)using.clone());
        }

        clone.setUpdatingModelCount(getUpdatingModelCount());
        copyMetadataState(clone);
        clone.setAsClauseSet(isAsClauseSet());
        return clone;
    }

    /**
     * @see org.teiid.query.sql.lang.Command#getType()
     */
    public int getType() {
        return Command.TYPE_DYNAMIC;
    }

    /**
     * @see org.teiid.query.sql.lang.Command#getProjectedSymbols()
     *
     * Once past resolving, an EMPTY set of project columns indicates that the
     * project columns of the actual command do not need to be checked during
     * processing.
     */
    public List getProjectedSymbols() {
        if (intoGroup != null) {
            return Command.getUpdateCommandSymbol();
        }

        if (asColumns != null) {
            return asColumns;
        }

        return Collections.EMPTY_LIST;
    }

    /**
     * @see org.teiid.query.sql.lang.Command#areResultsCachable()
     */
    public boolean areResultsCachable() {
        return false;
    }

    public void setUpdatingModelCount(int count) {
        if (count < 0) {
            count = 0;
        } else if (count > 2) {
            count = 2;
        }
        this.updatingModelCount = count;
    }

    public int getUpdatingModelCount() {
        return this.updatingModelCount;
    }

    /**
     * @see org.teiid.query.sql.LanguageObject#acceptVisitor(org.teiid.query.sql.LanguageVisitor)
     */
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @return Returns the columns.
     */
    public List getAsColumns() {
        if (this.asColumns == null) {
            return Collections.EMPTY_LIST;
        }
        return this.asColumns;
    }

    /**
     * @param columns The columns to set.
     */
    public void setAsColumns(List columns) {
        this.asColumns = columns;
    }

    /**
     * @return Returns the intoGroup.
     */
    public GroupSymbol getIntoGroup() {
        return this.intoGroup;
    }

    /**
     * @param intoGroup The intoGroup to set.
     */
    public void setIntoGroup(GroupSymbol intoGroup) {
        this.intoGroup = intoGroup;
    }

    /**
     * @return Returns the sql.
     */
    public Expression getSql() {
        return this.sql;
    }

    /**
     * @param sql The sql to set.
     */
    public void setSql(Expression sql) {
        this.sql = sql;
    }

    /**
     * @return Returns the using.
     */
    public SetClauseList getUsing() {
        return this.using;
    }

    /**
     * @param using The using to set.
     */
    public void setUsing(SetClauseList using) {
        this.using = using;
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DynamicCommand)) {
            return false;
        }

        DynamicCommand other = (DynamicCommand)obj;

        return this.updatingModelCount == other.updatingModelCount &&
        EquivalenceUtil.areEqual(getAsColumns(), other.getAsColumns()) &&
        EquivalenceUtil.areEqual(getSql(), other.getSql()) &&
        EquivalenceUtil.areEqual(getIntoGroup(), other.getIntoGroup()) &&
        EquivalenceUtil.areEqual(getUsing(), other.getUsing());
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.sql);
        myHash = HashCodeUtil.hashCode(myHash, this.asColumns);
        return myHash;
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    /**
     * @return Returns the asClauseSet.
     */
    public boolean isAsClauseSet() {
        return this.asClauseSet;
    }

    /**
     * @param asClauseSet The asClauseSet to set.
     */
    public void setAsClauseSet(boolean asClauseSet) {
        this.asClauseSet = asClauseSet;
    }

    @Override
    public boolean returnsResultSet() {
        return intoGroup == null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<? extends Expression> getResultSetColumns() {
        if (returnsResultSet()) {
            return asColumns;
        }
        return Collections.emptyList();
    }

}
