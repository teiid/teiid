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
import java.util.LinkedHashMap;
import java.util.List;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;


public class SetClauseList implements LanguageObject {

    private static final long serialVersionUID = 8174681510498719451L;

    private List<SetClause> setClauses;

    public SetClauseList() {
        this.setClauses = new ArrayList<SetClause>();
    }

    public SetClauseList(List<SetClause> setClauses) {
        this.setClauses = setClauses;
    }

    public void addClause(ElementSymbol symbol, Expression expression) {
        this.setClauses.add(new SetClause(symbol, expression));
    }

    public void addClause(SetClause clause) {
        this.setClauses.add(clause);
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    @Override
    public Object clone() {
        SetClauseList copy = new SetClauseList();
        for (SetClause clause : this.setClauses) {
            copy.addClause((SetClause)clause.clone());
        }
        return copy;
    }

    /**
     * @return a non-updateable map representation
     */
    public LinkedHashMap<ElementSymbol, Expression> getClauseMap() {
        LinkedHashMap<ElementSymbol, Expression> result = new LinkedHashMap<ElementSymbol, Expression>();
        for (SetClause clause : this.setClauses) {
            result.put(clause.getSymbol(), clause.getValue());
        }
        return result;
    }

    public List<SetClause> getClauses() {
        return this.setClauses;
    }

    public boolean isEmpty() {
        return this.setClauses.isEmpty();
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(!(obj instanceof SetClauseList)) {
            return false;
        }

        SetClauseList other = (SetClauseList) obj;

        return this.setClauses.equals(other.setClauses);
    }

    @Override
    public int hashCode() {
        return setClauses.hashCode();
    }

}
