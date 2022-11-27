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

package org.teiid.language;

import org.teiid.language.visitor.LanguageObjectVisitor;

public class Join extends BaseLanguageObject implements TableReference {

    public enum JoinType {
        INNER_JOIN,
        CROSS_JOIN,
        LEFT_OUTER_JOIN,
        RIGHT_OUTER_JOIN,
        FULL_OUTER_JOIN
    }

    private TableReference leftItem;
    private TableReference rightItem;
    private JoinType joinType;
    private Condition condition;

    public Join(TableReference left, TableReference right, JoinType joinType, Condition criteria) {
        this.leftItem = left;
        this.rightItem = right;
        this.joinType = joinType;
        this.condition = criteria;
    }

    public TableReference getLeftItem() {
        return leftItem;
    }

    public TableReference getRightItem() {
        return rightItem;
    }

    public JoinType getJoinType() {
        return this.joinType;
    }

    public Condition getCondition() {
        return condition;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setLeftItem(TableReference item) {
        this.leftItem = item;
    }

    public void setRightItem(TableReference item) {
        this.rightItem = item;
    }

    public void setJoinType(JoinType type) {
        this.joinType = type;
    }

    public void setCondition(Condition criteria) {
        this.condition = criteria;
    }

}
