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

 package org.teiid.query.sql.symbol;

import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class WindowSpecification implements LanguageObject {

    private List<Expression> partition;
    private OrderBy orderBy;
    private WindowFrame windowFrame;

    public WindowSpecification() {

    }

    public List<Expression> getPartition() {
        return partition;
    }

    public void setPartition(List<Expression> grouping) {
        this.partition = grouping;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public WindowFrame getWindowFrame() {
        return windowFrame;
    }

    public void setWindowFrame(WindowFrame frame) {
        this.windowFrame = frame;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(0, partition, orderBy, windowFrame);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof WindowSpecification)) {
            return false;
        }
        WindowSpecification other = (WindowSpecification)obj;
        return EquivalenceUtil.areEqual(this.partition, other.partition) &&
        EquivalenceUtil.areEqual(this.orderBy, other.orderBy) &&
        EquivalenceUtil.areEqual(this.windowFrame, other.windowFrame);
    }

    @Override
    public WindowSpecification clone() {
        WindowSpecification clone = new WindowSpecification();
        if (this.partition != null) {
            clone.setPartition(LanguageObject.Util.deepClone(this.partition, Expression.class));
        }
        if (this.orderBy != null) {
            clone.setOrderBy(this.orderBy.clone());
        }
        if (this.windowFrame != null) {
            clone.setWindowFrame(this.windowFrame.clone());
        }
        return clone;
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
