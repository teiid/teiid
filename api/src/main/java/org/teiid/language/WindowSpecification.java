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

import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.language.visitor.LanguageObjectVisitor;

public class WindowSpecification extends BaseLanguageObject {

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

    public void setWindowFrame(WindowFrame windowFrame) {
        this.windowFrame = windowFrame;
    }

    @Override
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(partition.hashCode(), orderBy, windowFrame);
    }

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

}
