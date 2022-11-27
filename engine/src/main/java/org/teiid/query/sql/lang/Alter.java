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
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;

public abstract class Alter<T extends Command> extends Command {

    private GroupSymbol target;
    private T definition;

    public GroupSymbol getTarget() {
        return target;
    }

    public void setTarget(GroupSymbol target) {
        this.target = target;
    }

    public T getDefinition() {
        return definition;
    }

    public void setDefinition(T definition) {
        this.definition = definition;
    }

    @Override
    public boolean areResultsCachable() {
        return false;
    }

    @Override
    public List<Expression> getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
    }

    public void cloneOnTo(Alter<T> clone) {
        copyMetadataState(clone);
        if (this.definition != null) {
            clone.setDefinition((T)this.definition.clone());
        }
        clone.setTarget(getTarget().clone());
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(this.target.hashCode(), this.definition);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        Alter<?> other = (Alter<?>)obj;
        return EquivalenceUtil.areEqual(this.target, other.target)
        && EquivalenceUtil.areEqual(this.definition, other.definition);
    }

}
