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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.metadata.Table;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.proc.TriggerAction;

public class AlterTrigger extends Alter<TriggerAction> {

    private Table.TriggerEvent event;
    private Boolean enabled;
    private boolean create;
    private boolean after;
    private String name;

    public Table.TriggerEvent getEvent() {
        return event;
    }

    public void setEvent(Table.TriggerEvent operation) {
        this.event = operation;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public AlterTrigger clone() {
        AlterTrigger clone = new AlterTrigger();
        cloneOnTo(clone);
        clone.event = event;
        clone.enabled = this.enabled;
        clone.create = this.create;
        clone.after = this.after;
        clone.name = this.name;
        return clone;
    }

    @Override
    public int getType() {
        return TYPE_ALTER_TRIGGER;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        AlterTrigger other = (AlterTrigger)obj;
        return EquivalenceUtil.areEqual(this.enabled, other.enabled)
        && EquivalenceUtil.areEqual(this.name, other.name)
        && this.create == other.create
        && other.event == this.event
        && other.after == this.after;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public boolean isCreate() {
        return create;
    }

    public void setCreate(boolean create) {
        this.create = create;
    }

    public boolean isAfter() {
        return after;
    }

    public void setAfter(boolean after) {
        this.after = after;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
