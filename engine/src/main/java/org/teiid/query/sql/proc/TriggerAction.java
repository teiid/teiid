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

package org.teiid.query.sql.proc;

import java.util.List;

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class TriggerAction extends Command {

    private GroupSymbol view;
    private Block block;

    public TriggerAction(Block b) {
        this.setBlock(b);
    }

    public Block getBlock() {
        return block;
    }

    public void setBlock(Block block) {
        block.setAtomic(true);
        this.block = block;
    }

    public GroupSymbol getView() {
        return view;
    }

    public void setView(GroupSymbol view) {
        this.view = view;
    }

    @Override
    public int hashCode() {
        return block.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TriggerAction)) {
            return false;
        }
        TriggerAction other = (TriggerAction) obj;
        return block.equals(other.block);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public TriggerAction clone() {
        TriggerAction clone = new TriggerAction(this.block.clone());
        if (this.view != null) {
            clone.setView(view.clone());
        }
        return clone;
    }

    @Override
    public boolean areResultsCachable() {
        return false;
    }

    @Override
    public List<Expression> getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
    }

    @Override
    public int getType() {
        return Command.TYPE_TRIGGER_ACTION;
    }

}
