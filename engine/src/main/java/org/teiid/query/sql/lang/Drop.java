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
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;



/**
 * @since 5.5
 */
public class Drop extends Command implements TargetedCommand {
    /** Identifies the table to be dropped. */
    private GroupSymbol table;

    public GroupSymbol getTable() {
        return table;
    }

    @Override
    public GroupSymbol getGroup() {
        return table;
    }

    public void setTable(GroupSymbol table) {
        this.table = table;
    }

    public int getType() {
        return Command.TYPE_DROP;
    }

    public Drop clone() {
        Drop drop =  new Drop();
        GroupSymbol copyTable = table.clone();
        drop.setTable(copyTable);
        copyMetadataState(drop);
        return drop;
    }

    public List getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
    }

    public boolean areResultsCachable() {
        return false;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.table);
        return myHash;
    }

    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof Drop)) {
            return false;
        }

        Drop other = (Drop) obj;

        return EquivalenceUtil.areEqual(getTable(), other.getTable());
    }
}
