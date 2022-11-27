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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.teiid.query.sql.symbol.GroupSymbol;


/**
 *  A GroupContext represents a set of groups in a hierarchy that determines
 *  resolving order.
 */
public class GroupContext implements Cloneable {

    private Collection<GroupSymbol> groups;

    private GroupContext parent;

    public GroupContext() {
        this(null, null);
    }

    public GroupContext(GroupContext parent, Collection<GroupSymbol> groups) {
        this.parent = parent;
        if (groups == null) {
            this.groups = new LinkedList<GroupSymbol>();
        } else {
            this.groups = groups;
        }
    }

    public Collection<GroupSymbol> getGroups() {
        return this.groups;
    }

    public void addGroup(GroupSymbol symbol) {
        this.groups.add(symbol);
    }

    public GroupContext getParent() {
        return this.parent;
    }

    /**
     * Flattens all contexts to a single list
     *
     * @return
     */
    public List<GroupSymbol> getAllGroups() {
        LinkedList<GroupSymbol> result = new LinkedList<GroupSymbol>();

        GroupContext root = this;
        while (root != null) {
            result.addAll(root.getGroups());
            root = root.getParent();
        }

        return result;
    }

    public GroupContext clone() {
        return new GroupContext(parent, new LinkedList<GroupSymbol>(groups));
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        String result = groups.toString();

        if (parent != null) {
            result += "\n" + parent.toString(); //$NON-NLS-1$
        }

        return result;
    }

}
