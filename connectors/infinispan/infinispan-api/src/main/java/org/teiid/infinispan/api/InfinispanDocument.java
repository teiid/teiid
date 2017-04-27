/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.infinispan.api;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.translator.document.Document;

public class InfinispanDocument extends Document {
    private TreeMap<Integer, TableWireFormat> wireMap;
    private boolean matched = true;
    private Map<String, Stats> statsMap = new HashMap<>();

    static class Stats {
        AtomicInteger matched = new AtomicInteger(0);
        AtomicInteger unmatched = new AtomicInteger(0);
    }

    public InfinispanDocument(String name, TreeMap<Integer, TableWireFormat> columnMap, InfinispanDocument parent) {
        super(name, false, parent);
        this.wireMap = columnMap;
    }

    public void addProperty(int wireType, Object value) {
        TableWireFormat twf = this.wireMap.get(wireType);
        if (twf.isArrayType()) {
            super.addArrayProperty(twf.getColumnName(), value);
        } else {
            super.addProperty(twf.getColumnName(), value);
        }
    }

    public TreeMap<Integer, TableWireFormat> getWireMap() {
        return wireMap;
    }

    public boolean isMatched() {
        return matched;
    }

    public void setMatched(boolean matched) {
        this.matched = matched;
    }

    public void incrementUpdateCount(String childName, boolean matched) {
        Stats s = statsMap.get(childName);
        if (s == null) {
            s = new Stats();
            statsMap.put(childName, s);
        }

        if (matched) {
            s.matched.incrementAndGet();
        } else {
            s.unmatched.incrementAndGet();
        }
    }

    public int getUpdateCount(String childName, boolean matched) {
        Stats s = statsMap.get(childName);
        if (s == null) {
            return 0;
        }

        if (matched) {
            return s.matched.get();
        } else {
            return s.unmatched.get();
        }
    }
}
