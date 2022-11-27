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

package org.teiid.common.buffer;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

import org.teiid.client.ResizingArrayList;

/**
 * Creates a copy of a reference list when modified.
 *
 * @param <T>
 */
public class LightWeightCopyOnWriteList<T> extends AbstractList<T> implements RandomAccess {

    private List<T> list;
    private boolean modified;

    public LightWeightCopyOnWriteList(List<T> list) {
        this.list = list;
    }

    @Override
    public T get(int index) {
        return list.get(index);
    }

    public List<T> getList() {
        return list;
    }

    public void add(int index, T element) {
        if (!modified) {
            List<T> next = new ArrayList<T>(list.size() + 1);
            next.addAll(list);
            list = next;
            modified = true;
        }
        list.add(index, element);
    }

    public T set(int index, T element) {
        checkModified();
        return list.set(index, element);
    }

    private void checkModified() {
        if (!modified) {
            list = new ArrayList<T>(list);
            modified = true;
        }
    }

    public boolean addAll(Collection<? extends T> c) {
        return addAll(size(), c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        checkModified();
        return list.addAll(index, c);
    }

    @Override
    public T remove(int index) {
        checkModified();
        return list.remove(index);
    }

    @Override
    public Object[] toArray() {
        return list.toArray();
    }

    public <U extends Object> U[] toArray(U[] a) {
        return list.toArray(a);
    }

    @Override
    public void clear() {
        if (!modified) {
            list = new ResizingArrayList<T>();
            modified = true;
        } else {
            list.clear();
        }
    }

    @Override
    public int size() {
        return list.size();
    }

}
