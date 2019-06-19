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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.teiid.client.BatchSerializer;
import org.teiid.common.buffer.LobManager.ReferenceMode;
import org.teiid.common.buffer.SPage.SearchResult;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.Assertion;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.relational.ListNestedSortComparator;

/**
 * Self balancing search tree using skip list like logic
 * This has similar performance similar to a B+/-Tree,
 * but with fewer updates.
 */
@SuppressWarnings("unchecked")
public class STree implements Cloneable {

    public enum InsertMode {ORDERED, NEW, UPDATE}

    private static final Random seedGenerator = new Random(0);

    protected int randomSeed;
    private int mask = 1;
    private int shift = 1;

    protected HashMap<Long, SPage> pages = new HashMap<Long, SPage>();
    protected volatile SPage[] header = new SPage[] {new SPage(this, true)};
    protected BatchManager keyManager;
    protected BatchManager leafManager;
    protected ListNestedSortComparator comparator;
    private int pageSize;
    protected int leafSize;
    protected int minPageSize;
    protected int minStorageSize;
    protected int keyLength;
    protected boolean batchInsert;
    protected SPage incompleteInsert;
    protected LobManager lobManager;

    protected ReentrantLock updateLock = new ReentrantLock();

    private AtomicLong rowCount = new AtomicLong();

    public STree(BatchManager manager,
            BatchManager leafManager,
            final ListNestedSortComparator comparator,
            int pageSize,
            int leafSize,
            int keyLength,
            LobManager lobManager) {
        Assertion.assertTrue(pageSize > 1 && leafSize > 1);
        randomSeed = seedGenerator.nextInt() | 0x00000100; // ensure nonzero
        this.keyManager = manager;
        manager.setPrefersMemory(true);
        this.leafManager = leafManager;
        this.comparator = comparator;
        this.pageSize = pageSize;
        pageSize >>>= 3;
        while (pageSize > 0) {
            pageSize >>>= 1;
            shift++;
            mask <<= 1;
            mask++;
        }
        this.leafSize = leafSize;
        this.keyLength = keyLength;
        this.lobManager = lobManager;
        this.minPageSize = this.pageSize>>5;
        this.minStorageSize = this.pageSize>>2;
    }

    public STree clone() {
        updateLock.lock();
        try {
            STree clone = (STree) super.clone();
            if (lobManager != null) {
                clone.lobManager = lobManager.clone();
            }
            clone.updateLock = new ReentrantLock();
            clone.rowCount = new AtomicLong(rowCount.get());
            //clone the pages
            clone.pages = new HashMap<Long, SPage>(pages);
            for (Map.Entry<Long, SPage> entry : clone.pages.entrySet()) {
                entry.setValue(entry.getValue().clone(clone));
            }
            //reset the pointers
            for (Map.Entry<Long, SPage> entry : clone.pages.entrySet()) {
                SPage clonePage = entry.getValue();
                clonePage.next = clone.getPage(clonePage.next);
                clonePage.prev = clone.getPage(clonePage.prev);
                if (clonePage.children != null) {
                    for (int i = 0; i < clonePage.children.size(); i++) {
                        clonePage.children.set(i, clone.getPage(clonePage.children.get(i)));
                    }
                }
            }
            clone.header = Arrays.copyOf(header, header.length);
            for (int i = 0; i < header.length; i++) {
                clone.header[i] = clone.pages.get(header[i].getId());
            }
            return clone;
        } catch (CloneNotSupportedException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30039, e);
        } finally {
            updateLock.unlock();
        }
    }

    private SPage getPage(SPage page) {
        if (page == null) {
            return page;
        }
        return pages.get(page.getId());
    }

    public void writeValuesTo(ObjectOutputStream oos) throws TeiidComponentException, IOException {
        SPage page = header[0];
        oos.writeLong(this.rowCount.get());
        while (true) {
            List<List<?>> batch = page.getValues();
            BatchSerializer.writeBatch(oos, leafManager.getTypes(), batch);
            if (page.next == null) {
                break;
            }
            page = page.next;
        }
    }

    public void setBatchInsert(boolean batchInsert) throws TeiidComponentException {
        if (this.batchInsert == batchInsert) {
            return;
        }
        this.batchInsert = batchInsert;
        if (batchInsert || incompleteInsert == null) {
            return;
        }
        SPage toFlush = incompleteInsert;
        incompleteInsert = null;
        if (toFlush.managedBatch != null || toFlush.values == null) {
            return;
        }
        toFlush.setValues(toFlush.getValues());
    }

    public void readValuesFrom(ObjectInputStream ois) throws IOException, ClassNotFoundException, TeiidComponentException {
        long size = ois.readLong();
        int sizeHint = this.getExpectedHeight(size);
        batchInsert = true;
        while (this.getRowCount() < size) {
            List<List<Object>> batch = BatchSerializer.readBatch(ois, leafManager.getTypes());
            for (List list : batch) {
                this.insert(list, InsertMode.ORDERED, sizeHint);
            }
        }
        batchInsert = false;
    }

    protected SPage findChildTail(SPage page) {
        if (page == null) {
            page = header[header.length - 1];
            while (page.next != null) {
                page = page.next;
            }
            return page;
        }
        if (page.children != null) {
            page = page.children.get(page.children.size() - 1);
            while (page.next != null) {
                page = page.next;
            }
        }
        return page;
    }

    /**
     * Determine a new random level using an XOR rng.
     *
     * This uses the simplest of the generators described in George
     * Marsaglia's "Xorshift RNGs" paper.  This is not a high-quality
     * generator but is acceptable here.
     *
     * See also the JSR-166 working group ConcurrentSkipListMap implementation.
     *
     * @return
     */
    private int randomLevel() {
        int x = randomSeed;
        x ^= x << 13;
        x ^= x >>> 17;
        randomSeed = x ^= x << 5;
        int level = 0;
        while ((x & mask) == mask) {
            ++level;
            x >>>= shift;
        }
        return level;
    }

    /**
     * Search each level to find the pointer to the next level
     * @param n
     * @param places
     * @return
     * @throws TeiidComponentException
     */
    List find(List n, List<SearchResult> places) throws TeiidComponentException {
        SPage x = null;
        for (int i = header.length - 1; i >= 0; i--) {
            if (x == null) {
                x = header[i];
            }
            SearchResult s = SPage.search(x, n, places);
            if (places != null) {
                places.add(s);
            }
            if ((s.index == -1 && s.page == header[i]) || s.values.isEmpty()) {
                x = null;
                continue; //start at the beginning of the next level
            }
            x = s.page;
            int index = s.index;
            boolean matched = true;
            if (index < 0) {
                matched = false;
                index = Math.max(0, -index - 2);
            }
            if (i == 0) {
                if (!matched) {
                    return null;
                }
                return s.values.get(index);
            }
            x = x.children.get(index);
        }
        return null;
    }

    public List find(List n) throws TeiidComponentException {
        return find(n, new LinkedList<SPage.SearchResult>());
    }

    public List insert(List tuple, InsertMode mode, int sizeHint) throws TeiidComponentException {
        if (tuple.size() != this.leafManager.getTypes().length) {
            throw new AssertionError("Invalid tuple."); //$NON-NLS-1$
        }
        LinkedList<SearchResult> places = new LinkedList<SearchResult>();
        List match = null;
        if (this.lobManager != null) {
            this.lobManager.updateReferences(tuple, ReferenceMode.CREATE);
        }
        if (mode == InsertMode.ORDERED) {
            SPage last = null;
            while (last == null || last.children != null) {
                last = findChildTail(last);
                //TODO: do this lazily
                List<List<?>> batch = last.getValues();
                places.add(new SearchResult(-batch.size() -1, last, batch));
            }
        } else {
            match = find(tuple, places);
            if (match != null) {
                if (mode != InsertMode.UPDATE) {
                    return match;
                }
                SearchResult last = places.getLast();
                SPage page = last.page;
                last.values.set(last.index, tuple);
                page.setValues(last.values);
                if (this.lobManager != null) {
                    this.lobManager.updateReferences(tuple, ReferenceMode.REMOVE);
                }
                return match;
            }
        }
        List key = extractKey(tuple);
        int level = 0;
        if (mode != InsertMode.ORDERED) {
            if (sizeHint > -1) {
                level = Math.min(sizeHint, randomLevel());
            } else {
                level = randomLevel();
            }
        } else if (!places.isEmpty() && places.getLast().values.size() == getPageSize(true)) {
            long row = rowCount.get();
            while (row != 0 && row%getPageSize(true) == 0) {
                row = (row - getPageSize(true) + 1)/getPageSize(true);
                level++;
            }
        }
        assert header.length == places.size();
        if (level >= header.length) {
            header = Arrays.copyOf(header, level + 1);
        }
        rowCount.addAndGet(1);
        SPage page = null;
        for (int i = 0; i <= level; i++) {
            if (places.isEmpty()) {
                SPage newHead = new SPage(this, false);
                List<List<?>> batch = newHead.getValues();
                batch.add(key);
                newHead.setValues(batch);
                newHead.children.add(page);
                header[i] = newHead;
                page = newHead;
            } else {
                SearchResult result = places.removeLast();
                Object value = (i == 0 ? tuple : page);
                page = insert(key, result, places.peekLast(), value, mode == InsertMode.ORDERED);
            }
        }
        return null;
    }

    public int getExpectedHeight(long sizeHint) {
        if (sizeHint == 0) {
            return 0;
        }
        int logSize = 1;
        while (sizeHint > this.getPageSize(logSize==0)) {
            logSize++;
            sizeHint/=this.getPageSize(logSize==0);
        }
        return logSize;
    }

    List extractKey(List tuple) {
        if (tuple.size() > keyLength) {
            return new ArrayList(tuple.subList(0, keyLength));
        }
        return tuple;
    }

    SPage insert(List k, SearchResult result, SearchResult parent, Object value, boolean ordered) throws TeiidComponentException {
        SPage page = result.page;
        int index = -result.index - 1;
        boolean leaf = !(value instanceof SPage);
        if (result.values.size() == getPageSize(leaf)) {
            SPage nextPage = new SPage(this, leaf);
            List<List<?>> nextValues = nextPage.getValues();
            nextPage.next = page.next;
            nextPage.prev = page;
            if (nextPage.next != null) {
                nextPage.next.prev = nextPage;
            }
            page.next = nextPage;
            boolean inNext = false;
            if (!ordered) {
                //split the values
                nextValues.addAll(result.values.subList(getPageSize(leaf)/2, getPageSize(leaf)));
                result.values.subList(getPageSize(leaf)/2, getPageSize(leaf)).clear();
                if (!leaf) {
                    nextPage.children.addAll(page.children.subList(getPageSize(leaf)/2, getPageSize(false)));
                    page.children.subList(getPageSize(false)/2, getPageSize(false)).clear();
                }
                if (index <= getPageSize(leaf)/2) {
                    setValue(index, k, value, result.values, page);
                } else {
                    inNext = true;
                    setValue(index - getPageSize(leaf)/2, k, value, nextValues, nextPage);
                }
                page.setValues(result.values);
                if (parent != null) {
                    List min = nextPage.getValues().get(0);
                    SPage.correctParents(parent.page, min, page, nextPage);
                }
            } else {
                inNext = true;
                setValue(0, k, value, nextValues, nextPage);
            }
            nextPage.setValues(nextValues);
            if (inNext) {
                page = nextPage;
            }
        } else {
            setValue(index, k, value, result.values, page);
            page.setValues(result.values);
        }
        return page;
    }

    static void setValue(int index, List key, Object value, List<List<?>> values, SPage page) {
        if (value instanceof SPage) {
            values.add(index, key);
            page.children.add(index, (SPage) value);
        } else {
            values.add(index, (List)value);
        }
    }

    public List remove(List key) throws TeiidComponentException {
        LinkedList<SearchResult> places = new LinkedList<SearchResult>();
        List tuple = find(key, places);
        if (tuple == null) {
            return null;
        }
        rowCount.addAndGet(-1);
        for (int i = 0; i < header.length; i++) {
            SearchResult searchResult = places.removeLast();
            if (searchResult.index < 0) {
                continue;
            }
            searchResult.values.remove(searchResult.index);
            boolean leaf = true;
            if (searchResult.page.children != null) {
                leaf = false;
                searchResult.page.children.remove(searchResult.index);
            }
            int size = searchResult.values.size();
            if (size == 0) {
                if (header[i] != searchResult.page) {
                    searchResult.page.remove(false);
                    if (searchResult.page.next != null) {
                        searchResult.page.next.prev = searchResult.page.prev;
                    }
                    searchResult.page.prev.next = searchResult.page.next;
                    searchResult.page.next = header[i];
                    searchResult.page.prev = null;
                    continue;
                }
                header[i].remove(false);
                if (header[i].next != null) {
                    header[i] = header[i].next;
                    header[i].prev = null;
                } else {
                    if (i != 0) {
                        header = Arrays.copyOf(header, i);
                        break;
                    }
                    header[0] = new SPage(this, true);
                }
                continue;
            } else if (size < getPageSize(leaf)/2) {
                //check for merge
                if (searchResult.page.next != null) {
                    List<List<?>> nextValues = searchResult.page.next.getValues();
                    if (nextValues.size() < getPageSize(leaf)/4) {
                        SPage.merge(places, nextValues, searchResult.page, searchResult.values);
                        continue;
                    }
                }
                if (searchResult.page.prev != null) {
                    List<List<?>> prevValues = searchResult.page.prev.getValues();
                    if (prevValues.size() < getPageSize(leaf)/4) {
                        SPage.merge(places, searchResult.values, searchResult.page.prev, prevValues);
                        continue;
                    }
                }
            }
            searchResult.page.setValues(searchResult.values);
        }
        if (lobManager != null) {
            lobManager.updateReferences(tuple, ReferenceMode.REMOVE);
        }
        return tuple;
    }

    public void remove() {
        truncate(true);
        this.keyManager.remove();
        this.leafManager.remove();
        if (this.lobManager != null) {
            this.lobManager.remove();
        }
    }

    public long getRowCount() {
        return this.rowCount.get();
    }

    public long truncate(boolean force) {
        long oldSize = rowCount.getAndSet(0);
        for (int i = 0; i < header.length; i++) {
            SPage page = header[i];
            while (page != null) {
                page.remove(force);
                page = page.next;
            }
        }
        header = new SPage[] {new SPage(this, true)};
        return oldSize;
    }

    public int getHeight() {
        return header.length;
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        for (int i = header.length -1; i >= 0; i--) {
            SPage page = header[i];
            result.append("Level ").append(i).append(" "); //$NON-NLS-1$ //$NON-NLS-2$
            while (page != null) {
                result.append(page);
                result.append(", "); //$NON-NLS-1$
                page = page.next;
            }
            result.append("\n"); //$NON-NLS-1$
        }
        return result.toString();
    }

    public int getKeyLength() {
        return keyLength;
    }

    public void setPreferMemory(boolean preferMemory) {
        this.leafManager.setPrefersMemory(preferMemory);
    }

    public boolean isPreferMemory() {
        return this.leafManager.prefersMemory();
    }

    public ListNestedSortComparator getComparator() {
        return comparator;
    }

    /**
     * Quickly check if the index can be compacted
     */
    public void compact() {
        while (true) {
            if (this.header.length == 1) {
                return;
            }
            SPage child = this.header[header.length - 2];
            if (child.next != null) {
                //TODO: condense the page pointers
                return;
            }
            //remove unneeded index level
            this.header = Arrays.copyOf(this.header, header.length - 1);
        }
    }

    public void removeRowIdFromKey() {
        this.keyLength--;
        int[] sortParameters = this.comparator.getSortParameters();
        sortParameters = Arrays.copyOf(sortParameters, sortParameters.length - 1);
        this.comparator.setSortParameters(sortParameters);
    }

    public void clearClonedFlags() {
        for (SPage page : pages.values()) {
            if (page.trackingObject != null) {
                Long val = page.managedBatch;
                if (val != null) {
                    SPage.REFERENCES.remove(val);
                }
                page.trackingObject = null;
                //we don't really care about using synchronization or a volatile here
                //since the worst case is that we'll just use gc cleanup
            }
        }
    }

    public int getPageSize(boolean leaf) {
        if (leaf) {
            return leafSize;
        }
        return pageSize;
    }

    BatchManager getBatchManager(boolean leaf) {
        if (leaf) {
            return leafManager;
        }
        return keyManager;
    }

    public TupleSource getTupleSource(final boolean destructive) {
        return new TupleSource() {
            SPage current = header[0];
            List<List<?>> values;
            int index = 0;

            @Override
            public List<?> nextTuple() throws TeiidComponentException,
                    TeiidProcessingException {
                if (current == null) {
                    return null;
                }
                if (values == null) {
                    values = current.getValues();
                }
                if (index >= values.size()) {
                    if (destructive) {
                        current.remove(true);
                    }
                    values = null;
                    current = current.next;
                    if (current == null) {
                        return null;
                    }
                    values = current.getValues();
                    index = 0;
                }
                return values.get(index++);
            }

            @Override
            public void closeSource() {

            }
        };
    }

    public void setMinStorageSize(int minStorageSize) {
        this.minStorageSize = minStorageSize;
    }

    public void setSaveTemporaryLobs(boolean b) {
        if (this.lobManager != null) {
            this.lobManager.setSaveTemporary(b);
        }
    }

}