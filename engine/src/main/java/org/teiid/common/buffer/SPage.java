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

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.client.ResizingArrayList;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;

/**
 * A linked list Page entry in the tree
 *
 * State cloning allows a single storage reference to be shared in many trees.
 * A phantom reference is used for proper cleanup once cloned.
 *
 * TODO: a better purging strategy for managedbatchs.
 *
 */
@SuppressWarnings("unchecked")
class SPage implements Cloneable {

    static class SearchResult {
        int index;
        SPage page;
        List<List<?>> values;
        public SearchResult(int index, SPage page, List<List<?>> values) {
            this.index = index;
            this.page = page;
            this.values = values;
        }
    }

    static final Map<Long, PhantomReference<Object>> REFERENCES = new ConcurrentHashMap<Long, PhantomReference<Object>>();
    private static ReferenceQueue<Object> QUEUE = new ReferenceQueue<Object>();
    static class CleanupReference extends PhantomReference<Object> {

        private Long batch;
        private Reference<? extends BatchManager> ref;

        public CleanupReference(Object referent, Long batch, Reference<? extends BatchManager> ref) {
            super(referent, QUEUE);
            this.batch = batch;
            this.ref = ref;
        }

        public void cleanup() {
            try {
                BatchManager batchManager = ref.get();
                if (batchManager != null) {
                    batchManager.remove(batch);
                }
            } finally {
                this.clear();
            }
        }
    }

    private static AtomicLong counter = new AtomicLong();

    STree stree;

    private long id;
    protected SPage next;
    protected SPage prev;
    protected Long managedBatch;
    protected Object trackingObject;
    protected List<List<?>> values;
    protected List<SPage> children;

    SPage(STree stree, boolean leaf) {
        this.stree = stree;
        this.id = counter.getAndIncrement();
        stree.pages.put(this.id, this);
        this.values = new ResizingArrayList<List<?>>();
        if (!leaf) {
            children = new ResizingArrayList<SPage>();
        }
    }

    public SPage clone(STree tree) {
        try {
            if (this.managedBatch != null && trackingObject == null) {
                this.trackingObject = new Object();
                CleanupReference managedBatchReference  = new CleanupReference(trackingObject, managedBatch, stree.getBatchManager(children == null).getBatchManagerReference());
                REFERENCES.put(managedBatch, managedBatchReference);
            }
            SPage clone = (SPage) super.clone();
            clone.stree = tree;
            if (children != null) {
                clone.children = new ResizingArrayList<SPage>(children);
            }
            if (values != null) {
                clone.values = new ResizingArrayList<List<?>>(values);
            }
            return clone;
        } catch (CloneNotSupportedException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30038, e);
        }
    }

    public long getId() {
        return id;
    }

    static SearchResult search(SPage page, List k, List<SearchResult> parent) throws TeiidComponentException {
        List<List<?>> previousValues = null;
        for (;;) {
            List<List<?>> values = page.getValues();
            int index = Collections.binarySearch(values, k, page.stree.comparator);
            int flippedIndex = - index - 1;
            if (previousValues != null) {
                if (flippedIndex == 0) {
                    //systemic weakness of the algorithm
                    return new SearchResult(-previousValues.size() - 1, page.prev, previousValues);
                }
                if (parent != null && index != 0) {
                    page.stree.updateLock.lock();
                    try {
                        index = Collections.binarySearch(values, k, page.stree.comparator);
                        if (index != 0) {
                            //for non-matches move the previous pointer over to this page
                            SPage childPage = page;
                            List oldKey = null;
                            List newKey = page.stree.extractKey(values.get(0));
                            for (ListIterator<SearchResult> desc = parent.listIterator(); desc.hasPrevious();) {
                                SearchResult sr = desc.previous();
                                int parentIndex = Math.max(0, -sr.index - 2);
                                if (oldKey == null) {
                                    oldKey = sr.values.set(parentIndex, newKey);
                                } else if (page.stree.comparator.compare(oldKey, sr.values.get(parentIndex)) == 0 ) {
                                    sr.values.set(parentIndex, newKey);
                                } else {
                                    break;
                                }
                                sr.page.children.set(parentIndex, childPage);
                                sr.page.setValues(sr.values);
                                childPage = sr.page;
                            }
                        }
                    } finally {
                        page.stree.updateLock.unlock();
                    }
                }
            }
            if (flippedIndex != values.size() || page.next == null) {
                return new SearchResult(index, page, values);
            }
            previousValues = values;
            page = page.next;
        }
    }

    protected void setValues(List<List<?>> values) throws TeiidComponentException {
        if (values instanceof LightWeightCopyOnWriteList<?>) {
            values = ((LightWeightCopyOnWriteList<List<?>>)values).getList();
        }
        if (values.size() < stree.minPageSize || stree.getRowCount() < stree.minStorageSize) {
            setDirectValues(values);
            return;
        } else if (stree.batchInsert && children == null && values.size() < stree.leafSize) {
            setDirectValues(values);
            stree.incompleteInsert = this;
            return;
        }
        this.values = null;
        managedBatch = stree.getBatchManager(children == null).createManagedBatch(values, managedBatch, trackingObject == null);
        this.trackingObject = null;
    }

    private void setDirectValues(List<List<?>> values) {
        if (managedBatch != null && trackingObject == null) {
            stree.getBatchManager(children == null).remove(managedBatch);
            managedBatch = null;
            trackingObject = null;
        }
        this.values = values;
    }

    protected void remove(boolean force) {
        if (managedBatch != null) {
            if (force || trackingObject == null) {
                stree.getBatchManager(children == null).remove(managedBatch);
            }
            managedBatch = null;
            trackingObject = null;
        }
        values = null;
        children = null;
    }

    protected List<List<?>> getValues() throws TeiidComponentException {
        if (values != null) {
            return values;
        }
        if (managedBatch == null) {
            //we need this to be a check exception as the batch can be removed
            //by the size check at the buffermanager level
            throw new TeiidComponentException("Batch removed"); //$NON-NLS-1$
        }
        for (int i = 0; i < 10; i++) {
            CleanupReference ref = (CleanupReference)QUEUE.poll();
            if (ref == null) {
                break;
            }
            if (REFERENCES.remove(ref.batch) != null) {
                ref.cleanup();
            }
        }
        List<List<?>> result = stree.getBatchManager(children == null).getBatch(managedBatch, true);
        if (trackingObject != null) {
            return new LightWeightCopyOnWriteList<List<?>>(result);
        }
        return result;
    }

    static void merge(LinkedList<SearchResult> places, List<List<?>> nextValues, SPage current, List<List<?>> currentValues)
    throws TeiidComponentException {
        SearchResult parent = places.peekLast();
        if (parent != null) {
            correctParents(parent.page, nextValues.get(0), current.next, current);
        }
        currentValues.addAll(nextValues);
        if (current.children != null) {
            current.children.addAll(current.next.children);
        }
        current.next.remove(false);
        current.next = current.next.next;
        if (current.next != null) {
            current.next.prev = current;
        }
        current.setValues(currentValues);
    }

    /**
     * Remove the usage of page in favor of nextPage
     * @param parent
     * @param page
     * @param nextPage
     * @throws TeiidComponentException
     */
    static void correctParents(SPage parent, List key, SPage page, SPage nextPage) throws TeiidComponentException {
        SearchResult location = SPage.search(parent, key, null);
        while (location.index == -1 && location.page.prev != null ) {
            parent = location.page.prev;
            location = SPage.search(parent, key, null);
        }
        parent = location.page;
        int index = location.index;
        if (index < 0) {
            index = -index - 1;
        }
        while (parent != null) {
            while (index < parent.children.size()) {
                if (parent.children.get(index) != page) {
                    return;
                }
                parent.children.set(index++, nextPage);
            }
            index = 0;
            parent = parent.next;
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        try {
            List<List<?>> tb = getValues();
            result.append(id);
            if (children == null) {
                if (tb.size() <= 1) {
                    result.append(tb);
                } else {
                    result.append("[").append(tb.get(0)).append(" . ").append(tb.size()). //$NON-NLS-1$ //$NON-NLS-2$
                    append(" . ").append(tb.get(tb.size() - 1)).append("]"); //$NON-NLS-1$ //$NON-NLS-2$
                }
            } else {
                result.append("["); //$NON-NLS-1$
                for (int i = 0; i < children.size(); i++) {
                    result.append(tb.get(i)).append("->").append(children.get(i).getId()); //$NON-NLS-1$
                    if (i < children.size() - 1) {
                        result.append(", "); //$NON-NLS-1$
                    }
                }
                result.append("]");//$NON-NLS-1$
            }
        } catch (Throwable e) {
            result.append("Removed"); //$NON-NLS-1$
        }
        return result.toString();
    }

}
