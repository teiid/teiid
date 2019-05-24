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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.common.buffer.SPage.SearchResult;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;

/**
 * Implements intelligent browsing over a {@link STree}
 *
 * TODO: this is not as efficient as it should be over partial matches
 */
public class TupleBrowser implements TupleSource {

    private final STree tree;

    private TupleSource valueSet;

    private SPage page;
    private int index;

    private SPage bound;
    private int boundIndex = -1;

    private List<List<?>> values;
    private boolean updated;
    private boolean direction;

    private boolean inPartial;

    private List<Object> cachedBound;

    private ArrayList<SearchResult> places = new ArrayList<SearchResult>();

    private boolean readOnly = true;

    private boolean atBound;

    /**
     * Construct a value based browser.  The {@link TupleSource} should already be in the
     * proper direction.
     * @param sTree
     * @param valueSet
     * @param direction
     */
    public TupleBrowser(STree sTree, TupleSource valueSet, boolean direction) {
        this(sTree, valueSet, direction, true);
    }

    /**
     * Construct a value based browser.  The {@link TupleSource} should already be in the
     * proper direction.
     * @param sTree
     * @param valueSet
     * @param direction
     * @param readOnly
     */
    public TupleBrowser(STree sTree, TupleSource valueSet, boolean direction, boolean readOnly) {
        this.tree = sTree;
        this.direction = direction;
        this.valueSet = valueSet;
        this.readOnly = readOnly;
    }

    /**
     * Construct a range based browser
     * @param sTree
     * @param lowerBound
     * @param upperBound
     * @param direction
     * @throws TeiidComponentException
     */
    public TupleBrowser(STree sTree, List<Object> lowerBound, List<Object> upperBound, boolean direction, boolean readOnly) throws TeiidComponentException {
        this.tree = sTree;
        this.direction = direction;
        this.readOnly = readOnly;
        init(lowerBound, upperBound, false);
    }

    private void init(List<Object> lowerBound,
            List<?> upperBound, boolean isPartialKey)
            throws TeiidComponentException {
        atBound = false;
        if (lowerBound != null) {
            lowerBound.addAll(Collections.nCopies(tree.getKeyLength() - lowerBound.size(), null));
            setPage(lowerBound);
        } else {
            page = tree.header[0];
        }

        boolean valid = true;

        if (upperBound != null) {
            if (!isPartialKey && lowerBound != null && this.tree.comparator.compare(upperBound, lowerBound) < 0) {
                valid = false;
            }
            this.tree.find(upperBound, getPlaces());
            SearchResult upper = places.get(places.size() - 1);
            bound = upper.page;
            boundIndex = upper.index;
            if (boundIndex < 0) {
                //we are guaranteed by find to not get back the -1 index, unless
                //there are no tuples, in which case a bound of -1 is fine
                boundIndex = Math.min(upper.values.size(), -boundIndex -1) - 1;
            }
            if (!direction) {
                setValues(upper.values);
            }
            if (lowerBound != null && page == bound) {
                valid = index<=boundIndex;
            }
        } else {
            while (bound == null || bound.children != null) {
                bound = tree.findChildTail(bound);
            }
            if (!direction) {
                if (page != bound || values == null) {
                    setValues(bound.getValues());
                }
                boundIndex = values.size() - 1;
            }
        }

        if (!direction) {
            SPage swap = page;
            page = bound;
            bound = swap;
            int upperIndex = boundIndex;
            boundIndex = index;
            index = upperIndex;
        }

        if (!valid) {
            page = null;
        }
    }

    private boolean setPage(List<?> lowerBound) throws TeiidComponentException {
        this.tree.find(lowerBound, getPlaces());

        SearchResult sr = places.get(places.size() - 1);
        page = sr.page;
        index = sr.index;
        boolean result = true;
        if (index < 0) {
            result = false;
            index = -index - 1;
        }
        setValues(sr.values);
        return result;
    }

    private ArrayList<SearchResult> getPlaces() {
        places.clear();
        return places;
    }

    @Override
    public List<?> nextTuple() throws TeiidComponentException,
            TeiidProcessingException {
        for (;;) {
            //first check for value iteration
            if (!inPartial && valueSet != null) {
                List<?> newValue = valueSet.nextTuple();
                if (newValue == null) {
                    resetState();
                    return null;
                }
                if (newValue.size() < tree.getKeyLength()) {
                    if (cachedBound == null) {
                        cachedBound = new ArrayList<Object>(tree.getKeyLength());
                    }
                    cachedBound.clear();
                    cachedBound.addAll(newValue);
                    init(cachedBound, newValue, true);
                    inPartial = true;
                    continue;
                }
                if (values != null) {
                    int possibleIndex = Collections.binarySearch(values, newValue, tree.comparator);
                    if (possibleIndex >= 0) {
                        //value exists in the current page
                        index = possibleIndex;
                        return values.get(possibleIndex);
                    }
                    //check for end/terminal conditions
                    if (direction && possibleIndex == -values.size() -1) {
                        if (page.next == null) {
                            resetState();
                            return null;
                        }
                    } else if (!direction && possibleIndex == -1) {
                        if (page.prev == null) {
                            resetState();
                            return null;
                        }
                    } else {
                        //the value simply doesn't exist
                        continue;
                    }
                }
                resetState();
                if (!setPage(newValue)) {
                    continue;
                }
                return values.get(index);
            }
            if (page == null) {
                if (inPartial) {
                    inPartial = false;
                    continue;
                }
                return null;
            }
            if (values == null) {
                setValues(page.getValues());
                if (direction) {
                    index = 0;
                } else {
                    index = values.size() - 1;
                }
            }
            if (index >= 0 && index < values.size() && !atBound) {
                List<?> result = values.get(index);
                if (page == bound && index == boundIndex) {
                    atBound = true;
                }
                index+=getOffset();
                return result;
            }
            resetState();
            if (direction) {
                page = page.next;
            } else {
                page = page.prev;
            }
            if (atBound) {
                page = null;
            }
        }
    }

    public void reset(TupleSource ts) throws TeiidComponentException {
        this.valueSet = ts;
        resetState();
    }

    private void resetState() throws TeiidComponentException {
        if (updated) {
            page.setValues(values);
        }
        updated = false;
        setValues(null);
    }

    private int getOffset() {
        if (!inPartial && valueSet != null) {
            return 0;
        }
        return direction?1:-1;
    }

    /**
     * Perform an in-place update of the tuple just returned by the next method
     * WARNING - this must not change the key value
     * @param tuple
     * @throws TeiidComponentException
     */
    public void update(List<?> tuple) throws TeiidComponentException {
        if (readOnly) {
            throw new AssertionError("read only"); //$NON-NLS-1$
        }
        values.set(index - getOffset(), tuple);
        updated = true;
    }

    /**
     * Notify the browser that the last value was deleted.
     * @throws TeiidComponentException
     */
    public void removed() throws TeiidComponentException {
        if (readOnly) {
            throw new AssertionError("read only"); //$NON-NLS-1$
        }
        index-=getOffset();
        List<?> lowerBound = values.remove(index);
        //check if the page has been removed
        if (page != null && page.managedBatch == null && page.values == null) {
            setPageAfterRemove(lowerBound);
        }
        if (index == values.size() && page != null) {
            boolean search = false;
            if (direction) {
                search = page.next == null;
            } else {
                search = page.prev == null;
            }
            if (search) {
                setPageAfterRemove(lowerBound);
            }
        }
    }

    private void setPageAfterRemove(List<?> lowerBound)
            throws TeiidComponentException, AssertionError {
        //navigate to the current position
        if (!setPage(lowerBound)) {
            throw new AssertionError("Expected lowerBound to still exist"); //$NON-NLS-1$
        }
        //remove the tuple from the iteration set, it will be removed from the tree by
        //an external call
        values.remove(index);
    }

    @Override
    public void closeSource() {

    }

    private void setValues(List<List<?>> values) {
        if (updated) {
            throw new AssertionError("should have committed updates"); //$NON-NLS-1$
        }
        if (readOnly || values == null || values instanceof LightWeightCopyOnWriteList) {
            this.values = values;
        } else {
            //use a lightweight copy if we're possibly mutative
            //as we'll modify the values in line
            //-for updates the browser will save back to the tree
            //-for deletes we let the normal tree logic handle the actual delete
            //so that the tree can rebalance
            this.values = new LightWeightCopyOnWriteList<List<?>>(values);
        }
    }

    /**
     * For testing
     * @return
     */
    Integer getValueCount() {
        if (values == null) {
            return null;
        }
        return values.size();
    }

}