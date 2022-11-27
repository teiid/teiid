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

package org.teiid.query.processor;

import java.util.ArrayList;
import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;


public class FakeTupleSource implements TupleSource {

    static int maxOpen;
    static int open;

    static void resetStats() {
        maxOpen = 0;
        open = 0;
    }

    public static class FakeComponentException extends TeiidComponentException {

    }

    private List elements;
    private List[] tuples;
    private int index = 0;
    private List expectedSymbols;
    private int[] columnMap;

    //used to test blocked exception. If true,
    //the first time nextTuple is called, it will throws BlockedExceptiom
    private boolean blockOnce;

    public FakeTupleSource(List elements, List[] tuples) {
        this.elements = elements;
        this.tuples = tuples;
    }

    public FakeTupleSource(List elements, List[] tuples, List expectedSymbols, int[] columnMap) {
        this.elements = elements;
        this.tuples = tuples;
        this.expectedSymbols = expectedSymbols;
        this.columnMap = columnMap;
        open++;
        maxOpen = Math.max(open, maxOpen);
    }

    public List getSchema() {
        List theElements = null;
        if(expectedSymbols != null) {
            theElements = expectedSymbols;
        } else {
            theElements = elements;
        }

        return theElements;
    }

    public void openSource() {
        index = 0;
    }

    public List nextTuple()
        throws TeiidComponentException {

        if(this.blockOnce){
            this.blockOnce = false;
            throw BlockedException.INSTANCE;
        }

        if(index < tuples.length) {
            // Get full data tuple, with elements
            List tuple = tuples[index++];

            if(expectedSymbols == null) {
                return tuple;
            }
            // Build mapped data tuple, with expectedSymbols
            List mappedTuple = new ArrayList(expectedSymbols.size());
            for(int i=0; i<columnMap.length; i++) {
                int colIndex = columnMap[i];
                if(colIndex >= 0) {
                    mappedTuple.add( tuple.get(colIndex) );
                } else {
                    mappedTuple.add( null );
                }
            }
            return mappedTuple;
        }
        return null;
    }

    public void closeSource() {
        open--;
    }

    public void setBlockOnce(){
        this.blockOnce = true;
    }

}
