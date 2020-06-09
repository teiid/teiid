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

package org.teiid.translator.google.api.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.teiid.translator.google.api.result.PartialResultExecutor;
import org.teiid.translator.google.api.result.RowsResult;
import org.teiid.translator.google.api.result.SheetRow;


/**
 * This unit test verifies batch behavior and iteration behavior of RowsResult.
 *
 * @author fnguyen
 *
 */
@SuppressWarnings("nls")
public class RowsResultTest {

    @Test
    public void simpleIteration(){
        final List<SheetRow> rows = new ArrayList<SheetRow>();
        rows.add(new SheetRow(new String[]{"a","b","c"}));
        rows.add(new SheetRow(new String[]{"a2","b2","c2"}));
        rows.add(new SheetRow(new String[]{"a3","b3","c3"}));

        RowsResult result = new RowsResult(new PartialResultExecutor() {
            boolean called = false;

            @Override
            public List<SheetRow> getResultsBatch(int startIndex, int endIndex) {
                if (called)
                    Assert.fail("getResultsBatch should've been called only once");
                called = true;
                return rows;
            }
        }, 10);

        int i = 0;
        for (SheetRow row : result){
            Assert.assertEquals(row, rows.get(i++));
        }
    }


    @Test
    public void twoBatchCallsNeeded(){
        final List<SheetRow> firstPart = new ArrayList<SheetRow>();
        final List<SheetRow> secondPart = new ArrayList<SheetRow>();
        final List<SheetRow> all = new ArrayList<SheetRow>();
        firstPart.add(new SheetRow(new String[]{"a","b","c"}));
        firstPart.add(new SheetRow(new String[]{"a2","b2","c2"}));
        firstPart.add(new SheetRow(new String[]{"a3","b3","c3"}));
        secondPart.add(new SheetRow(new String[]{"a4","b4","c4"}));
        secondPart.add(new SheetRow(new String[]{"a5","b5","c5"}));
        all.addAll(firstPart);
        all.addAll(secondPart);

        RowsResult result = new RowsResult(new PartialResultExecutor() {
            int called = 0;

            @Override
            public List<SheetRow> getResultsBatch(int startIndex, int endIndex) {
                if (called++ > 2)
                    Assert.fail("getResultsBatch at most twice");

                if (called == 1){
                    return firstPart;
                } else if (called == 2){
                    return secondPart;
                }
                //Shouldn't reach here.
                return null;
            }
        }, 3);

        int i = 0;
        for (SheetRow row : result){
            Assert.assertEquals(row, all.get(i++));
        }
        Assert.assertEquals("Six rows should be in the result",5, i);
    }


    @Test
    public void noRows(){
        final List<SheetRow> all = new ArrayList<SheetRow>();


        RowsResult result = new RowsResult(new PartialResultExecutor() {
            int called = 0;

            @Override
            public List<SheetRow> getResultsBatch(int startIndex, int endIndex) {
                if (called++ > 1)
                    Assert.fail("getResultsBatch at once");

                return all;
            }
        }, 3);

        int i = 0;
        for (SheetRow row : result){
            i++;
        }
        Assert.assertEquals("No iterations should be made",0, i);
    }


    @Test
    public void sixBatchCalls(){
        final List<SheetRow> all = new ArrayList<SheetRow>();
        all.add(new SheetRow(new String[]{"a","b","c"}));
        all.add(new SheetRow(new String[]{"a2","b2","c2"}));
        all.add(new SheetRow(new String[]{"a3","b3","c3"}));
        all.add(new SheetRow(new String[]{"a4","b4","c4"}));
        all.add(new SheetRow(new String[]{"a5","b5","c5"}));
        all.add(new SheetRow(new String[]{"a6","b6","c6"}));

        RowsResult result = new RowsResult(new PartialResultExecutor() {
            int called = 0;

            @Override
            public List<SheetRow> getResultsBatch(int startIndex, int endIndex) {
                if (called > 6)
                    Assert.fail("getResultsBatch at least 6 times");

                if (called == 6)
                    return new ArrayList<SheetRow>();
                return Collections.singletonList(all.get(called++));
            }
        }, 1);

        int i = 0;
        for (SheetRow row : result){
            Assert.assertEquals(all.get(i++), row);
        }
        Assert.assertEquals("Six iterations should be made",6, i);
    }

    @Test
    public void offsetTest(){
        final List<SheetRow> all = new ArrayList<SheetRow>();
        all.add(new SheetRow(new String[]{"a","b","c"}));
        all.add(new SheetRow(new String[]{"a2","b2","c2"}));
        all.add(new SheetRow(new String[]{"a3","b3","c3"}));
        all.add(new SheetRow(new String[]{"a4","b4","c4"}));
        all.add(new SheetRow(new String[]{"a5","b5","c5"}));
        all.add(new SheetRow(new String[]{"a6","b6","c6"}));

        RowsResult result = new RowsResult(new PartialResultExecutor() {
            int called = 0;

            @Override
            public List<SheetRow> getResultsBatch(int startIndex, int endIndex) {
                if (called++ > 1)
                    Assert.fail("getResultsBatch at most 2 times");

                ArrayList<SheetRow> result = new ArrayList<SheetRow>();
                if (called == 1) {
                    result.add(all.get(3));
                    result.add(all.get(4));
                } else if (called == 2)
                    result.add(all.get(5));

                return result;
            }
        }, 2);

        result.setOffset(3);
        result.setLimit(3);

        int i = 3;
        for (SheetRow row : result){
            Assert.assertEquals(all.get(i++), row);
        }
        Assert.assertEquals("3 iterations should be made",3, i-3);
    }

    @Test
    public void offsetTestOne(){
        final List<SheetRow> all = new ArrayList<SheetRow>();
        all.add(new SheetRow(new String[]{"a","b","c"}));
        all.add(new SheetRow(new String[]{"a2","b2","c2"}));
        all.add(new SheetRow(new String[]{"a3","b3","c3"}));
        all.add(new SheetRow(new String[]{"a4","b4","c4"}));
        all.add(new SheetRow(new String[]{"a5","b5","c5"}));
        all.add(new SheetRow(new String[]{"a6","b6","c6"}));

        RowsResult result = new RowsResult(new PartialResultExecutor() {
            int called = 0;

            @Override
            public List<SheetRow> getResultsBatch(int startIndex, int endIndex) {
                if (called++ > 2)
                    Assert.fail("getResultsBatch at most 3 times");

                ArrayList<SheetRow> result = new ArrayList<SheetRow>();
                if (called == 1) {
                    result.add(all.get(1));
                    result.add(all.get(2));
                } else if (called == 2) {
                    result.add(all.get(3));
                    result.add(all.get(4));
                } else if (called == 3)
                    result.add(all.get(5));
                return result;
            }
        }, 2);

        result.setOffset(1);
        result.setLimit(6);

        int i = 1;
        for (SheetRow row : result){
            Assert.assertEquals(all.get(i++), row);
        }
        Assert.assertEquals("5 iterations should be made",5, i-1);
    }
}

