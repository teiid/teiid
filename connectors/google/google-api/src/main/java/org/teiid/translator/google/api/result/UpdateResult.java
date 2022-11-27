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


/**
 * This class represents number of updated rows
 *
 * @author felias
 *
 */
public class UpdateResult {

    private int expectedNumberOfRows = -1;
    private int actualNumberOfRows = -1;

    /**
     *
     * @param expectedNumberOfRows
     *            number of rows that should have been updated
     * @param actualNumberOfRows
     *            actual number of updated rows
     */
    public UpdateResult(int expectedNumberOfRows, int actualNumberOfRows) {
        this.expectedNumberOfRows = expectedNumberOfRows;
        this.actualNumberOfRows = actualNumberOfRows;
    }

    /*
     * Returns number of rows that should have been updated
     */
    public int getExpectedNumberOfRows() {
        return expectedNumberOfRows;
    }

    /**
     * Returns actual number of updated rows
     *
     */
    public int getActualNumberOfRows() {
        return actualNumberOfRows;
    }

}
