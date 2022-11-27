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

package org.teiid.translator.salesforce.execution;

import java.util.Calendar;
import java.util.List;

public class DeletedResult {

    private Calendar latestDateCovered;
    private Calendar earliestDateAvailable;
    private List<DeletedObject> resultRecords;

    public Calendar getLatestDateCovered() {
        return latestDateCovered;
    }

    public void setLatestDateCovered(Calendar latestDateCovered) {
        this.latestDateCovered = latestDateCovered;
    }


    public Calendar getEarliestDateAvailable() {
        return earliestDateAvailable;
    }

    public void setEarliestDateAvailable(Calendar earliestDateAvailable) {
        this.earliestDateAvailable = earliestDateAvailable;
    }

    public void setResultRecords(List<DeletedObject> resultRecords) {
        this.resultRecords = resultRecords;
    }

    public List<DeletedObject> getResultRecords() {
        return resultRecords;
    }
}
