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

package org.teiid.query.report;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.teiid.core.util.Assertion;


/**
 * A report holds the output of some activity.  The report collects information during the activity, such
 * as failures or successes.
 */
public class ActivityReport<R extends ReportItem> implements Serializable {

    /**
     * Type of report
     */
    private String reportType;

    /**
     * Holder for report items.  Holds collection of {@link ReportItem}s.
     */
    private Collection<R> items = new ArrayList<R>();

    /**
     * Construct new report of given type
     * @param reportType Type of report
     */
    public ActivityReport(String reportType) {
        this.reportType = reportType;
    }

    /**
     * Get type of report.
     * @return Type of report
     */
    public String getReportType() {
        return this.reportType;
    }

    /**
     * Add a new item to the report.
     * @param item Item being added
     */
    public void addItem(R item) {
        Assertion.isNotNull(item);
        this.items.add(item);
    }

    /**
     * Add a new collection of items to the report.
     * @param items Items being added
     */
    public void addItems(Collection<R> items) {
        Assertion.isNotNull(items);
        for (R r : items) {
            addItem(r);
        }
    }

    public boolean hasItems() {
        return (this.items.size() > 0);
    }

    public Collection<R> getItems() {
        return items;
    }

    @Override
    public String toString() {
        return reportType + " " + getItems(); //$NON-NLS-1$
    }

}
