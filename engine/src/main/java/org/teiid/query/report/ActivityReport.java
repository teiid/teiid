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
