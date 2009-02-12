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

package com.metamatrix.query.report;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * A report holds the output of some activity.  The report collects information during the activity, such
 * as failures or successes.
 */
public class ActivityReport implements Serializable {

	/**
	 * Type of report
	 */
	private String reportType;

	/**
	 * Holder for report items.  Holds collection of {@link ReportItem}s.
	 */
	private Collection items = new ArrayList();

	/**
	 * Holder for report item types.  Holds collection of {@link java.lang.String}s.
	 */
	private Collection types = new ArrayList();

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
	public void addItem(ReportItem item) {
		if(item == null) {
	    	throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.REPORT_0001));
	    }

	    this.items.add(item);
	    this.types.add(item.getType());
	}

    /**
     * Add a new collection of items to the report.
     * @param items Items being added
     */
    public void addItems(Collection items) {
        if(items == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.REPORT_0001));
        }

        Iterator iter = items.iterator();
        while(iter.hasNext()) {
            addItem((ReportItem)iter.next());
        }
    }

	public boolean hasItems() {
	    return (this.items.size() > 0);
	}

	public Collection getItems() {
		return items;
	}

	public Collection getItemsByType(String type) {
	    Collection typedItems = new ArrayList();

		Iterator iter = this.items.iterator();
		while(iter.hasNext()) {
			ReportItem item = (ReportItem) iter.next();
			if(item.getType().equals(type)) {
			    typedItems.add(item);
			}
		}

		return typedItems;
	}

	public Collection getItemTypes() {
	    return types;
	}

    public String toString() {
       StringBuffer str = new StringBuffer();
       str.append(getReportType());
       str.append("\n"); //$NON-NLS-1$

       Iterator typeIter = getItemTypes().iterator();
       while (typeIter.hasNext()) {
        	String type = (String) typeIter.next();
        	str.append(type);
        	str.append(" items:\n"); //$NON-NLS-1$

        	Collection typeItems = getItemsByType(type);
			Iterator itemIter = typeItems.iterator();
			while(itemIter.hasNext()) {
				ReportItem item = (ReportItem) itemIter.next();
				str.append("\t"); //$NON-NLS-1$
				str.append(item.toString());
				str.append("\n"); //$NON-NLS-1$
			}
        }

       return str.toString();
    }
}
