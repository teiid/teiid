package org.teiid.resource.adapter.salesforce.execution;

import java.util.Calendar;
import java.util.List;

public class UpdatedResult {

	private Calendar latestDateCovered;
	private List<String> IDs;
	
	public Calendar getLatestDateCovered() {
		return latestDateCovered;
	}

	public void setLatestDateCovered(Calendar latestDateCovered) {
		this.latestDateCovered = latestDateCovered;
	}

	public List<String> getIDs() {
		return IDs;
	}

	public void setIDs(List<String> list) {
		this.IDs = list;
	}
}
