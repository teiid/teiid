package com.metamatrix.connector.salesforce.execution;

import java.util.Calendar;

public class UpdatedResult {

	private Calendar latestDateCovered;
	private String[] IDs;
	
	public Calendar getLatestDateCovered() {
		return latestDateCovered;
	}

	public void setLatestDateCovered(Calendar latestDateCovered) {
		this.latestDateCovered = latestDateCovered;
	}

	public String[] getIDs() {
		return IDs;
	}

	public void setIDs(String[] ids) {
		this.IDs = ids;
	}
}
