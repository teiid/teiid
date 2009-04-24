package com.metamatrix.connector.salesforce.execution;

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
