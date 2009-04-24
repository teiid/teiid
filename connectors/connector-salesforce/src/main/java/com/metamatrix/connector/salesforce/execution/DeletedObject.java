package com.metamatrix.connector.salesforce.execution;

import java.util.Calendar;

public class DeletedObject {

	String ID;
	Calendar deletedDate;
	
	public String getID() {
		return ID;
	}
	public void setID(String id) {
		ID = id;
	}
	public Calendar getDeletedDate() {
		return deletedDate;
	}
	public void setDeletedDate(Calendar deletedDate) {
		this.deletedDate = deletedDate;
	}
}
