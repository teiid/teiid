package org.teiid.translator.google;

import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.common.UpdateResult;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.metadata.Worksheet;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public abstract class AbstractSpreadsheetExecution implements UpdateExecution {
	protected GoogleSpreadsheetConnection connection;
	protected RuntimeMetadata metadata;
	protected ExecutionContext context;
	protected Command command;
	protected UpdateResult result;

	public AbstractSpreadsheetExecution(Command command, GoogleSpreadsheetConnection connection, ExecutionContext context,RuntimeMetadata metadata) {
		super();
		this.connection = connection;
		this.metadata = metadata;
		this.context = context;
		this.command = command;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void cancel() throws TranslatorException {
		// TODO Auto-generated method stub
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		if(result.checkResult()==false){
			context.addWarning(new SpreadsheetOperationException("Not all rows has been properly updated."+" Expected: "+result.getExpectedNumberOfRows()+"Actual: "+result.getActualNumberOfRows()));
		}		
		return new int[]{result.getActualNumberOfRows()};
	}
    
	 void checkHeaders(String worksheetTitle) throws TranslatorException{
		SpreadsheetInfo info=connection.getSpreadsheetInfo();
		Worksheet worksheet=info.getWorksheetByName(worksheetTitle);
		if(worksheet==null){
			throw new SpreadsheetOperationException("Worksheet "+worksheetTitle+" doesn't exist in the spreadsheet");
		}
		if(!worksheet.isHeaderEnabled()){
			throw new TranslatorException("Spreadsheet's column labels must exists to support UPDATE, INSERT and DELETE statements.");
		}
	}
	
	public GoogleSpreadsheetConnection getConnection(){
		return connection;
	}
}
