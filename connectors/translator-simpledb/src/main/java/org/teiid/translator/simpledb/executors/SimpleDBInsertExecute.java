package org.teiid.translator.simpledb.executors;

import java.util.Map;

import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.visitors.SimpleDBInsertVisitor;

public class SimpleDBInsertExecute implements UpdateExecution{

	private Command command;
	private SimpleDBConnection connection;
	private int updatedCount=0;
	
	public SimpleDBInsertExecute(Command command, SimpleDBConnection connection) {
		this.command = command;
		this.connection = connection;
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public void cancel() throws TranslatorException {
		
	}

	@Override
	public void execute() throws TranslatorException {
		Insert insert = (Insert) command;
		Map<String, String> columnsMap = SimpleDBInsertVisitor.getColumnsValuesMap(insert);
		updatedCount = connection.getAPIClass().performInsert(SimpleDBInsertVisitor.getDomainName(insert), columnsMap.get("itemName()"), columnsMap);
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return new int[] { updatedCount };
	}

}
