package org.teiid.translator.google.execution;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.common.RectangleIdentifier;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.Constants;
import org.teiid.translator.google.SpreadsheetExecutionFactory;

import au.com.bytecode.opencsv.CSVWriter;


public class SpreadsheetProcedureExecution implements ProcedureExecution {

	private String worksheet;
	private RectangleIdentifier dataBlock;
	private GoogleSpreadsheetConnection connection;
	private Call procedure;
	private Iterator<SheetRow> rowIterator;
	private StringWriter sw;
	private CSVWriter writer; 
		
	
	public SpreadsheetProcedureExecution(Call procedure, GoogleSpreadsheetConnection connection) {
		this.connection = connection;
		this.procedure = procedure;

	}
	
	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException{
		

		if (rowIterator.hasNext()) {
		   List<String> list=rowIterator.next().getRow();
		   writer.writeNext(list.toArray(new String[list.size()]));		   		
			LinkedList<String> resultlist=new LinkedList<String>();
			resultlist.add(sw.toString());
			sw.getBuffer().setLength(0);
			return resultlist;		
		}
		try {
			writer.close();
		} catch (IOException e) {
            throw new TranslatorException(e);
		}
		return null;		
	}

	@Override
	public void close() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("closed",Constants.GETASTEXT));
		
	}

	@Override
	public void cancel() throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("cancel", Constants.GETASTEXT));
		
	}

	@Override
	public void execute() throws TranslatorException {
		if(procedure.getProcedureName().equals(Constants.GETASTEXT)){
		worksheet=(String)procedure.getArguments().get(0).getArgumentValue().getValue();
        try{
        checkParameters(procedure.getArguments());
		dataBlock=new RectangleIdentifier( 
				(Integer)procedure.getArguments().get(1).getArgumentValue().getValue(),
				(String)procedure.getArguments().get(2).getArgumentValue().getValue(),
				(Integer)procedure.getArguments().get(3).getArgumentValue().getValue(),
				(String)procedure.getArguments().get(4).getArgumentValue().getValue());		
        }catch(SpreadsheetOperationException e){
        	throw new TranslatorException(e);
        }
		rowIterator=connection.executeProcedureGetAsText(worksheet, dataBlock).iterator();
        initCsv();
		}
	}
	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
	}
	
    private StringBuilder duplicateQuotes(StringBuilder col){
        for(int i=0;i<col.length();i++){
            if(col.charAt(i) == '\"'){
                col.insert(i, '\"');
                i++;
            }
        }
        return col;
    }
    
    private void initCsv(){
    	sw = new StringWriter();
	    writer= new CSVWriter(sw);
    }
    
    private void checkParameters(List<Argument> args) throws TranslatorException{
    	for(Argument arg : args){
    		if(arg.getArgumentValue().getValue()==null){
    			throw new TranslatorException("Procedure parameters mustn't be null.");
    		}
    	}
    }
	private String createCsv(List<String> row) {
		StringBuilder result=new StringBuilder();
		for(String column : row){			
			result.append("\""+duplicateQuotes(new StringBuilder(column))+"\"");
			result.append(',');
		}
		result.setLength(result.length()-1);
		return result.toString();
	}
}
