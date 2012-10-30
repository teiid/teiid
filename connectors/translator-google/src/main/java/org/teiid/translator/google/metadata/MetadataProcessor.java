package org.teiid.translator.google.metadata;


import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Table;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.metadata.Column;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.metadata.Worksheet;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.google.Constants;

public class MetadataProcessor {

	
	MetadataFactory metadataFactory;
	SpreadsheetInfo spreadsheetMetadata;

	public MetadataProcessor(MetadataFactory metadataFactory, SpreadsheetInfo metadata) {
		this.metadataFactory = metadataFactory;
		spreadsheetMetadata = metadata;
	}
/**
 * Creates metadata from all spreadsheets in the user account. Table name consists of Spreadsheet name 
 * and worksheet name. Columns of the table are columns of the worksheet.
 * 
 */
	public void processMetadata() {
		try {
		for (Worksheet worksheet : spreadsheetMetadata.getWorksheets()) {			
				addTable(worksheet);

		}
		addProcedures();
		} catch (Exception ex)	{
			throw new SpreadsheetOperationException("Error processing metadata" ,ex);
		}
	}
	/**
	 * Adds new table to metadata.
	 * 
	 * @param spreadsheet  Name of the spreadsheet
	 * @param worksheet    Name of the worksheet
	 * @throws TranslatorException
	 */
	private void addTable(Worksheet worksheet) throws TranslatorException{
		Table table=null;
		if (worksheet.getColumnCount() == 0){
		return;
		}
		table=metadataFactory.addTable(worksheet.getName());
		addColumnsToTable(table, worksheet);

	}
	/**
	 * Adds column to table
	 * 
	 * @param table      Teiid table
	 * @param worksheet  
	 * @throws TranslatorException
	 */
	private void addColumnsToTable(Table table, Worksheet worksheet) throws TranslatorException{


		for(Column column : worksheet){ 
			switch(column.getDataType()){
			case DATE:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.DATE, table);
				break;
			case DOUBLE:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.DOUBLE, table);
				break;
			case STRING:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.STRING, table);
				break;
			case TIME:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.TIME, table);
				break;
			case CHAR:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.CHAR, table);
				break;
			case BIG_DECIMAL:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.BIG_DECIMAL, table);
				break;
			case BIG_INTEGER:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.BIG_INTEGER, table);
				break;
			case BOOLEAN:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.BOOLEAN, table);
				break;
			case FLOAT:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.FLOAT, table);
				break;
			case INTEGER:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.INTEGER, table);
				break;
			case LONG:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.LONG, table);
				break;				
			case SHORT:
				metadataFactory.addColumn(column.getAlphaName(), DataTypeManager.DefaultDataTypes.SHORT, table);
				break;
			default:
				new TranslatorException("Unsupported column type: "+column.getDataType());
				break;			
			}	
		}    
	}
	/**
	 * Adds procedures to metadataFactory
	 * 
	 * @throws TranslatorException
	 */
	private void addProcedures() throws TranslatorException{
		Procedure p = metadataFactory.addProcedure(Constants.GETASTEXT);
		p.setAnnotation("Returns part of the spreadsheet as text"); //$NON-NLS-1$
		ProcedureParameter param1 = metadataFactory.addProcedureParameter("worksheet", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param1.setAnnotation("Worksheet name"); //$NON-NLS-1$
		ProcedureParameter param2 = metadataFactory.addProcedureParameter("startRow", TypeFacility.RUNTIME_NAMES.INTEGER, Type.In, p); //$NON-NLS-1$
		param2.setAnnotation("Starting row"); //$NON-NLS-1$
		ProcedureParameter param3 = metadataFactory.addProcedureParameter("startCol", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p); //$NON-NLS-1$
		param3.setAnnotation("Starting column"); //$NON-NLS-1$
		ProcedureParameter param4 = metadataFactory.addProcedureParameter("endRow", TypeFacility.RUNTIME_NAMES.INTEGER, Type.In, p); //$NON-NLS-1$
		param4.setAnnotation("Ending row"); //$NON-NLS-1$
		ProcedureParameter param5 = metadataFactory.addProcedureParameter("endCol", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p); //$NON-NLS-1$
		param5.setAnnotation("Ending column"); //$NON-NLS-1$
		metadataFactory.addProcedureResultSetColumn("csv", TypeFacility.RUNTIME_NAMES.CLOB, p); //$NON-NLS-1$
	}
	
}
