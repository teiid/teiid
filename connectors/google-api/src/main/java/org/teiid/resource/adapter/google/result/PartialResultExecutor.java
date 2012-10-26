package org.teiid.resource.adapter.google.result;

import java.util.List;

import org.teiid.resource.adapter.google.common.SheetRow;


/**
 * Executable query that will retrieve just specified portion of results (rows). 
 * 
 * For example to get rows starting at row 10 and retrieves 5 rows (included) use this interface:
 *   
 *   partialResultExecutor.getResultBatch(10,5) 
 * 
 * @author fnguyen
 */
public interface PartialResultExecutor {
	
	/**
	 *  Returns part of the result.
	 *  
	 * @return null or empty list if no more results are in the batch. Maximum amount of sheet rows in the result
	 * is amount
	 */
	List<SheetRow> getResultsBatch(int startIndex, int amount);
}
