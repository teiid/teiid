package org.teiid.translator.object;

import java.util.List;

import org.teiid.language.Select;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.SelectProjections;

/**
 * Each SearchStrategy implementation is based on the data source language specifics for searching its cache.
 * 
 * @author vhalbert
 *
 */
public interface SearchStrategy {
	
	public List<Object> performSearch(Select command, SelectProjections projections,
			ObjectExecutionFactory factory, Object connection) throws TranslatorException;

}
