package org.teiid.translator.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;

/**
 * This iterator limits the number Keys that is needed for result
 */
public class LimitProjectionIterator extends Filter {

	private ArrayList<ColumnSet> filterColumns =  new ArrayList<ColumnSet>();
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);

		int columnCount = Integer.parseInt(options.get(EvaluatorIterator.COLUMNS_COUNT));
		
		for (int i = 0; i < columnCount; i++) {
			String cf = options.get(EvaluatorIterator.createColumnName(EvaluatorIterator.CF, i));
			String cq = options.get(EvaluatorIterator.createColumnName(EvaluatorIterator.CQ, i));

			if (cf != null && cq != null) {
				this.filterColumns.add(new ColumnSet(Arrays.asList(cf + ":" + cq))); //$NON-NLS-1$
			} 
			else {
				if (cf == null) {
					cf = AccumuloMetadataProcessor.ROWID;
				}
				this.filterColumns.add(new ColumnSet(Arrays.asList(cf)));
			}					
		}		
	}
	
	@Override
	public boolean accept(Key k, Value v) {
		for (ColumnSet column:this.filterColumns) {
			if (column.contains(k)) {
				return true;
			}
		}
		return false;
	}
}
