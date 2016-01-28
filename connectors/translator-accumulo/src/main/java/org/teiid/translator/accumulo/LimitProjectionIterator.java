package org.teiid.translator.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;

/**
 * This iterator limits the number Keys that is needed for result
 */
public class LimitProjectionIterator extends Filter {

	private ArrayList<ColumnSet> filterColumns =  new ArrayList<ColumnSet>();
	private static ColumnSet ROWID = new ColumnSet(Arrays.asList(AccumuloMetadataProcessor.ROWID));
	private boolean onlyRowId = false;
	public static final String COLUMN_NAME = "COLUMN_NAME"; //$NON-NLS-1$
    public static final String COLUMNS_COUNT = "COLUMN_COUNT"; //$NON-NLS-1$
    public static final String CF = "CF"; //$NON-NLS-1$
    public static final String CQ = "CQ"; //$NON-NLS-1$
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);

        int columnCount = Integer.parseInt(options.get(COLUMNS_COUNT));
        for (int i = 0; i < columnCount; i++) {
            String cf = options.get(createColumnName(CF, i));
            String cq = options.get(createColumnName(CQ, i));

        	if (cf != null && cq != null) {
        		this.filterColumns.add(new ColumnSet(Arrays.asList(cf + ":" + cq))); //$NON-NLS-1$
        	} 
        	else {
        		if (cf == null) {
        		    this.filterColumns.add(ROWID);
        		} else {
        		    this.filterColumns.add(new ColumnSet(Arrays.asList(cf)));    
        		}
        	}					
        }
        // When ROWID column is queried alone in SQL stmt, do not filter as scanner will report zero rows
        this.onlyRowId = (this.filterColumns.size() == 1 && this.filterColumns.contains(ROWID));
	}
	
    public static String createColumnName(String prop, int index) {
        return COLUMN_NAME+"."+index+"."+prop;//$NON-NLS-1$ //$NON-NLS-2$
    }
    
	@Override
	public boolean accept(Key k, Value v) {
	    if (this.onlyRowId) {
	        return true;
	    }
		for (ColumnSet column:this.filterColumns) {
			if (column.contains(k)) {
				return true;
			}
		}
		return false;
	}
}
