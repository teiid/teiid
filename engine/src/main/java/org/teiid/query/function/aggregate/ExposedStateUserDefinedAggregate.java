package org.teiid.query.function.aggregate;

import java.util.List;

import org.teiid.UserDefinedAggregate;

/**
 * Internal extension of the user defined aggregate interface so that we can expose the internal
 * state.  TODO: this should be exposed eventually for implementors
 * @param <T>
 */
public interface ExposedStateUserDefinedAggregate<T> extends UserDefinedAggregate<T> {
	
	List<? extends Class<?>> getStateTypes();
    
    void getState(List<Object> state);
    
    int setState(List<?> state, int index);

}
