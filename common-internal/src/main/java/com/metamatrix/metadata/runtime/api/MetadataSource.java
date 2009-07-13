package com.metamatrix.metadata.runtime.api;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import com.metamatrix.common.vdb.api.ModelInfo;

public interface MetadataSource {
	
	String getName();
	
	/**
	 * Return all files known by this metadata source
	 * @return
	 */
	Set<String> getEntries();
	
	/**
	 * Returns the file for the given path, which must exist in the entry set
	 * @param path
	 * @return
	 */
	File getFile(String path);
	
	/**
	 * Get the list of model names that will provide metadata
	 * @return
	 */
	Set<String> getConnectorMetadataModelNames();
	
	/**
	 * Whether to cache connector metadata
	 * @return
	 */
	boolean cacheConnectorMetadata();
	
	/**
	 * Save the stream to given path.
	 * @param path
	 */
	void saveFile(InputStream stream, String path) throws IOException;
	
	/**
	 * Get the model with the given name.
	 * @param name
	 * @return
	 */
	ModelInfo getModelInfo(String name);
	
}
