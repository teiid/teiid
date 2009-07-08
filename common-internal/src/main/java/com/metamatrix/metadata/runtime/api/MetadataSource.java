package com.metamatrix.metadata.runtime.api;

import java.io.File;
import java.util.Set;

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
	
}
