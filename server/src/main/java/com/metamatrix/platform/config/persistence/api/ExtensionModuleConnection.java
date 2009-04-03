package com.metamatrix.platform.config.persistence.api;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.ExtensionModuleTypes;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.spi.ExtensionModuleTransaction;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class ExtensionModuleConnection implements PersistentConnection {

	private ExtensionModuleTransaction trans;
    private ConfigurationModelContainerAdapter adapter = new ConfigurationModelContainerAdapter();
	
	public ExtensionModuleConnection(ExtensionModuleTransaction trans) {
		this.trans = trans;
	}
	
	@Override
	public void close() {
		trans.close();
	}

	@Override
	public void commit() throws ConfigurationException {
		try {
			trans.commit();
		} catch (ManagedConnectionException e) {
			throw new ConfigurationException(e);
		}
	}

	@Override
	public void delete(ConfigurationID configID, String principal)
			throws ConfigurationException {
		try {
            boolean inUse = trans.isNameInUse(configID.getFullName());

            if (inUse) {
                trans.removeSource(principal, configID.getFullName());
            }
        } catch (Exception e) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0153, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0153, configID));
        }
	}

	@Override
	public boolean isClosed() {
		return trans.isClosed();
	}

	@Override
	public ConfigurationModelContainer read(ConfigurationID configID)
			throws ConfigurationException {
        try {

            byte[] data = trans.getSource(configID.getFullName());

            if (data == null) {
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0154, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0154, configID));
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            InputStream isContent = new BufferedInputStream(bais);

        	ConfigurationModelContainer model = this.adapter.readConfigurationModel(isContent, configID);
            return model;
        } catch (ExtensionModuleNotFoundException notFound) {
            throw new ConfigurationException(notFound, ErrorMessageKeys.CONFIG_0154, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0154, configID));
        } catch (Exception e) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0155, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0155, configID));
        }

	}

	@Override
	public void rollback() throws ConfigurationException {
		try {
			trans.rollback();
		} catch (ManagedConnectionException e) {
			throw new ConfigurationException(e);
		}
	}

	@Override
	public void write(ConfigurationModelContainer model, String principal)
			throws ConfigurationException {
		try {
        	ByteArrayOutputStream out = new ByteArrayOutputStream();
        	BufferedOutputStream bos = new BufferedOutputStream(out);

            adapter.writeConfigurationModel(bos, model, principal);

			bos.close();
			out.close();

            byte[] data = out.toByteArray();

            if (data == null || data.length == 0) {
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0156, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0156));
            }

            boolean inUse = trans.isNameInUse(model.getConfigurationID().getFullName());

            if (inUse) {
                trans.setSource(principal, model.getConfigurationID().getFullName(), data, ExtensionModuleManager.getChecksum(data));
            } else {
                trans.addSource(principal, ExtensionModuleTypes.CONFIGURATION_MODEL_TYPE,
                                    model.getConfigurationID().getFullName(),
                                    data,
                                    ExtensionModuleManager.getChecksum(data),
                                    model.getConfigurationID().getFullName() + " Configuration Model", //$NON-NLS-1$
                                    true);

            }
             
        } catch (Exception e) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0157, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0157, model.getConfigurationID()));

        }
	}

}
