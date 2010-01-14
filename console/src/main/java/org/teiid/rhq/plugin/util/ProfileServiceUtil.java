package org.teiid.rhq.plugin.util;

import java.util.Set;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.profileservice.spi.ProfileService;

public class ProfileServiceUtil {

	/**
	 * Get the passed in {@link ManagedComponent}
	 * 
	 * @return {@link ManagedComponent}
	 * @throws NamingException
	 * @throws Exception
	 */
	public static ManagedComponent getManagedComponent(
			ComponentType componentType, String componentName)
			throws NamingException, Exception {
		ProfileService ps = getProfileService();
		ManagementView mv = getManagementView(ps);

		ManagedComponent mc = mv.getComponent(componentName, componentType);
		return mc;
	}

	/**
	 * Get the {@link ManagedComponent} for the {@link ComponentType} and sub
	 * type.
	 * 
	 * @return Set of {@link ManagedComponent}s
	 * @throws NamingException
	 * @throws Exception
	 */
	public static Set<ManagedComponent> getManagedComponents(
			ComponentType componentType) throws NamingException, Exception {
		ProfileService ps = getProfileService();
		ManagementView mv = getManagementView(ps);

		Set<ManagedComponent> mcSet = mv.getComponentsForType(componentType);

		return mcSet;
	}

	/**
	 * @param {@link ManagementView}
	 * @return
	 */
	private static ManagementView getManagementView(ProfileService ps) {
		ManagementView mv = ps.getViewManager();
		mv.load();
		return mv;
	}

	/**
	 * @return {@link ProfileService}
	 * @throws NamingException
	 */
	private static ProfileService getProfileService() throws NamingException {
		InitialContext ic = new InitialContext();
		ProfileService ps = (ProfileService) ic
				.lookup(PluginConstants.PROFILE_SERVICE);
		return ps;
	}

}
