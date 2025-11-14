package org.eclipse.slm.irs.services.shellregistry;

import org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShellDescriptor;
import org.eclipse.slm.common.aas.model.shellregistry.exceptions.ShellDescriptorNotFoundException;

public interface ShellRegistryService {

    AssetAdministrationShellDescriptor getShellDescriptorById(String aasId) throws ShellDescriptorNotFoundException;

}
