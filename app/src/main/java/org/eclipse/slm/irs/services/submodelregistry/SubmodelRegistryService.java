package org.eclipse.slm.irs.services.submodelregistry;

import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelDescriptor;
import org.eclipse.slm.aas.model.submodelregistry.exceptions.SubmodelDescriptorNotFoundException;

public interface SubmodelRegistryService {

    SubmodelDescriptor getSubmodelDescriptorById(String submodelId) throws SubmodelDescriptorNotFoundException;

}
