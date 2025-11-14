package org.eclipse.slm.irs.services.shellrepository;

import org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShell;
import org.eclipse.slm.common.aas.model.shellrepository.exceptions.ShellNotFoundException;

public interface ShellRepositoryService {

    AssetAdministrationShell getShellById(String aasId) throws ShellNotFoundException;

}
