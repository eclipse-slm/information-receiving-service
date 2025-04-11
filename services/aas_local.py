from typing import List

from aas_python_http_client import AssetAdministrationShellDescriptor, \
    Submodel
from aas_python_http_client.rest import ApiException

from aas.couch_db_basyx_client import aas_obj_store
from aas.shell_repo_client import ShellRepoClient
from aas.submodel_repo_client import SubmodelRepoClient
from logger.logger import LOG
from services.aas_utils import encode_id, convert_shell_descriptors_to_shells, \
    extract_submodel_references_from_shell_descriptor, convert_client_object_to_basyx_object

# Client for local repos:
shell_repo_client = ShellRepoClient()
submodel_repo_client = SubmodelRepoClient()

def write_shells_based_on_remote_descriptors(descriptors: List[AssetAdministrationShellDescriptor]):
    shells = convert_shell_descriptors_to_shells(descriptors)
    for shell in shells:
        try:
            try:
                aas_obj_store.add(convert_client_object_to_basyx_object(shell))
            except KeyError as e:
                LOG.info(f"Skip adding shell to CouchDB: {e}")
            shell_repo_client.post_asset_administration_shell(shell)
        except ApiException as e:
            if e.status == 409:
                LOG.info(f"Skip creation of shell with id = '{shell.id}' because it exists already: Status {e.status}")
            else:
                raise


def delete_shells_base_on_remote_descriptors(remote_descriptors: List[AssetAdministrationShellDescriptor]):
    local_shells = shell_repo_client.get_all_asset_administration_shells().result

    for local_shell in local_shells:
        filtered_remote_descriptors = [remote_descriptor for remote_descriptor in remote_descriptors if remote_descriptor.id == local_shell.id]
        if len(filtered_remote_descriptors) == 0:
            try:
                # Delete shell from local CouchDB:
                aas_obj_store.discard(convert_client_object_to_basyx_object(local_shell))

                # Delete shell from local repo:
                shell_repo_client.delete_asset_administration_shell(local_shell.id)
                LOG.info(f"Shell with id = '{local_shell.id}' has been deleted.")
            except ApiException as e:
                if e.status == 404:
                    LOG.info(f"Skip deletion of shell with id = '{local_shell.id}' because it does not exist: Status {e.status}")
                else:
                    raise


def write_submodels(submodels: List[Submodel]):
    for submodel in submodels:
        try:
            submodel_repo_client.post_submodel(submodel)
            LOG.info(f"Submodel with id = '{submodel.id}' has been created.")
        except ApiException as e:
            if e.status == 409:
                LOG.info(f"Skip creation of submodel with id = '{submodel.id}' because it exists already: Status {e.status}")
            else:
                raise

def delete_submodels(remote_submodels: List[Submodel]):
    local_submodels = submodel_repo_client.get_all_submodels().result

    for local_submodel in local_submodels:
        filtered_remote_submodels = [remote_submodel for remote_submodel in remote_submodels if remote_submodel.id == local_submodel.id]
        if len(filtered_remote_submodels) == 0:
            try:
                # Delete submodel from local CouchDB:
                aas_obj_store.discard(local_submodel)
            except KeyError as e:
                LOG.info(f"Skip deletion of submodel from CouchDB: {e}")

            try:
                # Delete submodel from local repo:
                submodel_repo_client.delete_submodel_by_id(encode_id(local_submodel.id))
                LOG.info(f"Submodel with id = '{local_submodel.id}' has been deleted.")
            except ApiException as e:
                if e.status == 404:
                    LOG.info(f"Skip deletion of submodel with id = '{local_submodel.id}' because it does not exist: Status {e.status}")
                else:
                    raise


def cache_submodels(shell_id: str, submodels: List[Submodel]):
    # shell = aas_obj_store.get(shell_id)
    # shell.submodel = submodels
    for submodel in submodels:
        try:
            aas_obj_store.add(submodel)
        except KeyError as e:
            LOG.info(f"Skip adding submodel to CouchDB: {e}")

    return


def write_submodel_references(descriptors: List[AssetAdministrationShellDescriptor]):
    for descriptor in descriptors:
        shell_id = descriptor.id
        shell_id_enc = encode_id(shell_id)
        submodel_references = extract_submodel_references_from_shell_descriptor(descriptor)

        for submodel_reference in submodel_references:
            submodel_id = submodel_reference.keys[0].value
            if shell_exists(shell_id) and submodel_exists(submodel_id):
                try:
                    shell_repo_client.post_submodel_reference_aas_repository(body=submodel_reference, aas_identifier=shell_id_enc)
                    LOG.info(f"Reference from shell '{shell_id}' to submodel '{submodel_id}' has been created.")
                except ApiException as e:
                    if e.status == 409:
                        LOG.info(f"Skip creation of reference from shell '{shell_id}' to submodel '{submodel_id}' because it exists already: Status {e.status}")
                    else:
                        raise

def delete_submodel_references(remote_descriptors: List[AssetAdministrationShellDescriptor]):
    for remote_descriptor in remote_descriptors:
        remote_submodel_references = extract_submodel_references_from_shell_descriptor(remote_descriptor)
        remote_descriptor_id_enc = encode_id(remote_descriptor.id)
        local_submodel_references = shell_repo_client.get_all_submodel_references_aas_repository(remote_descriptor_id_enc).result

        for local_submodel_reference in local_submodel_references:
            local_submodel_id = local_submodel_reference.key[0].value
            filtered_remote_submodel_references = [remote_submodel_reference for remote_submodel_reference in remote_submodel_references if remote_submodel_reference.keys[0].value == local_submodel_id]
            if len(filtered_remote_submodel_references) == 0:
                try:
                    shell_repo_client.delete_submodel_reference_by_id_aas_repository(remote_descriptor_id_enc, encode_id(local_submodel_id))
                    LOG.info(f"Reference from shell '{remote_descriptor.id}' to submodel '{local_submodel_id}' has been deleted.")
                except ApiException as e:
                    if e.status == 404:
                        LOG.info(f"Skip deletion of reference from shell '{remote_descriptor.id}' to submodel '{local_submodel_id}' because it does not exist: Status {e.status}")
                    else:
                        raise


def shell_exists(shell_id: str) -> bool:
    shell_id_enc = encode_id(shell_id)

    try:
        shell_repo_client.get_asset_administration_shell_by_id(aas_identifier=shell_id_enc)
        return True
    except TypeError: # client fails with deserializing shell
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        else:
            raise


def submodel_exists(submodel_id: str) -> bool:
    submodel_id_enc = encode_id(submodel_id)

    try:
        submodel_repo_client.get_submodel_by_id(submodel_identifier=submodel_id_enc)
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        else:
            raise