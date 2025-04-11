from services.in_memory_store.in_memory_store import InMemoryStore


class ShellDescriptorHandler:
    def __init__(self):
        self.in_memory_store = InMemoryStore()

    def get_shell_ids_by_asset_id(self, asset_id: str):
        """
        Get the shell ids linked to a specific Asset identifier.

        Args:
            asset_id (str): The identifier of the Asset.

        Returns:
            List[str]: A list of shell ids linked to the Asset identifier.
        """
        shell_ids = []
        for shell in self.in_memory_store.shells:
            if shell['assetInformation']['globalAssetId'] == asset_id:
                shell_ids.append(shell['id'])
        return shell_ids