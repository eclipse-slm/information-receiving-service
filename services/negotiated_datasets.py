import services.edc.contract_agreements as agreements
from services.edc.edr import EdrState
from services.edc import catalog
from services.edc import contract_negotiations as  contract_negotiation_service


filter_name_to_property_name = {
    "id": "edcid",
    "type": "edctype"
}

def get_negotiated_datasets():
    negotiated_agreements = agreements.get_contract_agreements_filtered({}, EdrState.NEGOTIATED)
    datasets = []

    for agreement in negotiated_agreements:
        asset_id = agreement.edcasset_id
        contract_negotiations = contract_negotiation_service.get_contract_negotiations_filtered({
            "contract_agreement_id": agreement.id
        })
        counter_party_address = contract_negotiations[0].edccounter_party_address

        datasets.append(
            catalog.get_dataset(asset_id, counter_party_address)
        )

    return datasets


def get_negotiated_datasets_filtered(filters: dict):
    negotiated_datasets = get_negotiated_datasets()

    for filter_name, property_value in filters.items():
        if filter_name not in filter_name_to_property_name:
            continue

        if property_value:
            negotiated_datasets = [
                dataset for dataset in negotiated_datasets if getattr(dataset, filter_name_to_property_name[filter_name]) == property_value
            ]

    return negotiated_datasets