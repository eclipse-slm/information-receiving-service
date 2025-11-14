import os

from dotenv import load_dotenv

load_dotenv()

_counter_part_addresses = set(os.getenv("COUNTER_PART_ADDRESSES").split(","))

def get_counter_part_addresses():
    return _counter_part_addresses

def add_counter_part_address(address: str):
    _counter_part_addresses.add(address)

def remove_counter_part_address(address: str):
    _counter_part_addresses.remove(address)