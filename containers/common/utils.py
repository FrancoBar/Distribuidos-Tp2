import os
import logging
import hashlib
from configparser import ConfigParser

def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """
    config = ConfigParser(os.environ)
    config.read('./root/config.ini')
    return config

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

def clear_all_files(directory):
    for root, dirs, files in os.walk(directory):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

def _hash_string(string_to_hash):
    return int(hashlib.sha512(string_to_hash.encode()).hexdigest(), 16)


def hash_fields(message, hashing_attributes):
        hashing_string = ''
        for attribute in hashing_attributes: # Extends to more than 2 receiving ends
            hashing_string += f"-{message[attribute]}"
        return _hash_string(hashing_string)
