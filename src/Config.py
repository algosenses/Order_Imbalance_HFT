import configparser

def read(filename):
    dictionary = {}
    
    config = configparser.ConfigParser()
    config.read(filename)

    for section in config.sections():
        dictionary[section] = {}
        for option in config.options(section):
            dictionary[section][option] = config.get(section, option)

    return dictionary