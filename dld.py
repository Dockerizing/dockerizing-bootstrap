#! /usr/bin/env python3

import sys
import getopt
import yaml
from collections import defaultdict

def ddict ():
    return defaultdict(ddict)

def ddict2dict(d):
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = ddict2dict(v)
    return dict(d)

class dld:
    configuration = None
    composeConfig = ddict()

    def __init__ (self, configuration):
        self.configuration = configuration
        print(self.configuration)
        self.provideModels(self.configuration["datasets"])

    def provideModels (self, datasets):
        defaultGraph = datasets["defaultGraph"]
        self.composeConfig["store"]["environment"]["DEFAULTGRAPH"] = defaultGraph
        #self.composeConfig.store.environment.DEFAULTGRAPH = defaultGraph

    def getComposeConfig (self):
        return self.composeConfig


def usage():
    print("please read at http://dld.aksw.org/ for further instructions")

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:u:f:l:", ["help", "config=", "uri=", "file=", "location="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    configFile = "dld.yml"
    uri = None
    location = None
    file = None

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--config"):
            configFile = a
        elif o in ("-u", "--uri"):
            uri = a
        elif o in ("-f", "--file"):
            file = a
        elif o in ("-l", "--location"):
            location = a
        else:
            assert False, "unhandled option"
    # read configuration file

    stream = open(configFile, 'r')
    config = yaml.load(stream)

    # Add command line arguments to configuration
    if (uri or file or location):
        if (uri and (file or location)):
            if (not "datasets" in config):
                config["datasets"] = {}
            if (not "default" in config["datasets"]):
                config["datasets"]["default"] = {}
            config["datasets"]["defaultGraph"] = uri
            config["datasets"]["default"]["uri"] = uri
            if (file):
                config["datasets"]["default"]["file"] = file
            elif (location):
                config["datasets"]["default"]["location"] = location
        else:
            print("only the combinations uri and file or uri and location are permitted")
            usage()
            sys.exit(2)

    if (not "datasets" in config or not "setup" in config):
        print("dataset and setup configuration is needed")
        usage()
        sys.exit(2)

    # start dld process
    app = dld(config)
    print(yaml.dump(ddict2dict(app.getComposeConfig())))

if __name__ == "__main__":
    main()
