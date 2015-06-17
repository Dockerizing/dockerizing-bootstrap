#! /usr/bin/env python3

import sys
import getopt
import yaml

class dld:

    configuration = None

    def __init__ (self, configuration):
        self.configuration = configuration

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

    print(config)

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


    print(config)

    # start dld app
    app = dld(config)

if __name__ == "__main__":
    main()
