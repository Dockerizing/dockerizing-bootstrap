#! /usr/bin/env python3

import sys
import os
import getopt
import yaml
import uuid
import shutil
import logging
from docker import Client
from collections import defaultdict

def ddict ():
    return defaultdict(ddict)

def ddict2dict(d):
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = ddict2dict(v)
    return dict(d)

def mkdirIfNotExists(dir, log):
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        log.warning("The given path \"" + dir + "\" already exists.")

class dld:
    configuration = None
    composeConfig = ddict()
    workingDirectory = None

    def __init__ (self, configuration, workingDirectory, log = logging.getLogger()):
        self.configuration = configuration
        self.workingDirectory = workingDirectory
        self.log = log
        print(self.configuration)
        mkdirIfNotExists(self.workingDirectory, self.log)
        modelsVolume = self.workingDirectory + "/models"
        mkdirIfNotExists(modelsVolume, self.log)

        self.pullImages(self.configuration)
        self.configureCompose(self.configuration, modelsVolume)
        self.provideModels(self.configuration["datasets"], modelsVolume)

    def pullImages (self, config):
        docker = Client()
        images = docker.images(filters = {"label": "org.aksw.dld"})

        print(images)

        #docker.inspect_image

    def configureCompose (self, configuration, modelsVolume):
        self.configureStore(configuration)
        self.configureLoad(configuration, modelsVolume)
        self.configurePresent(configuration)

    def configureStore (self, configuration):
        if not "store" in configuration["setup"]:
            return
        defaultGraph = configuration["datasets"]["defaultGraph"]
        self.composeConfig["store"]["environment"]["DEFAULTGRAPH"] = defaultGraph
        self.composeConfig["store"].update(configuration["setup"]["store"])

    def configureLoad (self, configuration, modelsVolume):
        if not "load" in configuration["setup"]:
            return
        self.composeConfig["load"].update(configuration["setup"]["load"])
        self.composeConfig["load"]["volumes"] = [modelsVolume + ":/import"]
        self.composeConfig["load"]["links"] = ["store"]

    def configurePresent (self, configuration):
        if not "present" in configuration["setup"]:
            return
        for k, v in configuration["setup"]["present"]:
            self.composeConfig["present_" + k].update(v)
            self.composeConfig["present_" + k]["links"] = ["store"]

    def provideModels (self, datasets, modelsVolume):
        for k,v in datasets.items():
            if k == "defaultGraph":
                continue
            if "file" in v:
                shutil.copyfile(v["file"], modelsVolume + "/" + v["file"])
                file = v["file"]
            elif "location" in v:
                # download v["location"] to modelsVolume
                print(v["location"])
                file = k + ".ttl"
            f = open(modelsVolume + "/" + file + ".graph", "w")
            f.write(v["uri"] + "\n")
            f.close()

    def getComposeConfig (self):
        return self.composeConfig


def usage():
    print("please read at http://dld.aksw.org/ for further instructions")

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:w:u:f:l:", ["help", "config=", "workingdirectory=", "uri=", "file=", "location="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    configFile = "dld.yml"
    workingDirectory = str(uuid.uuid4())
    uri = None
    location = None
    file = None

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--config"):
            configFile = a
        elif o in ("-w", "--workingdirectory"):
            workingDirectory = a
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
    app = dld(config, workingDirectory)
    print(yaml.dump(ddict2dict(app.getComposeConfig())))

if __name__ == "__main__":
    main()
