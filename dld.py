#! /usr/bin/env python3

import sys
import os
import getopt
import yaml
import uuid
import shutil
import logging
from docker import Client
from collections import defaultdict as ddict


def ddict2dict(d):
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = ddict2dict(v)
    return dict(d)


def ensure_dir_exists(dir, log):
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        log.warning("The given path \"" + dir + "\" already exists.")


class dld:
    configuration = None
    compose_config = ddict()
    workingDirectory = None

    def __init__(self, configuration, working_directory, log=logging.getLogger()):
        self.configuration = configuration
        self.workingDirectory = working_directory
        self.log = log
        print(self.configuration)
        ensure_dir_exists(self.workingDirectory, self.log)
        modelsVolume = self.workingDirectory + "/models"
        ensure_dir_exists(modelsVolume, self.log)

        self.pullImages(self.configuration)
        self.configureCompose(self.configuration, modelsVolume)
        self.provide_models(self.configuration["datasets"], modelsVolume)

    def pullImages(self, config):
        docker = Client()
        images = docker.images(filters={"label": "org.aksw.dld"})

        print(images)

        # docker.inspect_image

    def configureCompose(self, configuration, modelsVolume):
        self.configure_store(configuration)
        self.configure_load(configuration, modelsVolume)
        self.configure_present(configuration)

    def configure_store(self, configuration):
        if "store" not in configuration["setup"]:
            return
        default_graph = configuration["datasets"]["defaultGraph"]

        self.compose_config["store"]["environment"]["DEFAULTGRAPH"] = default_graph
        self.compose_config["store"].update(configuration["setup"]["store"])

    def configure_load(self, configuration, models_volume):
        if not "load" in configuration["setup"]:
            return
        self.compose_config["load"].update(configuration["setup"]["load"])
        self.compose_config["load"]["volumes"] = [models_volume + ":/import"]
        self.compose_config["load"]["links"] = ["store"]

    def configure_present(self, configuration):
        if not "present" in configuration["setup"]:
            return
        for k, v in configuration["setup"]["present"]:
            self.compose_config["present_" + k].update(v)
            self.compose_config["present_" + k]["links"] = ["store"]

    def provide_models(self, datasets, models_volume):
        for k, v in datasets.items():
            if k == "defaultGraph":
                continue
            if "file" in v:
                shutil.copyfile(v["file"], models_volume + "/" + v["file"])
                file = v["file"]
            elif "location" in v:
                # download v["location"] to modelsVolume
                print(v["location"])
                file = k + ".ttl"
            f = open(models_volume + "/" + file + ".graph", "w")
            f.write(v["uri"] + "\n")
            f.close()


def usage():
    print("please read at http://dld.aksw.org/ for further instructions")


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:w:u:f:l:",
                                   ["help", "config=", "workingdirectory=", "uri=", "file=", "location="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    configFile = "dld.yml"
    workingDirectory = str(uuid.uuid4())
    uri = None
    location = None
    file = None

    for opt, opt_val in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-c", "--config"):
            configFile = opt_val
        elif opt in ("-w", "--workingdirectory"):
            workingDirectory = opt_val
        elif opt in ("-u", "--uri"):
            uri = opt_val
        elif opt in ("-f", "--file"):
            file = opt_val
        elif opt in ("-l", "--location"):
            location = opt_val
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
    print(yaml.dump(ddict2dict(app.compose_config())))


if __name__ == "__main__":
    main()
