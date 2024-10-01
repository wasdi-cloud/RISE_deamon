import json
from types import SimpleNamespace
import sys, getopt
import logging
from osgeo import gdal

if __name__ == '__main__':
    sConfigFilePath = '/etc/rise/riseConfig.json'

    try:
        aoOpts, asArgs = getopt.getopt(sys.argv[1:], "hc:", ["config="])
    except getopt.GetoptError:
        print('RISE Deamon: python RiseDeamon.py -c <configfile>')
        sys.exit(2)

    for sOpt, sArg in aoOpts:
        if sOpt == '-h':
            print('RISE Deamon: python RiseDeamon.py -c <configfile>')
            sys.exit()
        if sOpt in ("-c", "--config"):
            sConfigFilePath = sArg

    sConfigContent = ""

    with open(sConfigFilePath, "r") as oConfigFile:
        sConfigContent = oConfigFile.read()

    oRiseConfig = json.loads(sConfigContent, object_hook=lambda d: SimpleNamespace(**d))

    print(oRiseConfig)
