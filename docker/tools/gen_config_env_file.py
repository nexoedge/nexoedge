#!/usr/bin/python

import configparser
import sys

if len(sys.argv) < 3:
    print("Usage: gen_config_env_file.py <config_file> <prefix of env names>")
    sys.exit(1)

config_file=sys.argv[1]
prefix=sys.argv[2]

config = configparser.ConfigParser()
config.read_file(open(config_file))

# for all key in all section, print the environment variable name <prefix_section_key>=
for sec in config.sections():
    sec_part = sec.capitalize()
    print("# [%s]" % (sec))
    for key in config.options(sec):
        key_part = key.capitalize()
        print("%s_%s_%s=" % (prefix, sec_part, key_part))
