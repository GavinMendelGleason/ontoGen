#!/usr/bin/env python

import re
import argparse
import subprocess
import os.path
import sys

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Merge multiple RDF files into a single HDT file')
    parser.add_argument('--base', help='Turtle file containing schema', default='instance.ttl')
    global_params = vars(parser.parse_args())
    base = global_params['base']
    fbase = re.sub(r'(.*).ttl$',r'\1',base)
    for i in range(0,100):
        f = fbase + '-' + str(i) + '.ttl'
        if os.path.isfile(f): 
        
            fhdt = fbase + '-' + str(i) + '.hdt'
            cmd = 'rdf2hdt -f turtle '+f+' '+fhdt
            print("Running '%s'" % cmd)
            subprocess.call(cmd,shell=True)
        else:
            break

    cmd2 = 'mergeHDT '+ fbase + '.hdt *.hdt'
    subprocess.call(cmd2,shell=True)


    
