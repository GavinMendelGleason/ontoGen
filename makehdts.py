#!/usr/bin/env python

import re
import argparse
import subprocess
import os
import sys
import shlex

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Merge multiple RDF files into a single HDT file')
    parser.add_argument('--base', help='Turtle file containing schema', default='instance')
    global_params = vars(parser.parse_args())
    fbase = global_params['base']
    
    # Get the largest numbered chunk
    matchbase = fbase+r'-([0-9]+)\.ttl'
    files = [f for f in os.listdir('.') if re.match(matchbase,f) ]
    mx = 0
    for f in files: 
        m = re.match(matchbase,f)
        if m:
            n = int(m.group(1))
            if n > mx:
                mx = n

    plist = []
    for i in range(0,mx+1):
        f = fbase + '-' + str(i) + '.ttl'
        if os.path.isfile(f): 
        
            fhdt = fbase + '-' + str(i) + '.hdt'
            if not os.path.isfile(fhdt): 
                cmd = 'rdf2hdt -f turtle '+f+' '+fhdt
                print("Running '%s'" % cmd)
                args = shlex.split(cmd)
                plist.append(subprocess.Popen(args))                
            else:
                print("Already have file %s" % fhdt)
        else:
            break

    for p in plist:
        # make sure everyone is done...
        p.wait()
        
    cmd2 = 'mergeHDT '+ fbase + '.hdt '+fbase+'*.hdt'
    subprocess.call(cmd2,shell=True)


    
