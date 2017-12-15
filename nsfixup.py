#!/usr/bin/env python

import re
import codecs
import argparse

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Fixup namespaces in ntriples.')
    parser.add_argument('--input', help='Ntriple instance file in', default='instance.nt')
    parser.add_argument('--output', help='Ntriple instance file out', default='instance.out.nt')
    
    # Might be worth adding an input file which specifies prefixes. 
    global_params = vars(parser.parse_args())

    
    fh = codecs.open(global_params['input'], "r", encoding='utf8')
    oh = codecs.open(global_params['output'], "w", encoding='utf8')

    line = fh.readline()
    while line != u'':
        
        line = re.sub('_:','http://dacura.scss.tcd.ie/instance/ipg#',line)
        line = re.sub('dacuraDataset:','http://dacura.scss.tcd.ie/ontology/dacuraDataset#',line)

        oh.write(line)
        line = fh.readline()


    fh.close()
    oh.close()
