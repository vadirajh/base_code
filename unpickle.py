import pickle
from sys import argv
import json
import pprint
pp = pprint.PrettyPrinter(indent=4)

pname, filen = argv


if not filen :
   filen = '/tmp/2f5f928b-c02e-41bf-b027-6e87bc1e956a_statefile.dat'

myjson =  pickle.load(open(filen, 'rb'))
pp.pprint(myjson['dssstoragedaemons'])
