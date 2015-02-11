import subprocess
from sys import argv

prog_name, fname =argv
tnail=fname[:len(fname)-4]+'_tn.jpg'
subprocess.call("ffmpeg -itsoffset -8 -i "+fname+" -vcodec mjpeg -vframes 1 -an -f rawvideo -s 120x90 -y "
                                +tnail, shell=True)
