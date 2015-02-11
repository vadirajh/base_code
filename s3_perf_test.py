#!/usr/bin/env python

import os
import re
import sys
import time
import hashlib
import httplib
import datetime
import hmac
import urllib
import mimetypes
import math
from logging import debug, info, warning, error
from optparse import OptionParser
import multiprocessing
import uuid
import json
import random

#### GLOBALS ####
PERF_FILE = "/tmp/s3_perf_file.%s" % time.time()
RFC822_FMT = '%a, %d %b %Y %H:%M:%S GMT'
ISO8601_FMT = '%Y-%m-%dT%H:%M:%S.000Z'
DEFAULT_FILE_SIZE_KIB = 16384

global_hosts_list = []

#### S3 BUCKET CLASS ####
class Client(object):
    def __init__(self, protocol, name, policy_id=None, access_key=None, secret_key=None, base_url=None, timeout=None, use_ssl=False, rate_limit=None):
        info("Accessing bucket '%s' at %s with user '%s'", name, base_url, access_key)
        
        self.protocol = protocol
        self.name = name
        self.policy = policy_id
        self.access_key = access_key
        self.secret_key = secret_key
        self.base_url = base_url
        self.timeout = timeout
        self.rate_limit = rate_limit
   
        self.host = base_url.split(':')[0].strip()
        self.port = base_url.split(':')[1].strip()
        self.use_ssl = use_ssl
        self.Reconnect()

    def Reconnect(self):
        try:
            if self.use_ssl == 1:
                self.opener = httplib.HTTPSConnection(self.host, self.port, timeout=self.timeout)
            else:
                self.opener = httplib.HTTPConnection(self.host, self.port, timeout=self.timeout)
        except Exception, ex:
            print "Failed to connect to %s:%s" % (self.host, self.port)
            print str(ex)
            exit(1)

    def __str__(self):
        return "<%s %s at %r>" % (self.__class__.__name__, self.name, self.base_url)

    def __repr__(self):
        return self.__class__.__name__ + "(%r, access_key=%r, base_url=%r)" % (
            self.name, self.access_key, self.base_url)

    ### Signature calculation ###
    def sign_description(self, desc):
        """ Sign the description """
        info("StringToSign: %s", desc)
        hasher = hmac.new(self.secret_key, desc.encode("utf-8"), hashlib.sha1)
        return hasher.digest().encode("base64").replace("\n", "")

    def make_description(self, method, key=None, data=None, headers={}, bucket=None):
        """ Make description from headers """
        res = self.canonicalized_resource(key, bucket=bucket)
        return "\n".join((method, headers.get("Content-MD5", ""),
            headers.get("Content-Type", ""), headers.get("Date", ""))) + "\n" +\
            _amz_canonicalize(headers) + res

    def get_request_signature(self, method, key=None, data=None, headers={}, bucket=None):
        """ Create signature """
        return self.sign_description(self.make_description(method, key=key,
            data=data, headers=headers, bucket=bucket))

    def canonicalized_resource(self, key, bucket=None):
        res = "/"
        if bucket or bucket is None:
            res += aws_urlquote(bucket or self.name)
        res += "/"
        if key:
            res += aws_urlquote(key)
        return res

    ### URL ###
    def make_url(self, key, args=None, arg_sep="&", protocol=None):
        url = "/"
        if protocol == "AXR":
            url = "namespace/"
        url += self.name + "/"
        if key:
            url += aws_urlquote(key)
        if args and hasattr(args, "iteritems"):
            url += "?"
            for key, val in args.iteritems():
                url += str(key) + "=" + str(val) + arg_sep
            url = url[:-1]
        info("URL: %s", url)
        return url

    ### Request ###
    def do_request(self, method, key=None, args=None, data=None, headers={}):
        # set debug level if necessary
        #self.opener.set_debuglevel(1)

        # set headers
        headers = headers.copy()
        if "Date" not in headers:
            headers["Date"] = time.strftime(RFC822_FMT, time.gmtime())
        
        if self.protocol == "S3":
            if data and "Content-MD5" not in headers and config.md5:
                headers["Content-MD5"] = aws_md5(data)
            if "Authorization" not in headers:
                sign = self.get_request_signature(method, key=key, data=data, headers=headers)
                headers["Authorization"] = "AWS %s:%s" % (self.access_key, sign)
        
        # set our latency limiters
        if self.rate_limit and self.rate_limit > 0:
            read_size = self.rate_limit * 1024
        else:
            read_size = config.read_size_kib
        sleep_latency = 0
        
        if "GET" in method and (config.random_read_offset_min >= 0 and config.random_read_offset_max > 0):
            random_read_offset = random.randrange(config.random_read_offset_min, config.random_read_offset_max + 1)
            headers["Range"] = "bytes=%d-%d" % (random_read_offset, random_read_offset + config.read_size_kib - 1)

        # set url
        url = self.make_url(key, args, protocol=self.protocol)
        try:
            # do the request
            info("Request: %s, %s", method, headers)
            op_start = datetime.datetime.now()
            
            if "PUT" in method:
                self.opener.putrequest(method, url)
                for k,v in headers.iteritems():
                    #print "HEADER: ",k,v
                    self.opener.putheader(k,v)
                self.opener.endheaders()
                while True:                   
                    chunk = data.read(read_size)
                    if not chunk:
                        break
                    body_start = datetime.datetime.now()
                    time.sleep(sleep_latency)  # default is 0 (noop)
                    self.opener.send(chunk)
                    body_end = datetime.datetime.now()
                    
                    if self.rate_limit:
                        latency = (body_end - body_start).total_seconds()
                        sleep_latency = max(0.0, sleep_latency + (1.0 - latency))
                        #print "Blocksize: %d KiB  Seconds: %f  Rate: %d KiB/s - Sleep for %f seconds to get under %d KiB/s rate." % (read_size/1024, latency, (self.rate_limit / latency), sleep_latency, self.rate_limit)                    
            else: # all other methods such as HEAD, LIST and etc...
                self.opener.request(method, url, data, headers)

            # get response
            response = self.opener.getresponse()
            
            '''
            if response.status != httplib.OK and \
               response.status != httplib.CREATED and \
               response.status != httplib.ACCEPTED and \
               response.status != httplib.PARTIAL_CONTENT:
            '''
            if ( not (response.status >= 200 and response.status < 300)):
                print "Status response:", response.status
                raise httplib.HTTPException(response)

            sleep_latency = 0 
            # read and discard the response body in manageable chunks
            while True:            
                body_start = datetime.datetime.now()
                time.sleep(sleep_latency)  # default is 0 (noop)
                response_body = response.read(read_size)
                body_end = datetime.datetime.now()
                if self.rate_limit:
                    print body_end
                    print body_start
                    print dir(body_end - body_start)
                    latency = (body_end - body_start).total_seconds()
                    sleep_latency = max(0.0, sleep_latency + (1.0 - latency))
                    #print "Blocksize: %d KiB  Seconds: %f  Rate: %d KiB/s - Sleep for %f seconds to get under %d KiB/s rate." % (read_size/1024, latency, (self.rate_limit / latency), sleep_latency, self.rate_limit)
                 
                if len(response_body) == 0: # len is O(1)
                    break            
                
        except httplib.HTTPException, ex:
            raise ex
        except:
            '''
            import traceback
            traceback.print_exc()
            print "1 Unexpected error:", sys.exc_info()[0]
            '''
            raise
        else:
            return response, response_body

    ### Object operations ###
    def get(self, key):
        try:
            return self.do_request("GET", key=key)
        except Exception, e:
            raise e

    def info(self, key):
        try:
            return self.do_request("HEAD", key=key)
        except Exception, e:
            raise e

    def put(self, key, data=None, metadata={}, mimetype=None, headers={}):
        headers = headers.copy()
        if mimetype:
            headers["Content-Type"] = str(mimetype)
        elif "Content-Type" not in headers:
            headers["Content-Type"] = guess_mimetype(key)
        headers.update(metadata_headers(metadata))
        if "Content-Length" not in headers:
            headers["Content-Length"] = config.file_size_kib * 1024 
        if "Content-MD5" not in headers and config.md5:
            headers["Content-MD5"] = aws_md5(data)

        try:
            return self.do_request("PUT", key=key, data=data, headers=headers)
        except Exception, e:
            raise e

    def delete(self, key):
        try:
            return self.do_request("DELETE", key=key)
        except Exception, e:
            raise e

    def head(self, key):
        try:
            return self.do_request("HEAD", key=key)
        except Exception, e:
            raise e
        
    ### Bucket operations ###
    def list_bucket(self, key, marker=None, max_keys=None):
        try:
            args = {}
            if marker:
                args.update( { "marker": marker } )
            if max_keys:
                args.update( { "max-keys": max_keys } )
            return self.do_request("GET", key=key, args=args)
        except Exception, e:
            raise e
        
    ### AXR specific  methods
    def do_configure_dss_ns(self):
        """check our namespace if it exists, if not, create one based on a supplied
        policy id and return a namespace to be used"""
        if self.name == None:
            self.name = "python-PUT-GET-"+str(uuid.uuid4())
            if self.policy and config.small_file_threshold:
                body = '{ "Name":"'+self.name+'", "Policy-Id":"'+self.policy+\
                '", "Small-Files-Policy-Id":"'+config.small_file_policy_id+\
                '", "Small-Files-Threshold":'+str(int(config.small_file_threshold)*1024)+\
                ' }'
                print "Creating namespace", self.name, "with policy-id",self.policy, body
            else:
                body = ' {"Name": "'+self.name+'", "Policy-Id":"'+self.policy+'" }'
                print "Creating namespace", self.name, "with policy-id", self.policy, body
    
            url = "/manage/namespace?meta=json"
            try:
                self.opener.request("PUT", url, body)
                response = self.opener.getresponse()
            except httplib.HTTPException, ex:
                raise ex
            except:
                print "2 Unexpected error:", sys.exc_info()[0]
                raise
            
        else:
            print "Getting policy-id of namespace (", self.name, ") for file size", config.file_size_kib
            url = "/manage/namespace/"+self.name+"?meta=json"
            try:
                self.opener.request("GET", url)
                response = self.opener.getresponse()
            except httplib.HTTPException, ex:
                raise ex
            except:
                print "3 Unexpected error:", sys.exc_info()[0]
                raise
                        
            if response.status == 404:
                print "Namespace", self.name, "doesn't exist"
                sys.exit(1)
        
        if response.status == 501:
            print "Is this an AXR service you are connecting to?"
            print "Debug:", response.status, response.read()
            sys.exit(1)
            
        # Ready policy information
        jsonResponse = json.loads(response.read())
        small_files_threshold_bytes = jsonResponse.get('Small-Files-Threshold')
        
        # determine which policy id to use
        policy_key="Policy-Id"
        if small_files_threshold_bytes:
            print "Small-Files-Threshold:", small_files_threshold_bytes, "bytes"
            file_size_bytes=1024 * config.file_size_kib
            if file_size_bytes <= small_files_threshold_bytes:
                policy_key="Small-Files-Policy-Id"
    
        # determine the policy id
        self.policy = jsonResponse.get(policy_key)
        if self.policy == None:
            print "Error: could not get policy id of namespace", self.name
            sys.exit(1)
    
        # Note: no repair strategies: if a blockstore fails the put should fail,
        # otherwise our results are not accurate.
        url = "/manage/policy/"+self.policy+"?meta=json"
        try:
            self.opener.request("GET", url)
            response = self.opener.getresponse()
        except httplib.HTTPException, ex:
            raise ex
        except:
            print "4 Unexpected error:", sys.exc_info()[0]
            raise
                    
        if response.status == 404:
            print "Protocol", self.name, "doesn't exist"
            sys.exit(1)
            
        policy = json.loads(response.read())

        print "SAFETY_STRATEGIES =", policy.get('Safety-Strategy')
        if policy.get('Safety-Strategy') and config.allow_all_strategies == False:
            print "To be sure of the test results, the specified policy should have no safety strategies."
            sys.exit(1)
    
        print "NAMESPACE_NAME =", self.name
        print "POLICY_ID =", self.policy
        return self.name, policy

#### UTILITIES ####
def create_perf_file(filename, ksize, sparse=True, random=False):
    """ Create a perf file """
    try:
        if os.path.exists(filename):
            os.remove(filename)
        f = open(filename, "wb")
        if sparse and not random:
            print "FILE_TYPE = Sparse"
            f.seek(long(ksize*1024)-1)
            f.write(b'\x00')
        elif random:
            print "FILE_TYPE = Random"
            f.write(os.urandom(long(ksize*1024)))
        else:
            print "FILE_TYPE = Zeros"
            f.write(b'\x00' * (long(ksize*1024)))

    finally:
        f.close()

def _amz_canonicalize(headers):
    rv = {}
    for header, value in headers.iteritems():
        header = header.lower()
        if header.startswith("x-amz-"):
            rv.setdefault(header, []).append(value)
    parts = []
    for key in sorted(rv):
        parts.append("%s:%s\n" % (key, ",".join(rv[key])))
    return "".join(parts)

def expire2datetime(expire, base=None):
    if hasattr(expire, "timetuple"):
        return expire
    if base is None:
        base = datetime.datetime.now()
    try:
        return base + expire
    except TypeError:
        unix_eighties = 315529200
        if expire < unix_eighties:
            return base + datetime.timedelta(seconds=expire)
        else:
            return datetime.datetime.fromtimestamp(expire)

def aws_md5(data):
    hasher = hashlib.new("md5")
    if hasattr(data, "read"):
        data.seek(0)
        while True:
            chunk = data.read(8192)
            if not chunk:
                break
            hasher.update(chunk)
        data.seek(0)
    else:
        hasher.update(str(data))
    return hasher.digest().encode("base64").rstrip()

def aws_urlquote(value):
    if isinstance(value, unicode):
        value = value.encode("utf-8")
    return urllib.quote(value, "/")

def guess_mimetype(fn, default="application/octet-stream"):
    """Guess a mimetype from filename *fn*."""
    if "." not in fn:
        return default
    bfn, ext = fn.lower().rsplit(".", 1)
    if ext == "jpg": ext = "jpeg"
    return mimetypes.guess_type(bfn + "." + ext)[0] or default

#def headers_metadata(headers):
#    return dict((h[11:], v) for h, v in headers.iteritems()
#                        if h.lower().startswith("x-amz-meta-"))

def metadata_headers(metadata):
    return dict(("X-AMZ-Meta-" + h, v) for h, v in metadata.iteritems())

#def info_dict(headers):
#    def _rfc822_dt(v):
#        return datetime.datetime.strptime(v, RFC822_FMT)
#
#    def _iso8601_dt(v):
#        return datetime.datetime.strptime(v, ISO8601_FMT)
#
#    rv = {"headers": headers, "metadata": headers_metadata(headers)}
#    if "content-length" in headers:
#        rv["size"] = int(headers["content-length"])
#    if "content-type" in headers:
#        rv["mimetype"] = headers["content-type"]
#    if "date" in headers:
#        rv["date"] = _rfc822_dt(headers["date"])
#    if "last-modified" in headers:
#        rv["modify"] = _rfc822_dt(headers["last-modified"])
#    return rv

def str2bool(v):
  return str(v).lower() in ("yes", "true", "t", "1")

#### MAIN PERFORMANCE TESTING ####
def print_statistics(action, bucket, duration, num_series, num_files_per_series, file_size_kib):
    """ Print statistics for action """
    front_files = num_series * num_files_per_series
    front_files_sec = round(front_files / duration, 2)
    front_kib = front_files * file_size_kib
    front_kib_sec = round(front_kib / duration, 2)
    front_mib = front_kib / 1024
    front_mib_sec = round(front_kib_sec / 1024, 2)

    if config.protocol == "AXR":
        
        # Calculations varies based on codec. Latest version is 6
        if config.policy.get('Codec-Version') != 6:
            print "  WARNING: Codec-Version", config.policy.get('Codec-Version'), 
            "has changed, your ENCODED stats are not likely to be accurate"
        
        #v4: superblock_size*1.2*1.0267*spread_width/(spread_width - safety) 
        #    + n_messages*1.2*1.0267*spread_width/(spread_width - safety)*24
        #v6: superblock_size*1.1*1.0378*spread_width/(spread_width - safety) 
        #    + n_messages*1.1*1.0378*spread_width/(spread_width - safety)*4 
        #    + spread_width*120 + spread_width*12
        
        cb_factor = 1.1 * 1.0378 
        cb_overhead = 4
        cb_overhead_disk = config.policy.get('Spread-Width')*120 + config.policy.get('Spread-Width')*12
        
        if action == 'PUT':
            cb_spread_width = config.policy.get('Spread-Width')
            cb_safety = config.policy.get('Safety')
            if config.policy.get('Full-Copy'):
                full_copy_size_kib = file_size_kib
                cb_spread_width -= 1
                cb_safety -= 1
            else:
                full_copy_size_kib = 0
            cb_factor_put = (cb_factor * cb_spread_width) / (cb_spread_width - cb_safety)
            num_sbs = (file_size_kib * 1024.0 + config.policy.get('Max-Superblock-Size') - 1 ) / config.policy.get('Max-Superblock-Size')
            cb_header_overhead_put_per_sb = cb_factor_put * config.policy.get('N-Messages') * cb_overhead + cb_overhead_disk
            cb_header_overhead_put = num_sbs * cb_header_overhead_put_per_sb # cb_header_overhead_put is in bytes
            back_kib = (front_kib * cb_factor_put ) + full_copy_size_kib + (cb_header_overhead_put_per_sb / 1024.0)
        elif action == 'GET':
            num_sbs = ( config.read_size_kib * 1024.0 + config.policy.get('Max-Superblock-Size') - 1 ) / config.policy.get('Max-Superblock-Size')
            header_overhead_get_per_sb = cb_factor * config.policy.get('N-Messages') * cb_overhead + cb_overhead_disk
            header_overhead_get = num_sbs * header_overhead_get_per_sb  # HEADER_OVERHEAD_GET is in bytes
            if config.policy.get('Full-Copy'):
                back_kib = front_kib
            else:
                back_kib = front_kib * cb_factor + (header_overhead_get / 1024.0)
                full_copy_size_kib = 0
        else:
            back_kib = front_kib
        
        back_kib_sec = round(back_kib / duration, 2)
        back_mib = back_kib / 1024.0
        back_mib_sec = round(back_kib_sec/1024.0, 2)

    oneliner = []
    print "%s TIMEOUT %s seconds" % (action, config.timeout)
    oneliner.append(action)
    oneliner.append("ONELINER")
    oneliner.append(config.timeout)
    oneliner.append(num_series)
    if config.rate_limit:
        print "%s RATE_LIMIT %s KiB/s" % (action, config.rate_limit)
        oneliner.append(config.rate_limit)
    if action == "GET" and config.random_read_offset_max:
        print "%s RANDOM_RANGE %s - %s bytes" % (action, config.random_read_offset_min, config.random_read_offset_max)
        oneliner.append(config.random_read_offset_min)
        oneliner.append(config.random_read_offset_max)
    if config.protocol == "S3":
        print "%s BUCKET %s" % (action, bucket)
        oneliner.append(bucket)
    elif config.protocol == "AXR":
        print "%s NAMESPACE %s" % (action, bucket)
        print "%s POLICY %s/%s" % (action, 
                                    config.policy.get('Spread-Width'), 
                                    config.policy.get('Safety'),
                                    )
        print "%s FULL_COPY %s" % (action, config.policy.get('Full-Copy'))
        oneliner.append(bucket)
        oneliner.append(config.policy.get('Spread-Width'))
        oneliner.append(config.policy.get('Safety'))
        oneliner.append(config.policy.get('Full-Copy'))
    print "%s DURATION %s sec" % (action, round(duration,2))
    oneliner.append(round(duration,2))
    print "%s PERF %s files, %s files/sec" % (action, front_files, front_files_sec)
    oneliner.append(front_files)
    oneliner.append(front_files_sec)

    if action != "DELETE":
        print "%s UNENCODED %s KiB, %s KiB/sec, %s MiB/sec" % (action, round(front_kib,2), front_kib_sec, front_mib_sec)
        oneliner.append(round(front_kib,2))
        oneliner.append(front_kib_sec)
        oneliner.append(front_mib_sec)
        if config.protocol == "AXR":
            print "%s ENCODED %s KiB, %s KiB/sec, %s MiB/sec" % (action, round(back_kib,2), back_kib_sec, back_mib_sec)
            oneliner.append(round(back_kib,2))
            oneliner.append(back_kib_sec)
            oneliner.append(back_mib_sec)
            
    print " ".join(map(str, oneliner))

def parse_arguments():
    """ Option parser """
    global global_hosts_list

    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    # Default options for all protocols
    parser.add_option("--protocol", action="store", dest="protocol", 
                      help="Which protocol to use: S3 or AXR ") 
    parser.add_option("--hosts", action="store", dest="hosts", 
                      help="List of host:port to use, comma-separated. " 
                      "The series will be round-robine load balanced across the hosts.")
    parser.add_option("--hosts-file", action="store", dest="hosts_file", 
                      help="File containing list of host:port to use, "
                      "one per line. The series will be load balanced across the hosts.")
    parser.add_option("--tests", action="store", dest="tests", default="PUT GET",
                      help="A sequence of tests to perform. Currently supported: PUT, GET, LIST and DELETE. "
                      "Default is \"PUT GET\".")
    parser.add_option("--num-series", action="store", dest="num_series", type="int", default=8,
                      help="The number of series of PUT/GET to run in parallel."
                      "Series are distributed evenly over all host specified using"
                      "\"--hosts\". For example, with 12 series and 3 hosts, each host will receive 4 series."
                      "Default is 8.")
    parser.add_option("--num-files-per-series", action="store", dest="num_files_per_series", type="int", default=100,
                      help="The number of files to PUT/GET per series."
                      "Default is 100.")
    parser.add_option("--file-size-mib", action="store", dest="file_size_mib", type="long",
                      help="The size in mebibytes of the file to PUT/GET."
                      "Default is 16.")
    parser.add_option("--file-size-kib", action="store", dest="file_size_kib", type="long",
                      help="The size in kibibytes of the file to PUT/GET."
                      "Default is 16384")
    parser.add_option("--read-size-mib", action="store", dest="read_size_mib", type="long",
                      help="The size in mebibytes of the block of data to read from each file."
                      "Default is to read the entire file.")
    parser.add_option("--read-size-kib", action="store", dest="read_size_kib", type="long",
                      help="The size in kebibytes of the block of data to read from each file."
                      "Default is to read the entire file.")    
    parser.add_option("--random-read-offset-min", action="store", dest="random_read_offset_min", type="long",
                      help="The minimum offset in bytes to start reading at."
                      "A random value in the range of the minimum and maximum offset is chosen."
                      "See also option --random-read-offset-max.")
    parser.add_option("--random-read-offset-max", action="store", dest="random_read_offset_max", type="long",
                      help="The maximum offset in bytes to start reading at."
                      "A random value in the range of the minimum and maximum offset is chosen."
                      "See also option --random-read-offset-min.")   
    parser.add_option("--id", action="store", dest="pid", type="int",
                      help="A unique id used to avoid collisions between object names of different" 
                      " parallel runs of the script. Default is process id. For \"--tests GET\","
                      " specify the id of the PUT run")
    parser.add_option("--use-ssl", action="store_true", default=False, dest="use_ssl",
                      help="Use HTTPS instead of HTTP")
    parser.add_option("--rate-limit", action="store", default=None, dest="rate_limit", type="long",
                      help="Limit the bandwidth to a give rate in KiB/s")
    parser.add_option("--timeout", action="store", default=10, dest="timeout", type="int",
                      help="Timeout in seconds for a connection to be made.")
    parser.add_option("--use-sparse", action="store", default=True, dest="use_sparse",
                      help="Use sparse files.Default is True")
    parser.add_option("--use-random", action="store", default=False, dest="use_random",
                      help="Use random data in files. Default is False")
    parser.add_option("--run-on", action="store", default="", dest="run_on",
                      help="Run this program on each host in parallel."
                      " Default is \"\", meaning execute on localhost without using ssh.")
    parser.add_option("--ssh-username", action="store", default="", dest="ssh_username",
                      help="The username to use during ssh operations, see --run-on") 
    parser.add_option("--ssh-password", action="store", default="", dest="ssh_password",
                      help="The password to use during ssh operations, see --run-on")         
    
    # S3 protocol parameters
    parser.add_option("--bucket", action="store", default=None, dest="bucket", help="S3 bucket name")
    parser.add_option("--access-key", action="store", dest="access_key", help="S3 access key")
    parser.add_option("--secret-key", action="store", dest="secret_key", help="S3 secret key")
    parser.add_option("--list-chunks", action="store", default=100, dest="list_chunks", type="int",
                      help="S3 number of keys to retrieve per call. Only used for LIST action")
    parser.add_option("--md5", action="store_true", default=False, dest="md5", 
                      help="S3 calculate Content-MD5 header")
    # AXR protocol parameters
    parser.add_option("--namespace", action="store", default=None, dest="namespace_name", help="AXR: namespace name")
    parser.add_option("--policy-id", action="store", default=None, dest="policy_id", help="AXR: policy id")
    parser.add_option("--allow-all-strategies", action="store_true", default=False, 
                      dest="allow_all_strategies", help="AXR: allow all safety stretegies")
    parser.add_option("--small-files-policy-id", action="store", default=None, 
                      dest="small_file_policy_id", help="AXR: id of the small files policy for the namespace to create for the test run")
    parser.add_option("--small-file-threshold", action="store", default=None, 
                      dest="small_file_threshold", help="AXR: maximum size in bytes for which the small files policy should be used")
    

    (options, args) = parser.parse_args()

    # protocol validation
    if options.protocol == None:
        print "please specify which protocol you wish to use: S3 or AXR"
        exit(1)
    
    # S3 specific options
    if options.protocol == "S3":
        if options.bucket == None:
            print "please specify a S3 bucket with --bucket"
            exit(1)
        else:
            options.name = options.bucket

        if options.access_key == None or options.secret_key == None:
            print "please specify a S3 access and secret key (--access-key,--secret-key)"
            exit(1)
    
        if options.list_chunks < 10:
            print "list chunks must higher than 10"
            exit(1)

    # AXR specific options
    if options.protocol == "AXR":
        if options.namespace_name and options.policy_id == None:
            options.name = options.namespace_name
        elif options.policy_id and options.namespace_name == None:
            options.name = None
        else:
            print "please specify an AXR namespace with --namespace or policy id with --policy-id"
            sys.exit(1)
            
        if options.policy_id:
            if (options.small_file_policy_id and options.small_file_threshold) \
            or (options.small_file_policy_id == None and options.small_file_threshold == None):
                pass
            else:
                print "please specify both --small-file-policy and --small-file-threshold"
                sys.exit(1)

    # generic options
    if len(options.run_on) > 0 and len(options.ssh_username) == 0 and len(options.ssh_password) == 0:
        print "If you are going to run this script on remote machines, then you"\
        " also specify --ssh-username and --ssh-password"
        sys.exit(1)
    elif (len(options.ssh_username) > 0 or len(options.ssh_password) > 0) and len(options.run_on) == 0:
        print "You need to specify --run-on if you wish to provide a --ssh-username and --ssh-password."
        sys.exit(1)
    
    if options.hosts == None and options.hosts_file == None:
        print "please specify the client host(s) with option --hosts or --hosts-file"
        exit(1)
    elif options.hosts:
        global_hosts_list = options.hosts.split(',')
    elif options.hosts_file:
        if not os.path.exists(options.hosts_file):
            print "File '%s' does not exist." % options.hosts_file
            exit(1)

        with open(options.hosts_file, "rb") as fp:
            global_hosts_list = fp.readlines()

    if options.file_size_mib and options.file_size_kib == None:
        options.file_size_kib = int(options.file_size_mib * 1024)
    elif options.file_size_mib == None and options.file_size_kib == None:
        options.file_size_kib = DEFAULT_FILE_SIZE_KIB

    if options.read_size_mib and options.read_size_kib == None:
        options.read_size_kib = int(options.read_size_mib * 1024)
    elif options.read_size_mib == None and options.read_size_kib == None:
        options.read_size_kib = options.file_size_kib

    if (options.random_read_offset_min and options.random_read_offset_min < 0)\
        or (options.random_read_offset_min > options.file_size_kib*1024):
        print "random-read-offset-min should be >= 0 and less than the size of the file itself"
        sys.exit(1)

    if (options.random_read_offset_max and options.random_read_offset_max <= options.random_read_offset_min)\
        or (options.random_read_offset_max > options.file_size_kib*1024):
        print "random-read-offset-max should be > random-read-offset-min and less the size of the file itself"
        sys.exit(1)

    if options.pid == None:
        options.pid = os.getpid()

    if options.rate_limit and options.rate_limit < 1:
        print "rate-limit must be greater than 0"
        
    if options.timeout < 1:
        print "timeout must be greater than 0"
        
    options.use_sparse = str2bool(options.use_sparse)
    options.use_random = str2bool(options.use_random)

    return options

def open_connection(host):
    """ Open a S3 connection to a bucket """
    return Client(  protocol = config.protocol,
                    name = config.name,
                    policy_id = config.policy_id,
                    access_key = config.access_key,
                    secret_key = config.secret_key,
                    base_url = host,
                    timeout = config.timeout,
                    use_ssl = config.use_ssl,
                    rate_limit = config.rate_limit)

def do_action(host, action, i, q):
    """2 Perform action (PUT, GET, DELETE) for all files in a serie """

    client = open_connection(host)
    latencies = {}

    for j in xrange(1, config.num_files_per_series + 1): # files per series loop
        filename = "-file" + str(config.pid) + ".dat_" + str(i) + "_" + str(j)
        try:
            op_start = datetime.datetime.now()
            if action == "PUT":
                try:
                    with open(PERF_FILE, "rb") as fp:
                        client.put(filename, fp)
                except Exception, e:
                    if e.errno != 104:
                        raise e
                    client = open_connection(host)
                    with open(PERF_FILE, "rb") as fp:
                        client.put(filename, fp)
            elif action == "GET":
                f = client.get(filename)
            elif action == "DELETE":
                client.delete(filename)
            else:
                print "Unsupported action '%s'" % action
                return 1
            op_end = datetime.datetime.now()
            op_latency = (op_end - op_start).total_seconds()*1000000.0
            latency_bucket = int(math.log(op_latency,2))
            if latency_bucket not in latencies:
                latencies[latency_bucket] = 0
            latencies[latency_bucket] += 1
        except httplib.HTTPException, ex:
            print "Failed to %s file '%s'" % (action, filename)
            print "  * Error: %s %s (%s)" % (ex.status, ex.reason, ex.msg)
            return 1
    q.put(latencies)
    return 0

def do_listing(host):
    """ List all files in a bucket """
    client = open_connection(host)
    duration = 0
    last_key = None
    is_truncated = "True"
    nr_of_keys = 0

    while is_truncated != "false":
        # Do listing and calc duration
        start_time = datetime.datetime.now()
        response, listing = client.list_bucket(key='', marker=last_key, max_keys=config.list_chunks)
        duration += (datetime.datetime.now() - start_time).total_seconds()

        # Check if listing is final 
        is_trunc_start = listing.rfind("<IsTruncated>") + len("<IsTruncated>")
        is_trunc_end = listing.rfind("</IsTruncated>")
        is_truncated = listing[is_trunc_start:is_trunc_end]

        if is_truncated != "false":
            # Find last key as marker
            last_key_start = listing.rfind("<Key>") + len("<Key>")
            last_key_end = listing.rfind("</Key")
            last_key = listing[last_key_start:last_key_end]
            # Substract last key (will be repeated as 1st key of next chunk)
            nr_of_keys += (config.list_chunks - 1)

        else:
            # Count number of keys in last chunk
            nr_of_keys += listing.count("<Key>")

    return nr_of_keys, duration

def start_streams(action):
    """ Start parallel streams for action """
    mark = datetime.datetime.now()
    print action+" START %s" % time.mktime(mark.timetuple()), mark
    print "Run ID: %d" % config.pid

    start_time = time.mktime(mark.timetuple())
    errors = False
    workers = []
    current_host_idx = 0
    q = multiprocessing.Queue()
 
    # spawn streams 
    for seqid in range(config.num_series):
        host = global_hosts_list[current_host_idx] 
        worker = multiprocessing.Process(target=do_action, args=(host, action, seqid, q,)) 
        workers.append(worker)
        worker.start()

        if current_host_idx + 1 > len(global_hosts_list) - 1:
            current_host_idx = 0
        else:
            current_host_idx += 1   

    latencies = {}
    # wait for completion and get exit codes    
    for worker in workers:
        worker.join()
        if worker.exitcode != 0:
            errors = True
        else:
            l = q.get()
            for k,v in l.items():
                if not k in latencies:
                    latencies[k] = 0
                latencies[k] += v
                
    msg = 'Latencies:\n'
    for k in sorted(latencies.keys()):
        msg += '%d-%d: %d\n' % (2**k, 2**(k+1), latencies[k])
    print msg,

    if not errors:
        print_statistics(action, 
                         config.name, 
                         time.time() - start_time, 
                         config.num_series, 
                         config.num_files_per_series, 
                         config.file_size_kib)
    else:
        print "Errors detected during test. Exiting...."
        exit(1)

    mark = datetime.datetime.now()
    print action+" END %s" % time.mktime(mark.timetuple()), mark

def runOn(hostname):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
    ssh.connect( hostname, username=config.ssh_username, password=config.ssh_password )
    stdin, stdout, stderr = ssh.exec_command("python /tmp/"+str.join(' ', sys.argv))
    result = stdout.readlines()
    print hostname, str.join(' ', result)

def print_get_command():    
    print " *** To re-run the GET command, cut and paste the following. ***"
    command = "./"+os.path.basename(__file__)+" --tests=GET --protocol="+config.protocol
    if (config.hosts == None):
        command += " --hosts-file=" + config.hosts_file 
    else:
        command += " --hosts=" + config.hosts
    command += " --id="+str(config.pid)+" --file-size-kib="+str(config.file_size_kib)\
    +" --num-series="+str(config.num_series)+" --num-files-per-series="+str(config.num_files_per_series)\
    +" --read-size-kib="+str(config.read_size_kib)
    
    if config.random_read_offset_min and config.random_read_offset_max:
        command += " --random-read-offset-min="+str(config.random_read_offset_min)
        command += " --random-read-offset-max="+str(config.random_read_offset_max)
    
    if config.use_ssl:
        command +=" --use-ssl"
        
    if config.protocol == "S3":
        command += " --access-key="+str(config.access_key)+" --secret-key="+str(config.secret_key)\
        +" --bucket="+str(config.name)
    elif config.protocol == "AXR":
        command += " --namespace="+str(config.name)
        if config.allow_all_strategies:
            command += " --allow-all-strategies"
    else:
        print "What are you doing?"
        print "Protocol "+config.protocol+" doesn't exist!"
        sys.exit(1)
    
    print command
    #print
    #print config 

def main():
    """ Main function """
    # open connection
    client = open_connection(global_hosts_list[0])

    if config.protocol == "AXR":
        print """
  AXR protocol requires that you have executed:

  /opt/qbase3/qshell -c "q.dss.manage.setPermissions('/manage', 'everyone', ['UPDATE', 'CREATE', 'READ', 'LIST', 'DELETE'])"
    
  Warning: this should not be run on a production system or any system where security is important.        
        """
        # check if AXR namespace or policy exists
        config.namespace_name, config.policy = client.do_configure_dss_ns()
        
    elif config.protocol == "S3":
        # check if S3 bucket exists
        try:
            client.head('')
        except Exception, ex:
            print "Failed to check bucket %s at %s" % (config.name, global_hosts_list[0])
            exit(1)
            
    del client
        
    if "PUT" in config.tests:
        # """ PUT files """
        create_perf_file(PERF_FILE, config.file_size_kib, config.use_sparse, config.use_random)
        print " *** Storing %s files of %s KiB in %s parallel streams ***" % (  
             config.num_series * config.num_files_per_series, 
             config.file_size_kib, 
             config.num_series)
        start_streams("PUT")

    if "GET" in config.tests:
        # """ GET files """
        print " *** Retrieving %s files of %s KiB in %s parallel streams ***" % (
            config.num_series * config.num_files_per_series, 
            config.file_size_kib, 
            config.num_series)
        start_streams("GET")

    if "LIST" in config.tests:
        # """ LIST files """
        print " *** Listing files for bucket '%s'" % config.name
        print "LIST START: %s" % datetime.datetime.now()
        print "LIST BUCKET: %s" % config.name

        nr_of_keys, duration = do_listing(global_hosts_list[0])

        print "LIST NUMBER OF KEYS: %s" % nr_of_keys
        print "LIST DURATION: %s seconds" % duration
        print "LIST FILES/SEC: %s " % (nr_of_keys / duration)
        print "LIST END: %s" % datetime.datetime.now()

    if "DELETE" in config.tests:
        # """ DELETE files """
        print " *** Deleting %s files of %s KiB in %s parallel streams ***" % (
            config.num_series * config.num_files_per_series, 
            config.file_size_kib, 
            config.num_series)
        print " * deletion of files on storage occurs async from this command * "
        start_streams("DELETE")

    if not "DELETE" in config.tests:
        print_get_command()
    
# Get configuration arguments
config = parse_arguments()
if len(config.run_on) == 0:
    main() # run on localhost
else:
    import paramiko
    
    # clean out our ssh commands
    arguments = sys.argv[:]
    for arg in arguments:
        if ("run-on" in arg) or ("username" in arg) or ("password" in arg):
            sys.argv.remove(arg)
    
    # copy our files to the remote machines
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
    hostnames = config.run_on.split(',')
    for hostname in hostnames:
        ssh.connect(hostname, username=config.ssh_username, password=config.ssh_password)
        ftp = ssh.open_sftp()
        ftp.put(__file__, '/tmp/'+os.path.basename(__file__))
        ftp.close()
        ssh.close()
    
    # we run our tests in parallel 
    from multiprocessing import Pool
    pool = Pool( len( hostnames ) )
    pool.map(runOn, hostnames, 1)
    pool.close()
    pool.join()    

