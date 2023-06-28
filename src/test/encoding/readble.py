#!/usr/bin/env python3

import os
import subprocess
import tempfile
import time
import filecmp
import glob
import sys
import difflib
from concurrent.futures import ThreadPoolExecutor

#os.environ["CEPH_LIB"] = "/usr/lib64/ceph/denc"
CEPH_ROOT = os.getenv("CEPH_ROOT", "..")
CEPH_DENCODER = os.path.join(CEPH_ROOT, "build/bin/ceph-dencoder")
DIR = os.path.join(CEPH_ROOT, "ceph-object-corpus")
MAX_PARALLEL_JOBS = os.cpu_count()
temp_unrec = tempfile.mktemp(prefix=f"unrecognized_")

def test_object(type, vdir, arversion, myversion):
    _numtests = 0
    _failed = 0

    if subprocess.call([CEPH_DENCODER, "type", type], stderr=subprocess.DEVNULL) == 0:
        print(f"        {vdir}/objects/{type}")

        incompat = ""
        incompat_paths = ""
        sawarversion = False
        for iv in sorted(os.listdir(os.path.join(DIR, "archive"))):
            if iv == arversion:
                sawarversion = True

            if sawarversion and os.path.exists(os.path.join(DIR, "archive", iv, "forward_incompat", type)):
                incompat = iv

                if os.path.isdir(os.path.join(DIR, "archive", iv, "forward_incompat", type)):
                    files = sorted(os.listdir(os.path.join(DIR, "archive", iv, "forward_incompat", type)))
                    if files:
                        incompat_paths = os.path.join(DIR, "archive", iv, "forward_incompat", type)
                    else:
                        print("type {} directory empty, ignoring whole type instead of single objects".format(type))
                break

        if incompat:
            if not incompat_paths:
                print("skipping incompat {} version {}, changed at {} < code {}".format(type, arversion, incompat, myversion))
                return (_numtests, _failed)
            else:
                print("postponed skip one of incompact {} version {}, changed at {} < code {}".format(type, arversion, incompat, myversion))

        for f in sorted(os.listdir(os.path.join(vdir, "objects", type))):
            skip = False

            if incompat_paths:
                for i_path in incompat_paths:
                    if os.path.islink(os.path.join(i_path, f)) and os.path.exists(os.readlink(os.path.join(i_path, f))):
                        print("skipping object {} of type {}".format(f, type))
                        skip = True
                        break

            if skip:
                continue
            cmd1 = [CEPH_DENCODER, "type", type, "import", os.path.join(vdir, "objects", type, f), "decode", "dump_json"]
            cmd2 = [CEPH_DENCODER, "type", type, "import", os.path.join(vdir, "objects", type, f), "decode", "encode", "decode", "dump_json"]

            output1 = ""
            output2 = ""
            try:
                output1 = subprocess.check_output(cmd1)
                output2 = subprocess.check_output(cmd2)
            except subprocess.CalledProcessError as e:
                print(f"Error encountered in subprocess. Command: {e.cmd}")
                print(f"Return code: {e.returncode}")
                sys.exit(e.returncode)

            if output1 != output2:
                temp_file = tempfile.mktemp(prefix=f"dencerr_{type}_")

                print(f"**** reencode of {vdir}/objects/{type}/{f} resulted in a different dump ****")
                diff_output = "\n".join(difflib.ndiff(output1.decode().splitlines(), output2.decode().splitlines()))

                with open(temp_file, "wb") as file_temp:
                    file_temp.write("**** reencode of {}/objects/{}/{} resulted in a different dump ****\n".format(vdir, type, f).encode()+b'\n')
                    file_temp.write("differs:\n".encode()+b'\n\n')
                    file_temp.write(diff_output.encode()+b'\n\n')

            if type == "MOSDOp":
                temp_file2 = tempfile.mktemp(prefix=f"dencOSDop_")
                with open(temp_file2, "wb") as file_temp2:
                    file_temp2.write("****  {}/objects/{}/{} resulted in a different dump ****\n".format(vdir, type, f).encode()+b'\n')
                    file_temp2.write("output1:\n".encode()+b'\n\n')
                    file_temp2.write(output1+b'\n\n')
                    file_temp2.write("ouput2:\n".encode()+b'\n\n')
                    file_temp2.write(output2+b'\n\n')
                    diff_output = "\n".join(difflib.ndiff(output1.decode().splitlines(), output2.decode().splitlines()))
                    file_temp2.write(diff_output.encode()+b'\n\n')

                _failed += 1
            _numtests += 1

    else:
        debug_print("skipping unrecognized type {}".format(type))
        with open(temp_unrec, "a") as file_unrec:
            file_unrec.write("{}\n".format(type))
        #sys.exit(1)
    return (_numtests, _failed)

def test_object_wrapper(args):
    return test_object(*args)

def debug_print(msg):
    if debug:
        print("DEBUG: {}".format(msg))

def main():
    failed = 0
    numtests = 0

    myversion = subprocess.check_output([CEPH_DENCODER, "version"]).decode().strip()
    debug_print("running excutor with {} threads".format(MAX_PARALLEL_JOBS))
    executor = ThreadPoolExecutor(max_workers=MAX_PARALLEL_JOBS)
    futures = []
    
    
    for arversion in sorted(os.listdir(os.path.join(DIR, "archive"))):
        vdir = os.path.join(DIR, "archive", arversion)

        if not os.path.isdir(vdir) or not os.path.isdir(os.path.join(vdir, "objects")):
           continue

        for type in sorted(os.listdir(os.path.join(vdir, "objects"))):
            args = (type, vdir, arversion, myversion)
            future = executor.submit(test_object_wrapper, args)
            futures.append(future)

    debug_print("number of futures: {}".format(len(futures)))
    executor.shutdown()
    for future in futures:
        try:
            numtests_type, failed_type = future.result()
            debug_print("future {} - numtests: {}, failed: {}".format(future, numtests_type, failed_type))
            numtests += numtests_type
            failed += failed_type
        except SystemExit as e:
            print("SystemExit exception occurred. Skipping this future.")
            debug_print("Exception details: {}".format(e))
            continue

    if failed > 0:
        print("FAILED {}/{} tests.".format(failed, numtests))

    if numtests == 0:
        print("FAILED: no tests found to run!")

    print("Passed {} tests.".format(numtests))

if __name__ == "__main__":
    debug = False
    main()
