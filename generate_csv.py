#!/usr/bin/python

import os
import numpy as np
import csv
import json
import argparse

def parse_smallfile(base_dir, result_dir):
    result_fmt = {
            "smallfile-dir-ops.csv" : [ 'create', 'delete', 'rename', 'mkdir', 'rmdir', 'symlink', 'delete-renamed', 'readdir', 'ls-l' ],
            "smallfile-file-ops.csv" : [ 'append', 'overwrite', 'setxattr', 'chmod', 'read', 'stat' ]
    }
    smallfile_dir = os.path.join(base_dir, result_dir, 'smallfile')
    for csv_file in result_fmt:
        res_csv = result_dir
        for ops in result_fmt[csv_file]:
            with open("%s/%s" % (smallfile_dir, ops)) as f:
                data = json.load(f)
		print smallfile_dir + ops
                res_csv = res_csv + "," + str(data['results']['files-per-sec'])
        with open("%s/%s" % (base_dir, csv_file), "a+") as f:
            f.write(res_csv + '\n')

def parse_fio(base_dir, result_dir):
    result_fmt = {
            "read-write.csv" : [ 'SeqRead', 'RandomRead', 'SeqWrite', 'RandomWrite' ]
    }

    fio_dir = os.path.join(base_dir, result_dir, 'fio')
    for csv_file in result_fmt:
        res_csv = result_dir
        for res in result_fmt[csv_file]:
            with open("%s/%s.json" % (fio_dir, res)) as f:
                data = json.load(f)
                res_csv = res_csv + "," + data[res]['iops']
        with open("%s/%s" % (base_dir/csv_file), "a+") as f:
            f.write(res_csv + '\n')

def parse_pgbench(base_dir, result_dir):
    result_fmt = {
            "pgbench-scale.csv" : ["scale.json"],
            "pgbench-transactions.csv" : ["transactions.json"]
    }
    pgbench_dir = os.path.join(base_dir, result_dir, 'pgbench')
    for csv_file in result_fmt:
        res_csv = result_dir
        for res in result_fmt[csv_file]:
            with open("%s/%s" % (pgbench_dir, res)) as f:
                data = json.load(f)
                if res == "scale.json":
                    res_csv = res_csv + "," + data["time"]
                else:
                    res_csv = res_csv + "," + data["tps (including connections establishing)"]
        with open("%s/%s" % (base_dir/csv_file), "a+") as f:
            f.write(res_csv + '\n')

def parse_untar(base_dir, result_dir):
    result_fmt = {
            "untar.csv" : ["untar.json", "untar-rm-rf.json"]
    }
    untar_dir = os.path.join(base_dir, result_dir, 'untar')
    for csv_file in result_fmt:
        res_csv = result_dir
        for res in result_fmt[csv_file]:
            with open("%s/%s" % (pgbench_dir, res)) as f:
                data = json.load(f)
                res_csv = res_csv + "," + data["time"]
        with open("%s/%s" % (base_dir/csv_file), "a+") as f:
            f.write(res_csv + '\n')

def parse_result_dir_add_csv(base_dir, result_dir):
    parse_smallfile(base_dir, result_dir)
    parse_fio(base_dir, result_dir)
    parse_pgbench(base_dir, result_dir)
    parse_untar(base_dir, result_dir)

def main(base_dir, result_dir):
    #parse_base_dir() #optional only if all
    parse_result_dir_add_csv(base_dir, result_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "base_dir",
            help="The directory with the results from glusterfs-perf that will be parsed by the tool"
    )
    parser.add_argument(
            "result_dir",
            help="The directory with the results from glusterfs-perf that will be parsed by the tool"
    )
    args = parser.parse_args()
    main(
        base_dir=args.base_dir,
        result_dir=args.result_dir
    )
