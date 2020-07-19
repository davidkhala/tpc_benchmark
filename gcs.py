"""Google Cloud Storage upload methods

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import os
import glob
import subprocess

import pandas as pd

import config

class GSUtilUpload:
    def __init__(self, test, scale, bucket_name):
        self.test = test
        self.scale = scale

        self.data_test = {"h":config.fp_h_output, "ds":config.fp_ds_output}[self.test]
        self.data_test_scale = self.data_test + config.sep + str(self.scale) + "GB"
        
        self.bucket_name = bucket_name
        
    def parse_std_err(self, std_err):
        x = std_err[-200:-1]
        x = std_err[-200:]
        y = [_y.strip("\\").strip("/").strip() for _y in x.split("\r")]
        return y[-3], y[-1]
    
    def rename_data(self):

        data = config.fp_base_output
        prefix = self.test + "_" + str(self.scale) + "GB_"

        fp_data_test_scale = glob.glob(self.data_test_scale + config.sep + "*")
        a = [os.path.basename(x) for x in fp_data_test_scale]

        b = []
        for aa in a:
            if aa[:len(prefix)] == prefix:
                continue
            b.append(aa)
            
        if len(b) == 0:
            return False

        for f in b:
            os.rename(self.data_test_scale + config.sep + f,
                      self.data_test_scale + config.sep + prefix + f)
        return True
        
    def upload(self, verbose=False):
        cmd = ["gsutil", "-m", "cp", "*", "gs://" + self.bucket_name]
        
        t0 = pd.Timestamp.now()
        
        pipe = subprocess.run(cmd,
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE, 
                          cwd=self.data_test_scale)
        std_out = pipe.stdout.decode("utf-8")
        std_err = pipe.stderr.decode("utf-8")
        
        t1 = pd.Timestamp.now()
        
        if verbose:
            s1, s2 = self.parse_std_err(std_err)
            print(s1)
            print(s2)

        csv_fp = (self.data_test + config.sep + 
                  "gsutil_upload-" + self.test + "_" + str(self.scale) + 
                  "GB-" + str(pd.Timestamp.now()) + ".csv"
                  )

        with open(csv_fp, "w") as f:
            f.write("test,scale,t0,t1,m1,m2\n")
            d = ",".join([self.test, str(self.scale), str(t0), str(t1), s1, s2])
            f.write(d)
        
        return std_out, std_err