# -*- coding:utf8 -*-


__author__ = 'Mx'

import utils
import time


def test_get_path_files():
    print utils.get_path_files('.',patten='.*txt',type=3)
    print utils.get_path_files()


if __name__ == '__main__':
    #中文注释
    print 'start test...'; start = time.time()


    # test_get_path_files()
    print isinstance(['4','1dfa',12],[].__class__)


    print 'test done in ',time.time()-start,'s'
