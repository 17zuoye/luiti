#!/usr/bin/env bash

cd tests/zip_package_by_luiti; python setup.py bdist_egg 2&>1; cd -;



for file1 in test_main.py test_utils.py test_task.py test_luiti_ext.py test_mr_test_case.py test_manager.py
do
  echo "[test] $file1"
  python tests/$file1
done

rm -rf build dist luiti.egg-info # clean tmp files
