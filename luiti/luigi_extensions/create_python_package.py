# -*-coding:utf-8-*-

__all__ = ["create_packages_archive_with_support_egg"]

import os
from .luigi_root_context import luigi

orig_create_packages_archive = luigi.hadoop.create_packages_archive


def create_packages_archive_with_support_egg(packages, filename):
    """
    Fix original luigi's `create_packages_archive` cannt attach egg packages
    (zip file type) to tarfile, Cause it's coping file mechanism by absolute
    path.
    """
    # 1. original create tar file
    orig_create_packages_archive(packages, filename)

    # 2. append python egg packages that 1. not covered
    import tarfile
    tar = tarfile.open(filename, "a")  # Force append

    logger = luigi.hadoop.logger
    fake_exists_path = "/"  # root is awlays exists

    def get_parent_zip_file_within_absolute_path(path1):
        path2 = path1[:]
        is_success = False
        while path2 != fake_exists_path:
            path2 = os.path.dirname(path2)
            if os.path.isfile(path2):
                is_success = True
                break
        return is_success, path2

    def add(src, dst):
        logger.debug('adding to tar: %s -> %s', src, dst)
        tar.add(src, dst)

    import zipfile
    import tempfile
    for package1 in packages:
        path2 = (getattr(package1, "__path__", []) + [fake_exists_path])[0]
        if os.path.exists(path2):
            continue  # so luigi can import it.
        if not path2.startswith("/"):
            continue  # we only care about libraries.

        is_success, zipfilename3 = \
            get_parent_zip_file_within_absolute_path(path2)
        if is_success:
            tmp_dir3 = tempfile.mkdtemp()
            zipfile.ZipFile(zipfilename3).extractall(tmp_dir3)

            for root4, dirs4, files4 in os.walk(tmp_dir3):
                for file5 in files4:
                    if file5.endswith(".pyc"):
                        continue
                    add(
                        os.path.join(root4, file5),
                        os.path.join(
                            root4.replace(tmp_dir3, "").lstrip("/"), file5))

    client_cfg = os.path.join(os.getcwd(), "client.cfg")
    if os.path.exists(client_cfg):
        tar.add(client_cfg, "client.cfg")
    tar.close()

luigi.hadoop.create_packages_archive = create_packages_archive_with_support_egg  # wrap old function
