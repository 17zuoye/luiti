#-*-coding:utf-8-*-

__all__ = ["luigi"]


import luigi
import luigi.hadoop
luigi.hadoop = luigi.hadoop
import luigi.hdfs
luigi.hdfs = luigi.hdfs

from ..hadoop_ext import HadoopExt
luigi.hadoop.HadoopExt = HadoopExt # write back
# NOTE 对 luigi.hadoop 兼容 "track the job: "


import os
from collections import defaultdict

from ..utils import IOUtils, TargetUtils
luigi.HDFS = TargetUtils.hdfs # 本来就是需要读取全局配置，所以索性就绑定在 luigi 命名空间了吧。


from .check_date_range     import check_date_range
from .check_runtime_range  import check_runtime_range
from .mr_local             import mr_local
from .multiple_text_files  import multiple_text_files
from .persist_files        import persist_files
from .plug_packages        import plug_packages
from .ref_tasks            import ref_tasks

luigi.persist_files        = persist_files
luigi.ref_tasks            = ref_tasks
luigi.check_date_range     = check_date_range
luigi.check_runtime_range  = check_runtime_range
luigi.mr_local             = mr_local
luigi.multiple_text_files  = multiple_text_files
luigi.plug_packages        = plug_packages



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
    tar = tarfile.open(filename, "a")

    logger = luigi.hadoop.logger
    fake_exists_path = "/" # root is awlays exists
    def get_parent_zip_file_within_absolute_path(path1):
        path2      = path1[:]
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
        if os.path.exists(path2):     continue # so luigi can import it.
        if not path2.startswith("/"): continue # we only care about libraries.

        is_success, zipfilename3 = get_parent_zip_file_within_absolute_path(path2)
        if is_success:
            tmp_dir3 = tempfile.mkdtemp()
            zipfile.ZipFile(zipfilename3).extractall(tmp_dir3)

            for root4, dirs4, files4 in os.walk(tmp_dir3):
                curr_dir5 = os.path.basename(root4)
                for file5 in files4:
                    if file5.endswith(".pyc"): continue
                    add(os.path.join(root4, file5), os.path.join(root4.replace(tmp_dir3, "").lstrip("/"), file5))

    client_cfg = os.path.join(os.getcwd(), "client.cfg")
    if os.path.exists(client_cfg):
        tar.add(client_cfg, "client.cfg")
    tar.close()
luigi.hadoop.create_packages_archive = create_packages_archive_with_support_egg


from ..manager import luiti_config, active_packages
luigi.ensure_active_packages = lambda : active_packages # make a wrap
luigi.luiti_config           = luiti_config
luiti_config.linked_luigi    = luigi
