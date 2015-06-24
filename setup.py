# -*-coding:utf-8-*-


from setuptools import setup

setup(
    name='luiti',
    version='0.1.5',
    url='http://github.com/17zuoye/luiti/',
    license='MIT',
    author='David Chen',
    author_email=''.join(reversed("moc.liamg@emojvm")),
    description='Luiti = Luigi + time',
    long_description=open("README.markdown").read(),
    packages=[
                'luiti',
                'luiti/luigi_decorators',
                'luiti/luigi_extensions',
                'luiti/manager',
                'luiti/task_templates/',
                'luiti/task_templates/time',
                'luiti/task_templates/other',
                'luiti/daemon',
                'luiti/utils', ],
    scripts=[
        'bin/luiti',
    ],

    package_data={'luiti': [
        "luiti/java/*.java",

        "luiti/webui/*.html",
        "luiti/webui/assets/*/**",
        "luiti/webui/bower_components/*/*",
        "luiti/webui/bower_components/*/*/*",
        "luiti/webui/bower_components/*/*/*/*",
    ]},
    include_package_data=True,

    zip_safe=False,
    platforms='any',
    install_requires=[
        # 1. luigi related
        "luigi         == 1.1.2",
        "snakebite     ~= 2.5",
        "protobuf      ~= 2.6",
        "tornado       ~= 4.0",
        "mechanize     ~= 0.2",
        "python-daemon ~= 1.6",
        "MySQL-python  ~= 1.2",
        "pymongo       ~= 2.7",

        # 2. luiti self
        "etl_utils     ~= 0.1",
        "arrow         ~= 0.4",
        "inflector     ~= 2.0",
        "pygments      ~=2.0",
        "ujson",
        "jsonpickle",
        "six",
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
