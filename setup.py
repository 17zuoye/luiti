#-*-coding:utf-8-*-


from setuptools import setup

setup(
    name='luiti',
    version='0.1.2',
    url='http://github.com/17zuoye/luiti/',
    license='MIT',
    author='David Chen',
    author_email=''.join(reversed("moc.liamg@emojvm")),
    description='Luiti = Luigi + time',
    long_description=open("README.markdown").read(),
    packages=[
                'luiti',
                'luiti/manager',
                'luiti/task_templates/',
                'luiti/task_templates/time',
                'luiti/task_templates/other',
                'luiti/utils',
             ],
    scripts=[
        'bin/luiti',
    ],
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=[
        "luigi         == 1.1.2",
        "snakebite     == 2.5.2",

        "arrow         == 0.4.4",
        "inflector     == 2.0.11",
        "protobuf      == 2.6.1",
        "tornado       == 4.0.2",
        "mechanize     == 0.2.5",

        "python-daemon == 1.6.1",
        "MySQL-python  >= 1.2.5",
        "pymongo       >= 2.7.2",

        "etl_utils     == 0.1.9",
        "tabulate      == 0.7.3",
        "pipetools     == 0.2.7",
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
