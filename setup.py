import os
from setuptools import setup, Extension, find_packages
from Cython.Build import cythonize
import numpy
from pathlib import Path

root_dir = Path('.')
package_dir = Path('.')

def find_pyx(directory):
    pyx_s = []
    with os.scandir(directory) as it:
        for entry in it:
            if entry.is_dir():
                pyx_s.extend(find_pyx(entry))
            
            if entry.is_file() and os.path.splitext(entry)[1] == '.pyx' and 'checkpoint' not in entry.name:
                pyx_s.append(entry)
                
    return pyx_s

def make_Extension(entry):
    #name = os.path.splitext(entry.name)[0]
    path = entry.path
    name = os.path.splitext(Path(path).relative_to(root_dir))[0].replace(os.sep, '.')
    return Extension(
        name,
        [path],
        include_dirs = [numpy.get_include(), '.'],   # adding the '.' to include_dirs is CRUCIAL!!
        extra_compile_args = ['-O3', '-Wall'],
        extra_link_args = ['-g', '-std=c11'],
        libraries = [],
        )

def cython_extensions(directory='.'):
    ext_names = find_pyx(directory)
    extensions = list(map(make_Extension, ext_names))
    return extensions

def readme():
    with open('README.rst', 'r') as f:
        return f.read()

setup(
    name='pyutils',
    version='0.1',
    description='A python library that contains some utilities functions',
    long_description=readme(),
    url='',
    author='Percy',
    author_email='',
    license='',
    packages=find_packages(),
    ext_modules=cythonize(cython_extensions(package_dir)),
    include_dirs=[numpy.get_include()],
    install_requires=['numpy'],
    zip_safe=False,
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
)