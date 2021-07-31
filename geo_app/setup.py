import setuptools
from distutils.core import setup
from distutils.util import convert_path
from fnmatch import fnmatchcase
import os

NAME = 'geoutls'

def find_packages(where='.', exclude=()):
    out = []
    stack = [(convert_path(where), '')]

    while stack:
        where, prefix = stack.pop(0)
       
        for name in os.listdir(where):
            fn = os.path.join(where, name)
            
            if ('.' not in name and os.path.isdir(fn) and os.path.isfile(os.path.join(fn, '__init__.py'))):
                out.append(prefix + name)
                stack.append((fn, prefix + name + '.'))
    
    for pat in list(exclude):
        out = [item for item in out if not fnmatchcase(item, pat)]
    
    return out


def do_setup():
    setup(name=NAME,
          author="Olga",
          version='1.1',
          packages=find_packages(where=os.path.dirname(os.path.abspath(__file__))),
          install_requires=[])

# use pip install -e . for editable mode
# pip uninstall geo-utls
if __name__ == "__main__":
    do_setup()

