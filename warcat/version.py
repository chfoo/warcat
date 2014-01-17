'''Version info'''

short_version = '2.2'
'''Short version in the form of N.N'''

__version__ = short_version + '.2'
'''Version in the form of N.N[.N]+[{a|b|c|rc}N[.N]+][.postN][.devN]'''


__all__ = ['short_version', '__version__']


if __name__ == '__main__':
    print(__version__, end='')
