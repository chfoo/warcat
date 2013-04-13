'''Version info'''

short_version = '2.1'
'''Short version in the form of N.N'''

__version__ = short_version + '.1'
'''Version in the form of N.N[.N]+[{a|b|c|rc}N[.N]+][.postN][.devN]'''


if __name__ == '__main__':
    print(__version__, end='')
