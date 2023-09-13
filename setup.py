from distutils.core import setup

setup(
    name = 'simplesql',
    version = '0.0.4',
    description = 'SimpleSQL framework, high performance, easy to learn, fast to code, ready for production',
    author = 'Maximilian Quaeck',
    author_email = 'maximilian.quaeck@gmx.net',
    package_dir = {'simplesql': 'src'},
    install_requires = [
        'asyncpg >= 0.28.0',
    ]
)