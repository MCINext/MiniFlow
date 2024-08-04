from setuptools import setup, find_packages

def read_requirements(filename):
    with open(filename, 'r') as f:
        return f.read().splitlines()
setup(
    name='miniflow',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=read_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            'run_etl=examples.main:main',
        ],
    },
    author='Your Name',
    author_email='your.email@example.com',
    description='A simple, lightweight, and extendable ETL framework',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/miniflow',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
