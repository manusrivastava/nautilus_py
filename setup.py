from setuptools import setup

setup(name='nautilus',
        version='0.1',
        description='Kafka producer base',
        author='Varun Dhussa',
        packages=['nautilus'],
        install_requires=['kafka-python',]
        )
