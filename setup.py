from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
    name="enstore2cta",
    version="0.1.0",
    description="Enstore to CTA migration script",
    long_description=readme,
    author="Dmitry Litvintsev",
    url="https://github.com/DmitryLitvintsev/enstore2cta",
    license=license,
    #packages=["enstore2cta",],
    install_requires = ["psycopg2",],
    scripts=["enstore2cta/scripts/enstore2cta.py",],
    )
