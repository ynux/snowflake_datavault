from setuptools import setup 
setup(
    name="MinimalDatavault",
    author="ynux",
    description="building a minimal raw vault from sample data, in snowflake or sqlite",
    version="0.1",
    packages=['minimal_datavault',],
    install_requires=["jinja2","snowflake-sqlalchemy",],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        "": ["*.ini.sample", "*.jinja2", "*.csv", "*.json"],
    },
)
