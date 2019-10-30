from setuptools import find_namespace_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

name = "redivis-pandas-gbq"
description = "Redivis authentication wrapper around pandas-gbq"
version = "0.0.1"
# Should be one of:
# 'Development Status :: 3 - Alpha'
# 'Development Status :: 4 - Beta'
# 'Development Status :: 5 - Production/Stable'
release_status = "Development Status :: 3 - Alpha"
dependencies = [
    "pandas-gbq == 0.11.0",
    "redivis-bigquery @ git+https://github.com/redivis/redipy.git#subdirectory=bigquery"
 ]

setup(
    name=name,
    version=version,
    author="Redivis Inc.",
    author_email="support@redivis.com",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/redivis/redipy/tree/master/pandas-gbq",
    packages=find_namespace_packages(),
    # dependency_links=["git+https://github.com/redivis/redipy.git#egg=redivis-bigquery-0.0.1&subdirectory=bigquery"]
    install_requires=dependencies,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    zip_safe=False
)
