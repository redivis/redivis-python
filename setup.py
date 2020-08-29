from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

name = "redipy"
description = "Redivis client library"
version = "0.0.1"
# Should be one of:
# 'Development Status :: 3 - Alpha'
# 'Development Status :: 4 - Beta'
# 'Development Status :: 5 - Production/Stable'
release_status = "Development Status :: 3 - Alpha"
dependencies = [
    "google-cloud-bigquery == 1.25.0"
]

setup(
    name=name,
    version=version,
    author="Redivis Inc.",
    author_email="support@redivis.com",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/redivis/redipy.git",
    # packages=find_namespace_packages(),
    install_requires=dependencies,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    zip_safe=False
)
