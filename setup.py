from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="mc_automation_tools",
    author="MC",
    description="Map colonies automation infrastructure kit tools for mutual purpose",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MapColonies/automation-kit.git",
    packages=find_packages(),
    install_requires=requirements,
    version="1.2.63",  # Set the version to the desired tag
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)