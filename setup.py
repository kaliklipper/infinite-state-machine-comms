import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="python-state-machine-ism_comms-kaliklipper",
    version="0.1.0",
    author="kaliklipper",
    author_email="kaliklipper@gmail.com",
    description="IO action packs for the Python State Machine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kaliklipper/infinite-state-machine-comms",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
