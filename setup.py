import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = fh.read().replace(" ", "").split("\n")

setuptools.setup(
    name="ProtobufMessageFactory", # Replace with your own username
    version="0.1.1",
    author="cimera255",
    author_email="author@example.com",
    description="This package tries to ease up the work with protobuf messages in python.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cimera255/ProtobufMessageFactory",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)