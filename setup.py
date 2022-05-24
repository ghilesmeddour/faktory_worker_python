import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyfaktory",
    version="0.2.0",
    author="Ghiles Meddour",
    author_email="ghiles.meddour@munic.io",
    description="Faktory Client Python (Producer and Consumer/Worker)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ghilesmeddour/faktory_worker_python",
    project_urls={
        "Bug Tracker":
        "https://github.com/ghilesmeddour/faktory_worker_python/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=["pebble", "rfc3339-validator"],
    extras_require={
        "sentry": ["sentry-sdk"],
    },
)
