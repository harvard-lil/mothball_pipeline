import setuptools

setuptools.setup(
    name="mothball_pipeline",
    version="0.0.1",
    author="Jack Cushman",
    author_email="jcushman@law.harvard.edu",
    packages=setuptools.find_packages(),
    install_requires=[
        "boto3",
        "smart-open>=1.10.0",
        "tqdm",
        "s3mothball",
    ],
    tests_require=[
        "pytest",
        "moto",
    ],
    setup_requires=[
        'pytest-runner',
    ],
    dependency_links=[
        'https://github.com/harvard-lil/s3mothball/archive/master.zip#egg=s3mothball',
    ],
    entry_points = {
        'console_scripts': [
            'mothball_pipeline=mothball_pipeline:main',
        ],
    }
)