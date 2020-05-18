import setuptools

setuptools.setup(
    name="mothball_pipeline",
    version="0.0.1",
    author="Jack Cushman",
    author_email="jcushman@law.harvard.edu",
    packages=['mothball_pipeline'],
    install_requires=[
        "boto3",
        "smart-open>=1.10.0",
        "tqdm",
    ],
    tests_require=[
        "pytest",
        "moto",
    ],
    setup_requires=[
        'pytest-runner',
    ],
    entry_points = {
        'console_scripts': [
            'mothball_pipeline=mothball_pipeline.mothball_pipeline:main',
        ],
    }
)
