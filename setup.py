from setuptools import setup, find_packages

setup(
    name="obs-data-exporter",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'openai>=1.0.0',
        'click>=8.0.0',
        'pyyaml>=6.0.0',
        'tqdm>=4.65.0',
        'pathlib>=1.0.1',
    ],
    entry_points={
        'console_scripts': [
            'obs-data-exporter=obs_data_exporter.cli:translate',
        ],
    },
    author="Guance Cloud",
    author_email="guance@guance.com",
    description="A tool for exporting data from OBS to a file",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/obs-data-exporter",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
) 