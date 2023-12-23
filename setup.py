from pathlib import Path

from setuptools import setup, find_packages

HERE = Path(__file__).parent


def read_version() -> str:
    version_file = HERE / "dbt_parser" / "version.py"
    ns = {}
    exec(version_file.read_text(encoding="utf-8"), ns)
    return ns["VERSION"]


long_description = (HERE / "README.md").read_text(encoding="utf-8")


setup(
    name="dbt-date-harvester",
    version=read_version(),
    description="Ferramenta de parsing e analise estatica para projetos dbt",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="dbt-date-harvester contributors",
    license="GPL-3.0",
    packages=find_packages(),
    install_requires=[
        "pyyaml>=6.0",
        "networkx>=2.8",
        "textual>=0.47.0",
        "google-cloud-bigquery>=3.11.0",
        "python-dotenv>=1.0.0",
        "openpyxl>=3.1.0",
    ],
    extras_require={
        "gpu": [
            "cudf-cu12>=24.0",
        ],
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "pytest-mock>=3.10",
            "hypothesis>=6.0",
            "mypy>=0.990",
            "flake8>=6.0",
            "black>=22.0",
            "isort>=5.10",
            "pre-commit>=2.20",
        ],
    },
    entry_points={
        "console_scripts": [
            "dbt-parser=dbt_parser.cli:main",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)
