from setuptools import setup, find_packages

setup(
    name="financial-data-service",
    version="0.1.0",
    description="A simplified equity data pipeline for Snowflake",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "snowflake-connector-python>=3.5.0",
        "requests>=2.31.0",
        "pandas>=2.1.4",
        "python-dotenv>=1.0.0",
        "loguru>=0.7.2",
        "ratelimit>=2.2.1",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "flake8>=6.1.0",
            "mypy>=1.7.1",
        ]
    },
)