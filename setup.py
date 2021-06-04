from setuptools import setup
import sys
sys.path.insert(0, './onesaitplatformplugins')

setup(
    name="mlflow-onesait-platform-plugin",
    version="0.2.6",
    description="Plugin for MLflow and Onesait Platform",
    packages=['onesaitplatformplugins'],
    install_requires=["mlflow", "onesaitplatform-client-services"],
    entry_points={
        # Define a ArtifactRepository plugin for artifact URIs with scheme 'onesait-platform'
        "mlflow.artifact_repository": "onesait-platform=onesaitplatformplugins.plugins:OnesaitPlatformArtifactRepository"
    },
)
