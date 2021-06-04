import re
import os
import json
import mlflow
import logging

from mlflow.entities import FileInfo
from onesaitplatform.files import FileManager
from mlflow.store.artifact.artifact_repo import ArtifactRepository

ARTIFACTS_PARAM_KEY = '__artifacts__'

_logger = logging.getLogger(__name__)

class OnesaitPlatformArtifactRepository(ArtifactRepository):
    """OnesaitPlatformArtifactRepository provided through plugin system"""

    def __init__(self, *args, **kwargs):
        """Initialization of the object, given a config file for OSP deployment"""
        super(OnesaitPlatformArtifactRepository, self).__init__(*args, **kwargs)

        if 'OSP_CONFIG_PATH' not in os.environ:
            raise AttributeError('OSP_CONFIG_PATH env variable not setted')
        osp_config_path = os.environ['OSP_CONFIG_PATH']
        config = None
        try:
            config_fh = open(osp_config_path, 'r')
            config = json.loads(config_fh.read())
            config_fh.close()
        except ValueError:
            raise ValueError(
        'Unable to parse OSP config file {}'.format(
            osp_config_path
            ))
        except FileNotFoundError:
            raise  FileNotFoundError(
        'Unable to open OSP config file {}'.format(
            osp_config_path
            ))

        if not (isinstance(config, dict) and 'token' in config):
            raise AttributeError(
        'Attribute token not found in {}'.format(
            osp_config_path
            ))
        if not (isinstance(config, dict) and 'host' in config):
            raise AttributeError(
        'Attribute host not found in {}'.format(
            osp_config_path
            ))

        self.osp_file_manager = FileManager(
            host=config['host'], user_token="Bearer {}".format(config['token'])
            )
        self.osp_file_manager.protocol = "https"

    def parse_artifact_uri(self, artifact_uri):
        """Returns parameters from artifact uri"""
        match = re.match(
            'onesait-platform:[/]{2}([^/]+)[/]([0-9]+)[/]([^/]+)[/]artifacts(:?[/](.+))?',
            artifact_uri
            )
        if not match:
            raise ValueError('Unable to parse artifact uri {}'.format(artifact_uri))

        host = match.group(1)
        experiment_id = match.group(2)
        run_id = match.group(3)
        path = match.group(4)

        return {
            'host': host,
            'experiment_id': experiment_id,
            'run_id': run_id,
            'path': path
            }

    def get_artifacts_info(self, artifact_uri):
        """Returns run info according to tracking server"""
        info = self.parse_artifact_uri(artifact_uri)
        run_id = info['run_id']
        experiment_id = info['experiment_id']
        runs_in_experiment = mlflow.search_runs([experiment_id])
        run = runs_in_experiment[runs_in_experiment['run_id'] == run_id]

        if not len(run):
            raise AttributeError('No run found for this uri {}'.format(
            self.artifact_uri
            ))

        artifacts_info, artifact_index = [], 1
        while artifact_index:
            parameter_key = 'params.{}{}'.format(
                ARTIFACTS_PARAM_KEY, str(artifact_index)
                )
            if parameter_key in run.columns:
                artifact_info = run[parameter_key].tolist()[0]
                try:
                    artifact_info = json.loads(artifact_info)
                except ValueError:
                    raise ValueError('Unable to parse artifacts info: {}'.format(artifact_info))
                artifacts_info.append(artifact_info)
                artifact_index += 1
            else:
                artifact_index = None

        return artifacts_info

    def upload_artifact(self, local_path, artifact_name):
        """Upload artifact to OSP File Repository"""
        uploaded, info = self.osp_file_manager.upload_file(
            artifact_name, local_path
            )
        uploaded_artifact_id = None
        if not uploaded:
            file_manager_info = self.osp_file_manager.to_json()
            raise ConnectionError(
        "Not possible to upload artifact with file manager: {}".format(
            file_manager_info
            ))
        uploaded_artifact_id = info['id']
        _logger.info('Uploaded artifact: {}'.format(info))
        return uploaded_artifact_id

    def download_artifact(self, local_path, artifact_id):
        """Downloads artifact from OSP file repository"""
        downloaded, info = self.osp_file_manager.download_file(
            artifact_id, filepath=os.path.dirname(local_path)
            )
        if not downloaded:
            file_manager_info = self.osp_file_manager.to_json()
            raise ConnectionError(
        "Not possible to download artifact with file manager: {}".format(
            file_manager_info))
        _logger.info('Downloaded artifact: {}'.format(info))

    def _is_directory(self, artifact_path):
        """Checks if a path is a directory"""
        artifacts_info = self.get_artifacts_info(
            self.artifact_uri
            )
        for artifact_info in artifacts_info:
            if artifact_info[0] == artifact_path:
                return False
        return True

    def log_artifacts(self, local_dir, artifact_path=None):
        """Saves artifacts in OSP repository"""
        artifact_counter = 0
        for root, _, files in os.walk(local_dir):
            for _file in files:
                local_path = os.path.join(root, _file)
                file_size = os.path.getsize(local_path)
                remote_path = local_path[len(local_dir) + 1:]
                if artifact_path:
                    remote_path = '/'.join([artifact_path, remote_path])
                uploaded_artifact_id = self.upload_artifact(local_path, _file)
                artifact_counter += 1
                parameter_key = ARTIFACTS_PARAM_KEY + str(artifact_counter)
                parameter_value = json.dumps([remote_path, uploaded_artifact_id, file_size])
                mlflow.log_param(parameter_key, parameter_value)

    def list_artifacts(self, path=None):
        """Returns saved artifacts for current artifact uri"""

        artifacts_info = self.get_artifacts_info(self.artifact_uri)
        artifacts_under_path_info = None
        if path:
            artifacts_under_path_info = list(filter(
                lambda a: a[0].startswith(path), artifacts_info
                ))
        else:
            artifacts_under_path_info = artifacts_info

        already_seen_paths, file_infos = [], []
        path_len = 0 if path is None else len(path) + 1

        for artifact_under_path in artifacts_under_path_info:
            file_size = artifact_under_path[2]
            relative_path = artifact_under_path[0][path_len:]
            relative_path_steps = relative_path.split('/')
            next_step = relative_path_steps[0]
            if next_step in already_seen_paths:
                continue
            already_seen_paths.append(next_step)
            file_info_path = next_step if path is None else '/'.join([path, next_step])
            if len(relative_path_steps) == 1:
                file_infos.append(FileInfo(file_info_path, False, file_size))
            else:
                file_infos.append(FileInfo(file_info_path, True, None))

        return file_infos

    def _download_file(self, remote_file_path, local_path):
        """Downloads artifact from OSP file ropository"""
        artifacts_info = self.get_artifacts_info(
            self.artifact_uri
            )
        artifacts_info = list(filter(lambda a: a[0] == remote_file_path, artifacts_info))
        if len(artifacts_info) < 1:
            raise AttributeError('Not available artifact to download {}'.format(remote_file_path))
        elif len(artifacts_info) > 1:
            raise AttributeError('Ambiguous artefact to download {}'.format(remote_file_path))

        artifact_info = artifacts_info[0]
        osp_artifact_id = artifact_info[1]
        self.download_artifact(local_path, osp_artifact_id)