import re
import os
import mlflow
import urllib
import json
from onesaitplatform.files import FileManager
from mlflow.store.artifact.artifact_repo import ArtifactRepository
from mlflow.entities import FileInfo

CONFIG_PATH = '/home/benjamin/repositories/mlflow_artifact_file_repository/config.json'

class OnesaitPlatformArtifactRepository(ArtifactRepository):
    """OnesaitPlatformArtifactRepository provided through plugin system"""
    """Stores artifacts as files in a local directory."""

    def __init__(self, *args, **kwargs):
        super(OnesaitPlatformArtifactRepository, self).__init__(*args, **kwargs)

        osp_host = None
        osp_token = None
        with open(CONFIG_PATH, 'r') as filehandle:
            content = filehandle.read()
            json_content = json.loads(content)
            osp_token = json_content['token']
            osp_host = json_content['host']
        osp_file_manager = FileManager(
            host=osp_host, user_token="Bearer {}".format(osp_token)
        )
        osp_file_manager.protocol = "https"
        osp_file_manager.avoid_ssl_certificate = True
        self.osp_file_manager = osp_file_manager

    def log_artifact(self, local_file, artifact_path=None):
        print('---------------------------')
        print('-- ESTOY EN LOG_ARTIFACT --')
        print('---------------------------')

    def _is_directory(self, artifact_path):
        uri = self.artifact_uri
        match = re.match('onesait-platform:[/]{2}([^/]+)[/]([0-9]+)[/]([^/]+).*', uri)
        experiment_id = match.group(2)
        run_id = match.group(3)
        runs = mlflow.search_runs([experiment_id])
        runs = runs[runs['run_id'] == run_id]
        artifacts = json.loads(runs['params.__artifacts'].tolist()[0])
        for artifact in artifacts:
            if artifact[0] == artifact_path:
                return False
        return True

    def log_artifacts(self, local_dir, artifact_path=None):
        print('----------------------------')
        print('-- ESTOY EN LOG_ARTIFACTS --')
        print(artifact_path)
        print('----------------------------')
        rootlen = len(local_dir) + 1
        artifacts = []
        for root, dirs, files in os.walk(local_dir):
            for _file in files:
                abs_path = os.path.join(root, _file)
                rel_path = abs_path[rootlen:]
                if artifact_path:
                    rel_path = artifact_path + '/' + rel_path
                print(rel_path)
                try:
                    uploaded, info = self.osp_file_manager.upload_file(
                        os.path.basename(_file), abs_path
                    )
                    artifacts.append([rel_path, info['id']])
                except:
                    print('Unable to upload {}'.format(_file))
        mlflow.log_param('__artifacts', json.dumps(artifacts))

    def list_artifacts(self, path=None):
        # NOTE: The path is expected to be in posix format.
        # Posix paths work fine on windows but just in case we normalize it here.

        print('~~~~~ ' + str(path) + ' ~~~~~')

        if path is not None:
            path = re.sub('^[/](.+)', '\g<1>', path)

        uri = self.artifact_uri
        match = re.match('onesait-platform:[/]{2}([^/]+)[/]([0-9]+)[/]([^/]+).*', uri)
        experiment_id = match.group(2)
        run_id = match.group(3)
        runs = mlflow.search_runs([experiment_id])
        runs = runs[runs['run_id'] == run_id]
        artifacts = json.loads(runs['params.__artifacts'].tolist()[0])

        paths = []
        infos = []
        for artifact in artifacts:
            artifact_path = artifact[0]

            if path and not artifact_path.startswith(path):
                continue

            path_len = 0 if path is None else len(path) + 1
            reduced_artifact_path = artifact_path[path_len:]

            steps = reduced_artifact_path.split('/')
            next_step = steps[0]
            if next_step not in paths:
                paths.append(next_step)
                p = next_step if path is None else '/'.join([path, next_step])
                print('#### ' + str(p) + ' ####')
                if len(steps) == 1:
                    infos.append(FileInfo(p, False, 80))
                else:
                    infos.append(FileInfo(p, True, None))
        return infos


    def _download_file(self, remote_file_path, local_path):
        uri = self.artifact_uri
        match = re.match('onesait-platform:[/]{2}([^/]+)[/]([0-9]+)[/]([^/]+).*', uri)
        experiment_id = match.group(2)
        run_id = match.group(3)
        runs = mlflow.search_runs([experiment_id])
        runs = runs[runs['run_id'] == run_id]
        artifacts = json.loads(runs['params.__artifacts'].tolist()[0])
        for artifact in artifacts:
            if artifact[0] == remote_file_path:
                doc_id = artifact[1]
                downloaded, info = self.osp_file_manager.download_file(
                    doc_id, filepath=os.path.dirname(local_path)
                )
