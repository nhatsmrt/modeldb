# -*- coding: utf-8 -*-

from __future__ import print_function

from .._tracking.entity import _ModelDBEntity

from .._protos.public.registry import RegistryService_pb2 as _ModelVersionService
from .._protos.public.common import CommonService_pb2 as _CommonCommonService

from .._internal_utils import _utils, _artifact_utils

from ..external import six
import requests
import time


class RegisteredModelVersion(_ModelDBEntity):
    def __init__(self, conn, conf, msg):
        super(RegisteredModelVersion, self).__init__(conn, conf, _ModelVersionService, "registered_model_version", msg)

    def __repr__(self):
        raise NotImplementedError

    @property
    def name(self):
        self._refresh_cache()
        return self._msg.version

    @classmethod
    def _generate_default_name(cls):
        return "ModelVersion {}".format(_utils.generate_default_name())

    @classmethod
    def _get_proto_by_id(cls, conn, id):
        Message = _ModelVersionService.GetModelVersionRequest
        endpoint = "/api/v1/registry/registered_model_versions/{}".format(id)
        response = conn.make_proto_request("GET", endpoint)

        return conn.maybe_proto_response(response, Message.Response).model_version

    @classmethod
    def _get_proto_by_name(cls, conn, name, registered_model_id):
        Message = _ModelVersionService.FindModelVersionRequest
        predicates = [
            _CommonCommonService.KeyValueQuery(key="version",
                                               value=_utils.python_to_val_proto(name),
                                               operator=_CommonCommonService.OperatorEnum.EQ)
        ]
        endpoint = "/api/v1/registry/{}/versions/find".format(registered_model_id)
        msg = Message(predicates=predicates)

        proto_response = conn.make_proto_request("POST", endpoint, body=msg)
        response = conn.maybe_proto_response(proto_response, Message.Response)

        if not response.model_versions:
            return None

        # should only have 1 entry here, as name/version is unique
        return response.model_versions[0]

    @classmethod
    def _create_proto_internal(cls, conn, ctx, name, desc=None, tags=None, labels=None, attrs=None, date_created=None):
        ModelVersionMessage = _ModelVersionService.ModelVersion
        SetModelVersionMessage = _ModelVersionService.SetModelVersion
        registered_model_id = ctx.registered_model.id

        model_version_msg = ModelVersionMessage(version=name, description=desc, registered_model_id=registered_model_id,
                                                labels=labels, time_created=date_created, time_updated=date_created)
        endpoint = "/api/v1/registry/{}/versions".format(registered_model_id)
        response = conn.make_proto_request("POST", endpoint, body=model_version_msg)
        model_version = conn.must_proto_response(response, SetModelVersionMessage.Response).model_version

        print("Created new ModelVersion: {}".format(model_version.version))
        return model_version

    def set_model(self, model, overwrite=False):
        if isinstance(model, six.string_types):  # filepath
            serialized_model = open(model, 'rb')  # file handle
        else:
            serialized_model, _, _ = _artifact_utils.serialize_model(model)  # bytestream

        # TODO: update `ModelVersion.model` field with artifact message

        self._upload_artifact(
            "model", serialized_model,
            _CommonCommonService.ArtifactTypeEnum.MODEL,
        )

    def add_asset(self, key, asset, overwrite=False):
        # similar to ExperimentRun.log_artifact
        raise NotImplementedError

    def del_asset(self, key):
        raise NotImplementedError

    def set_environment(self, env):
        # Env must be an EnvironmentBlob. Let's re-use the functionality from there
        raise NotImplementedError

    def del_environment(self):
        raise NotImplementedError

    def _get_url_for_artifact(self, key, method, artifact_type, part_num=0):
        if method.upper() not in ("GET", "PUT"):
            raise ValueError("`method` must be one of {'GET', 'PUT'}")

        Message = _ModelVersionService.GetUrlForArtifact
        msg = Message(
            model_version_id=self.id,
            key=key,
            method=method,
            artifact_type=artifact_type,
            part_number=part_num
        )
        data = _utils.proto_to_json(msg)
        endpoint = "{}://{}/api/v1/registry/versions/{}/getUrlForArtifact".format(
            self._conn.scheme,
            self._conn.socket,
            self.id
        )
        response = _utils.make_request("POST", endpoint, self._conn, json=data)
        _utils.raise_for_http_error(response)
        return _utils.json_to_proto(response.json(), Message.Response)

    def _upload_artifact(self, key, artifact_type, file_handle, part_size=64*(10**6)):
        file_handle.seek(0)

        # check if multipart upload ok
        url_for_artifact = self._get_url_for_artifact(key, "PUT", artifact_type, part_num=1)

        print("uploading {} to ModelDB".format(key))
        if url_for_artifact.multipart_upload_ok:
            # TODO: parallelize this
            file_parts = iter(lambda: file_handle.read(part_size), b'')
            for part_num, file_part in enumerate(file_parts, start=1):
                print("uploading part {}".format(part_num), end='\r')

                # get presigned URL
                url = self._get_url_for_artifact(key, "PUT", artifact_type, part_num=part_num).url

                # wrap file part into bytestream to avoid OverflowError
                #     Passing a bytestring >2 GB (num bytes > max val of int32) directly to
                #     ``requests`` will overwhelm CPython's SSL lib when it tries to sign the
                #     payload. But passing a buffered bytestream instead of the raw bytestring
                #     indicates to ``requests`` that it should perform a streaming upload via
                #     HTTP/1.1 chunked transfer encoding and avoid this issue.
                #     https://github.com/psf/requests/issues/2717
                part_stream = six.BytesIO(file_part)

                # upload part
                #     Retry connection errors, to make large multipart uploads more robust.
                for _ in range(3):
                    try:
                        response = _utils.make_request("PUT", url, self._conn, data=part_stream)
                    except requests.ConnectionError:  # e.g. broken pipe
                        time.sleep(1)
                        continue  # try again
                    else:
                        break
                _utils.raise_for_http_error(response)

                # commit part
                url = "{}://{}/api/v1/registry/versions/{}/commitArtifactPart".format(
                    self._conn.scheme,
                    self._conn.socket,
                    self.id
                )
                msg = _ModelVersionService.CommitArtifactPart(
                    model_version_id=self.id,
                    key=key
                )
                msg.artifact_part.part_number = part_num
                msg.artifact_part.etag = response.headers['ETag']
                data = _utils.proto_to_json(msg)
                response = _utils.make_request("POST", url, self._conn, json=data)
                _utils.raise_for_http_error(response)
            print()

            # complete upload
            url = "{}://{}/api/v1/registry/versions/{}/commitMultipartArtifact".format(
                self._conn.scheme,
                self._conn.socket,
                self.id
            )
            msg = _ModelVersionService.CommitMultipartArtifact(
                model_version_id=self.id,
                key=key
            )
            data = _utils.proto_to_json(msg)
            response = _utils.make_request("POST", url, self._conn, json=data)
            _utils.raise_for_http_error(response)
        else:
            # upload full artifact
            response = _utils.make_request("PUT", url_for_artifact.url, self._conn, data=file_handle)
            _utils.raise_for_http_error(response)

        print("upload complete")
