import pytest

from .. import utils

import verta.dataset
import verta.environment

from sklearn.linear_model import LogisticRegression


class TestModelVersion:
    def test_get_by_name(self, registered_model):
        model_version = registered_model.get_or_create_version(name="my version")
        retrieved_model_version = registered_model.get_version(name=model_version.name)
        assert retrieved_model_version.id == model_version.id

    def test_get_by_id(self, registered_model):
        model_version = registered_model.get_or_create_version()
        retrieved_model_version = registered_model.get_version(id=model_version.id)
        assert model_version.id == retrieved_model_version.id

    def test_get_by_clent(self, client):
        registered_model = client.set_registered_model()
        model_version = registered_model.get_or_create_version(name="my version")

        retrieved_model_version_by_id = client.get_registered_model_version(id=model_version.id)
        retrieved_model_version_by_name = client.get_registered_model_version(name=model_version.name)

        assert retrieved_model_version_by_id.id == model_version.id
        assert retrieved_model_version_by_name.id == model_version.id

        if registered_model:
            utils.delete_registered_model(registered_model.id, client._conn)

    def test_set_model(self, registered_model):
        model_version = registered_model.get_or_create_version(name="my version")
        log_reg_model = LogisticRegression()
        model_version.set_model(log_reg_model, True)

        model_version._refresh_cache()
        assert model_version._msg.model.key == "model"

    def test_add_asset(self, registered_model):
        model_version = registered_model.get_or_create_version(name="my version")
        log_reg_model = LogisticRegression()
        model_version.add_asset("some-asset", log_reg_model)

        model_version.add_asset("some-asset", log_reg_model)
