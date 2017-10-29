import os
import shutil
import pytest


@pytest.fixture
def test_folder():
    TEMP_FOLDER = "./temp/"
    os.makedirs(TEMP_FOLDER)
    yield TEMP_FOLDER

    shutil.rmtree(TEMP_FOLDER)

