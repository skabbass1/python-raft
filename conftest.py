import pathlib
import os
import shutil

import pytest

@pytest.fixture(autouse=True)
def tmp_directory_setup():
    tmp_path = pathlib.Path('tmp')
    if not tmp_path.exists():
        os.mkdir(tmp_path)

    yield

    shutil.rmtree(tmp_path)


