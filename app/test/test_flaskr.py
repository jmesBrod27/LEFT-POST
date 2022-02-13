import os
import tempfile

import pytest

from app import api
from flask import json


def test_add():        
    response = api.api.test_client().post(
        '/api/v1/weather/sensor/create',
        data=json.dumps({'ID': 1, 'b': 2}),
        content_type='application/json',
    )

    data = json.loads(response.get_data(as_text=True))

    assert response.status_code == 200
    assert data['Item'] == data



