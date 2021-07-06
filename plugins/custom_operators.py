from airflow.models.baseoperator import BaseOperator

from datetime import timedelta
from dateutil import parser

import logging
import requests
import shutil


class HttpDownloadTimestampedFile(BaseOperator):
    """
    Operator to download files using timestamps in names over HTTP
    :param url: base url where files are located
    :type url: str
    :param local_dir: local directory path to store files
    :type local_dir: str
    :param step: step to add to ds to generate timestamp
    :type step: timedelta
    """

    def __init__(
            self,
            url: str,
            local_dir: str,
            step: timedelta,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.local_dir = local_dir
        self.step = step

    def execute(self, context):
        """
        Operator entry point

        :param context: dict
        """
        ts = context["ts"]
        dt = parser.parse(ts) + self.step
        file_url = dt.strftime(self.url)

        # creates the local file path
        local_path = self.local_dir + '/' + file_url.split('/')[-1]
        logging.info(f"Starting downloading {file_url} to {local_path}")

        # opens a stream to donwload the file and store it under local_dir
        with requests.get(file_url, stream=True) as r:
            with open(local_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        logging.info("Download finished")
