# -*- coding: utf-8 -*-

from asyncio import sleep as asleep
from time import time
from typing import Any, Dict, List, Tuple, Union, final

from requests.exceptions import RequestException

from backend.base.custom_exceptions import (DownloadLimitReached,
                                            ExternalClientNotWorking)
from backend.base.definitions import (Constants, DownloadSource, DownloadState,
                                      DownloadType, SearchResultData)
from backend.base.file_extraction import extract_filename_data
from backend.base.helpers import Session
from backend.base.logging import LOGGER
from backend.implementations.external_clients import (BaseExternalClient,
                                                      ExternalClients)
from backend.internals.settings import Settings


def get_dcpp_link_components(
    download_link: str
) -> Tuple[int, int, int, str, str]:
    """Extract components from a DC++ download link.

    Args:
        download_link (str): The DC++ download link.

    Returns:
        Tuple[int, int, int, str, str]: A tuple containing the client ID,
            search instance ID, result ID, TTH, and default name.
    """
    client_id, search_instance_id, result_id, tth, default_name = (
        download_link.replace(Constants.DCPP_URL_PREFIX, '').split('|')
    )
    return (
        int(client_id), int(search_instance_id), int(result_id),
        tth, default_name
    )


def make_dcpp_link(
    client_id: int,
    search_instance_id: int,
    result_id: int,
    tth: str,
    default_name: str
) -> str:
    """Create a DC++ download link from its components.

    Args:
        client_id (int): The client ID.
        search_instance_id (int): The search instance ID.
        result_id (int): The result ID.
        tth (str): The TTH hash.
        default_name (str): The default name.

    Returns:
        str: The DC++ download link.
    """
    return f'{Constants.DCPP_URL_PREFIX}{client_id}|{search_instance_id}|{result_id}|{tth}|{default_name}'


@final
class AirDCPP(BaseExternalClient):
    client_type = 'AirDC++'
    download_type = DownloadType.DCPP

    required_tokens = ('title', 'base_url', 'username', 'password')

    state_mapping = {
        "new": DownloadState.QUEUED_STATE,
        "queued": DownloadState.QUEUED_STATE,

        "download_error": DownloadState.FAILED_STATE,

        "downloaded": DownloadState.IMPORTING_STATE,
        "completed": DownloadState.IMPORTING_STATE,
        "shared": DownloadState.IMPORTING_STATE
    }

    def __init__(self, client_id: int) -> None:
        super().__init__(client_id)

        self.ssn: Union[Session, None] = None
        self.download_tths: Dict[str, Union[int, None]] = {}
        self.settings = Settings()
        return

    @staticmethod
    def _login(
        base_url: str,
        username: Union[str, None],
        password: Union[str, None]
    ) -> Union[Session, str]:
        ssn = Session()
        ssn.headers.update({
            "Content-Type": "application/json"
        })

        if not (username and password):
            return "No credentials provided"

        try:
            auth_request = ssn.post(
                f"{base_url}/api/v1/sessions/authorize",
                json={
                    "username": username,
                    "password": password,
                    "max_inactivity": 60 # minutes
                }
            )
            auth_response = auth_request.json()

        except RequestException:
            LOGGER.exception("Can't connect to AirDC++ instance: ")
            return "Can't connect; invalid base URL"

        if auth_request.status_code == 412:
            LOGGER.error(
                f"API version of AirDC++ instance unsupported: {auth_request.text}"
            )
            return "API version unsupported"

        if auth_request.status_code == 401:
            LOGGER.error(
                f"Failed to authenticate for AirDC++ instance: {auth_request.text}"
            )
            return "Can't authenticate"

        ssn.headers.update({
            "Authorization": f"Bearer {auth_response['auth_token']}"
        })

        return ssn

    def login(self) -> None:
        if not self.ssn:
            result = self._login(
                self.base_url, self.username, self.password
            )
            if isinstance(result, str):
                raise ExternalClientNotWorking(result)
            self.ssn = result
        return

    def add_download(
        self,
        download_link: str,
        target_folder: str,
        download_name: Union[str, None]
    ) -> str:
        if download_name is not None:
            download_name = download_link.split('|')[-1]

        client_id, search_instance_id, result_id, tth, default_name = get_dcpp_link_components(
            download_link)

        if not self.ssn:
            result = self._login(
                self.base_url, self.username, self.password
            )
            if isinstance(result, str):
                raise ExternalClientNotWorking(result)
            self.ssn = result

        add_result = self.ssn.post(
            f'{self.base_url}/api/v1/search/{search_instance_id}/results/{result_id}/download',
            data={
                "target_directory": target_folder,
                "target_name": download_name}).json()

        if not (isinstance(add_result, dict) and "bundle_info" in add_result):
            if (
                isinstance(add_result, dict)
                and "download limit" in str(
                    add_result.get("message", "")
                ).lower()
            ):
                raise DownloadLimitReached(
                    DownloadSource.DCPP,
                    external_client_id=self._id
                )

            raise ExternalClientNotWorking("Failed to add DC++ download")

        bundle_id = str(add_result["bundle_info"]["id"])
        self.download_tths[bundle_id] = None
        return str(bundle_id)

    def get_download(self, download_id: str) -> Union[dict, None]:
        if not self.ssn:
            result = self._login(
                self.base_url, self.username, self.password
            )
            if isinstance(result, str):
                raise ExternalClientNotWorking(result)
            self.ssn = result

        result = self.ssn.get(
            f'{self.base_url}/api/v1/queue/bundles/{download_id}',
        ).json()
        if not result or not isinstance(result, dict):
            if download_id in self.download_tths:
                return None
            else:
                return {}

        if "size" in result and result["size"] > 0:
            size = result["size"]
            progress = round(
                result.get("downloaded_bytes", 0) / result["size"] * 100,
                2
            )
        else:
            size = -1
            progress = result.get("downloaded_bytes", 0)

        state = self.state_mapping.get(
            result.get("status", {}).get("id"),
            DownloadState.IMPORTING_STATE
        )
        if state == DownloadState.FAILED_STATE:
            # Download is failing
            if self.download_tths[download_id] is None:
                self.download_tths[download_id] = round(time())
                state = DownloadState.DOWNLOADING_STATE

            else:
                timeout = self.settings.sv.failing_torrent_timeout
                if timeout and (
                    time() - (self.download_tths[download_id] or 0)
                    > timeout
                ):
                    state = DownloadState.FAILED_STATE
        else:
            self.download_tths[download_id] = None

        return {
            'size': size,
            'progress': progress,
            'speed': result.get("speed", 0),
            'state': state
        }

    def delete_download(self, download_id: str, delete_files: bool) -> None:
        if not self.ssn:
            result = self._login(
                self.base_url, self.username, self.password
            )
            if isinstance(result, str):
                raise ExternalClientNotWorking(result)
            self.ssn = result

        self.ssn.post(
            f'{self.base_url}/api/v1/queue/bundles/{download_id}/remove',
            data={
                'remove_finished': delete_files
            }
        )
        del self.download_tths[download_id]
        return

    @staticmethod
    def test(
        base_url: str,
        username: Union[str, None] = None,
        password: Union[str, None] = None,
        api_token: Union[str, None] = None
    ) -> Union[str, None]:
        result = AirDCPP._login(
            base_url,
            username,
            password
        )
        if isinstance(result, str):
            return result
        return None


# Mock of indexer class until we have a proper implementation
class DCPPIndexers:
    @staticmethod
    def get_indexers(
        download_type: Union[DownloadType, None] = None
    ) -> List[BaseExternalClient]:
        """Get a list of the indexers.

        Args:
            download_type (Union[DownloadType, None], optional): The download
            type to filter the indexers by.
                Defaults to None.

        Returns:
            List[Dict[str, Any]]: The list with the indexers.
        """
        if download_type == DownloadType.DCPP:
            return [
                AirDCPP(client["id"])
                for client in ExternalClients.get_clients(
                    download_type=DownloadType.DCPP
                )
            ]

        return []


async def AirDCPPSearch(
    client: AirDCPP,
    query: str
) -> List[SearchResultData]:
    if client.ssn is None:
        result = client._login(
            client.base_url, client.username, client.password
        )
        if isinstance(result, str):
            raise ExternalClientNotWorking(result)
        client.ssn = result

    instance_id: int = client.ssn.post(
        f"{client.base_url}/api/v1/search",
    ).json().get("id")

    client.ssn.post(
        f"{client.base_url}/api/v1/search/{instance_id}/hub_search",
        data={
            "query": {
                "pattern": query
            },
            "priority": Constants.DCPP_SEARCH_PRIORITY
        }
    )

    await asleep(Constants.DCPP_SEARCH_TIMEOUT)

    search_results = client.ssn.get(
        f"{client.base_url}/api/v1/search/{instance_id}/results/0/100"
    ).json()

    if isinstance(search_results, dict):
        results: List[Dict[str, Any]] = search_results.get("results", [])
    else:
        results: List[Dict[str, Any]] = search_results

    formatted_results: List[SearchResultData] = [
        {
            **extract_filename_data(
                result["name"],
                assume_volume_number=False,
                fix_year=True
            ),
            "download_type": DownloadType.DCPP.value,
            "link": make_dcpp_link(
                client.id, instance_id,
                result["id"], result["tth"],
                result["name"]
            ),
            "display_title": result["name"],
            "source": client.title,
            "size": result.get("size", 0),
            "seeders": result.get("slots", {}).get("total"),
            "leechers": result.get("slots", {}).get("free")
        }
        for result in results
    ]

    return formatted_results
