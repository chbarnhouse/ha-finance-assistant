"""API Client for the Finance Assistant Addon."""
import asyncio
import logging
import socket
import aiohttp
import json
from ynab_api import ApiException # Re-export for convenience in __init__

_LOGGER = logging.getLogger(__name__)

class FinanceAssistantApiClientError(Exception):
    """Base exception for API client errors."""

class FinanceAssistantApiClientAuthenticationError(FinanceAssistantApiClientError):
    """Exception for authentication errors."""

class FinanceAssistantApiClient:
    """API Client to handle communication with the Finance Assistant addon."""

    def __init__(self, session: aiohttp.ClientSession, supervisor_url: str, direct_url: str, supervisor_token: str | None = None):
        """Initialize the API client."""
        self.websession = session
        self.supervisor_url = supervisor_url
        self.direct_url = direct_url
        self.supervisor_token = supervisor_token
        self.supervisor_headers = {}
        if self.supervisor_token:
            self.supervisor_headers = {"Authorization": f"Bearer {self.supervisor_token}"}
        _LOGGER.debug(f"API Client Initialized. Supervisor URL: {supervisor_url}, Direct URL: {direct_url}, Token Present: {bool(supervisor_token)}")

    async def _request(self, method: str, endpoint: str, **kwargs) -> dict | list | None:
        """Make an API request, handling Supervisor/Direct fallback logic."""
        last_error = None
        endpoint_clean = endpoint.lstrip('/')

        # Determine primary and secondary URLs/methods
        primary_url = None
        secondary_url = None
        primary_method = "Unknown"
        secondary_method = "Unknown"

        if self.supervisor_token:
            # Production/Supervisor: Try Supervisor first, fallback to Direct (slug/service name)
            primary_url = f"{self.supervisor_url}/{endpoint_clean}"
            primary_method = "Supervisor"
            secondary_url = f"{self.direct_url}/{endpoint_clean}" # Uses slug/service hostname
            secondary_method = "Direct (via slug)"
            primary_headers = self.supervisor_headers.copy()
            secondary_headers = {} # Direct doesn't use token
            _LOGGER.debug(f"Supervisor env detected. Primary: {primary_method}, Secondary: {secondary_method}")
        else:
            # Dev environment: Prioritize Direct (host.docker.internal or localhost)
            primary_url = f"{self.direct_url}/{endpoint_clean}" # Uses direct URL
            primary_method = "Direct (Dev)"
            secondary_url = None
            secondary_method = "None"
            primary_headers = {} # No auth for direct dev typically
            secondary_headers = {}
            _LOGGER.debug(f"Dev env detected. Primary: {primary_method}, No Secondary.")

        # Prepare common request arguments
        request_kwargs = {
            "headers": primary_headers,
            "timeout": aiohttp.ClientTimeout(total=20) # Increased timeout
        }
        # Merge additional kwargs like params, data, json
        request_kwargs.update(kwargs)

        # --- 1. Try Primary Method ---
        if primary_url:
            _LOGGER.debug(f"Attempting {primary_method} API request to: {primary_url}")
            try:
                async with self.websession.request(method, primary_url, **request_kwargs) as response:
                    _LOGGER.debug(f"{primary_method} response status: {response.status}")
                    if 200 <= response.status < 300:
                        _LOGGER.debug(f"{primary_method} API success ({response.status}) for {endpoint}")
                        if response.status == 204: return {} # Handle No Content
                        try:
                            return await response.json()
                        except (aiohttp.ContentTypeError, json.JSONDecodeError) as json_err:
                            content_text = await response.text()
                            _LOGGER.error(f"{primary_method} API returned non-JSON (status {response.status}): {json_err}. Content: {content_text[:200]}...", exc_info=True)
                            last_error = FinanceAssistantApiClientError(f"{primary_method} API returned non-JSON: {content_text[:100]}...")
                    elif response.status in [401, 403]:
                         _LOGGER.warning(f"{primary_method} API Authentication error ({response.status}) for {endpoint}. Check token. Falling back if possible.")
                         last_error = FinanceAssistantApiClientAuthenticationError(f"{primary_method} API {response.status} for {endpoint}")
                    elif response.status == 404:
                         _LOGGER.info(f"{primary_method} API 404 for {endpoint}. Check slug/endpoint. Falling back if possible.")
                         last_error = FinanceAssistantApiClientError(f"{primary_method} API 404 for {endpoint}")
                    else:
                        response_text = await response.text()
                        _LOGGER.warning(f"{primary_method} API failed ({response.status}) for {endpoint}. Response: {response_text[:200]}... Falling back if possible.")
                        last_error = FinanceAssistantApiClientError(f"{primary_method} API failed ({response.status}): {response_text[:100]}...")

            except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as conn_err:
                _LOGGER.warning(f"{primary_method} API connection error for {endpoint}: {conn_err}. Falling back if possible.")
                last_error = FinanceAssistantApiClientError(f"{primary_method} API connection error: {conn_err}")
            except Exception as err:
                 _LOGGER.error(f"Unexpected {primary_method} API error for {endpoint}: {err}", exc_info=True)
                 last_error = FinanceAssistantApiClientError(f"Unexpected {primary_method} API error: {err}")
        else:
             _LOGGER.error("Primary URL was not determined. Cannot make request.")
             raise FinanceAssistantApiClientError("Internal configuration error: Primary URL not set.")

        # --- 2. Try Secondary Method (if Primary failed and Secondary exists) ---
        if last_error and secondary_url:
            _LOGGER.info(f"Primary method failed ({last_error.__class__.__name__}). Attempting {secondary_method} API fallback to: {secondary_url}")
            # Update headers for secondary request (no auth)
            request_kwargs["headers"] = secondary_headers
            try:
                async with self.websession.request(method, secondary_url, **request_kwargs) as response:
                    _LOGGER.debug(f"{secondary_method} response status: {response.status}")
                    if 200 <= response.status < 300:
                        _LOGGER.info(f"{secondary_method} API success ({response.status}) for {endpoint}") # Log fallback success as info
                        if response.status == 204: return {} # Handle No Content
                        try:
                            return await response.json() # Success!
                        except (aiohttp.ContentTypeError, json.JSONDecodeError) as json_err:
                            content_text = await response.text()
                            _LOGGER.error(f"{secondary_method} API returned non-JSON (status {response.status}): {json_err}. Content: {content_text[:200]}...", exc_info=True)
                            # Use the original error from primary attempt if available
                            raise last_error or FinanceAssistantApiClientError(f"{secondary_method} API returned non-JSON: {content_text[:100]}...")
                    else:
                        # Fallback attempt also failed
                        response_text = await response.text()
                        _LOGGER.error(f"{secondary_method} API request failed ({response.status}) for {endpoint}. Response: {response_text[:200]}...")
                        # Raise the original error from primary attempt
                        raise last_error

            except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as conn_err:
                _LOGGER.error(f"{secondary_method} API connection error for {endpoint}: {conn_err}. Raising original error.")
                raise last_error # Raise original error
            except Exception as err:
                _LOGGER.error(f"Unexpected {secondary_method} API error for {endpoint}: {err}", exc_info=True)
                raise last_error or FinanceAssistantApiClientError(f"Unexpected {secondary_method} API error: {err}") # Raise original or new error

        # If we reached here and last_error still exists, both methods failed
        if last_error:
            _LOGGER.error(f"API request failed for {endpoint} after all attempts. Final error: {last_error.__class__.__name__}")
            raise last_error

        # Should not happen if logic is correct
        _LOGGER.error(f"API request for {endpoint} finished unexpectedly without result or error.")
        raise FinanceAssistantApiClientError("API request finished unexpectedly.")

    async def async_ping(self) -> dict:
        """Ping the addon API to verify connection."""
        return await self._request("GET", "/ping")

    async def async_get_all_data(self) -> dict:
        """Fetch all combined data from the addon."""
        return await self._request("GET", "/all_data")

    # Add more methods as needed, e.g.:
    # async def async_get_accounts(self) -> list:
    #     return await self._request("GET", "/api/accounts")
    #
    # async def async_save_manual_asset(self, asset_id: str, details: dict) -> dict:
    #     return await self._request("PUT", f"/api/manual_asset/{asset_id}", json=details)