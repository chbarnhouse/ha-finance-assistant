"""The Finance Assistant integration."""
import asyncio
import logging
from datetime import timedelta, datetime
import os
import socket
import json
import inspect

import aiohttp
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.const import Platform
from homeassistant.components import persistent_notification
from homeassistant.exceptions import ConfigEntryNotReady

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# Define the update interval for fetching data from the addon
SCAN_INTERVAL = timedelta(minutes=5)

PLATFORMS = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Finance Assistant from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    # --- Determine Addon Slug and Connection Method ---
    addon_slug = "finance_assistant"
    _LOGGER.debug(f"Using addon slug: {addon_slug}")

    # --- Reliable Environment Check ---
    use_supervisor_api = False
    session = async_get_clientsession(hass)
    supervisor_token = os.getenv("SUPERVISOR_TOKEN")
    if supervisor_token:
        _LOGGER.debug("SUPERVISOR_TOKEN found, attempting Supervisor ping...")
        try:
            headers = {"Authorization": f"Bearer {supervisor_token}"}
            ping_url = "http://supervisor/supervisor/ping"
            async with session.get(ping_url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    ping_data = await resp.json()
                    if ping_data.get("result") == "ok":
                        use_supervisor_api = True
                        _LOGGER.info("Supervisor environment confirmed via successful ping.")
                    else:
                        _LOGGER.warning(f"Supervisor ping responded but result was not ok: {ping_data}")
                else:
                    _LOGGER.warning(f"Supervisor ping failed with status: {resp.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError, socket.gaierror) as err:
            _LOGGER.warning(f"Supervisor ping failed with exception: {err}")
        except Exception as err: # Catch unexpected errors during ping
            _LOGGER.error(f"Unexpected error during Supervisor ping: {err}", exc_info=True)
    else:
        _LOGGER.debug("SUPERVISOR_TOKEN not found.")

    if use_supervisor_api:
        _LOGGER.info("Will use Supervisor API for addon communication.")
    else:
        _LOGGER.warning("Will attempt direct connection for addon communication (Supervisor check failed or token missing).")
    # --- End Environment Check ---

    # Create the coordinator instance
    coordinator = FinanceAssistantDataUpdateCoordinator(
        hass,
        addon_slug,
        use_supervisor_api
    )

    # Perform initial connection verification
    try:
        # verify_connection will now use the proper primary/fallback logic
        await coordinator.verify_connection()
    except ConfigEntryNotReady as err:
        _LOGGER.error(f"Initial connection verification failed: {err}")
        raise # Re-raise ConfigEntryNotReady
    except Exception as err:
        _LOGGER.error(f"Unexpected error during initial connection: {err}", exc_info=True)
        # Wrap unexpected errors in ConfigEntryNotReady
        raise ConfigEntryNotReady(f"Unexpected error connecting to Finance Assistant addon: {err}") from err

    # Store coordinator instance
    hass.data[DOMAIN][entry.entry_id] = coordinator

    # Fetch initial data so sensors are ready
    await coordinator.async_config_entry_first_refresh()

    # Forward the setup to the sensor platform
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok


class FinanceAssistantDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Finance Assistant data from the addon."""

    def __init__(self, hass: HomeAssistant, addon_slug: str, use_supervisor: bool):
        """Initialize."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
        )
        self.addon_slug = addon_slug
        self.use_supervisor = use_supervisor
        self.websession = async_get_clientsession(hass)

        # Define base URLs based on environment
        if self.use_supervisor:
            # Production HA: Direct fallback uses the supervisor host with addon name
            # Use the format that the Supervisor DNS would resolve
            self.direct_api_base_url = f"http://{addon_slug}:8000/api"
            _LOGGER.debug(f"Supervisor mode: Direct fallback URL: {self.direct_api_base_url}")
        else:
            # Devcontainer: Always use localhost for direct testing
            self.direct_api_base_url = "http://localhost:8000/api"
            _LOGGER.debug(f"Non-Supervisor mode: Direct fallback URL uses localhost: {self.direct_api_base_url}")

        # TEMPORARY OVERRIDE FOR TESTING
        self.direct_api_base_url = "http://localhost:8000/api"
        _LOGGER.warning(f"TESTING: Forcing direct URL to {self.direct_api_base_url}")

        _LOGGER.debug(f"Coordinator initialized. use_supervisor={use_supervisor}, addon_slug={addon_slug}")

    async def _request(self, method, endpoint, params=None, data=None, json_data=None):
        """Make an API request, trying Supervisor first (if applicable), then direct."""
        headers = {}
        supervisor_url = None
        # Construct direct_url using the base defined in __init__
        direct_url = f"{self.direct_api_base_url}/{endpoint.lstrip('/')}"
        last_error = None

        # TEMPORARY TEST: Always use direct
        _LOGGER.warning(f"TESTING: Skipping Supervisor API, using direct URL: {direct_url}")

        # Clear Supervisor auth header if it was set
        headers.pop("Authorization", None)
        try:
            _LOGGER.warning(f"Making direct request to: {direct_url}")
            async with self.websession.request(
                method, direct_url, headers=headers, params=params, data=data, json=json_data, timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if 200 <= response.status < 300:
                    _LOGGER.debug(f"Direct API success ({response.status}) for {endpoint}")
                    try:
                         if response.status == 204:
                            return {}
                         return await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError) as json_err:
                        content_text = await response.text()
                        _LOGGER.error(f"Direct API for {endpoint} returned non-JSON content (status {response.status}, error: {json_err}). Content: {content_text[:100]}")
                        raise UpdateFailed(f"Direct API returned non-JSON for {endpoint}: {content_text[:100]}")
                else:
                    response_text = await response.text()
                    _LOGGER.error(f"Direct API request failed for {endpoint} with status {response.status}. Response: {response_text[:200]}")
                    raise UpdateFailed(f"Direct API failed for {endpoint}: Status {response.status}")

        except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
            _LOGGER.error(f"Direct API connection error for {endpoint}: {err}")
            # If direct connection also fails, raise the most recent error (prefer direct error if Supervisor also failed)
            raise UpdateFailed(f"Direct API connection error for {endpoint}: {err}")
        except UpdateFailed as err:
             # Re-raise UpdateFailed from non-200 direct response or non-JSON
             raise err
        except Exception as err:
             _LOGGER.exception(f"Unexpected direct API error for {endpoint}: {err}")
             raise UpdateFailed(f"Unexpected direct API error for {endpoint}: {err}")

        # End of temporary test block

        # Original code commented out for testing
        # 1. Try Supervisor API (if configured to use it)
        # if self.use_supervisor:
        #     # Supervisor code...
        # .... rest of original method

    async def verify_connection(self):
        """Verify connection to the addon API by trying a simple endpoint."""
        _LOGGER.info("Verifying connection to Finance Assistant API...")
        try:
            # Use '/api/ping' or similar simple GET endpoint in the addon
            # Addon needs a simple endpoint like /api/ping or /api/debug
            _LOGGER.info(f"Trying to connect to debug endpoint with base URL: {self.direct_api_base_url}")
            await self._request("GET", "debug") # Assuming addon has /api/debug
            _LOGGER.info("Finance Assistant API connection verified successfully.")
            return True
        # Catch UpdateFailed specifically from _request
        except UpdateFailed as err:
             _LOGGER.error(f"Finance Assistant API connection verification failed: {err}")
             raise ConfigEntryNotReady(f"Finance Assistant API connection failed: {err}") from err
        except Exception as err:
            _LOGGER.error(f"Unexpected error during Finance Assistant API connection verification: {err}", exc_info=True)
            raise ConfigEntryNotReady(f"Unexpected error verifying connection: {err}") from err

    async def _async_update_data(self):
        """Fetch data from the Finance Assistant addon API."""
        _LOGGER.debug("Fetching data from Finance Assistant addon")
        try:
            data = await self._request("GET", "all_data")
            if not isinstance(data, dict):
                _LOGGER.error(f"API '/all_data' returned non-dictionary data: {type(data)}")
                # If Supervisor/Direct failed, _request raises UpdateFailed
                # This path implies _request returned something unexpected but didn't fail
                return {} # Return empty dict to prevent component failure

            _LOGGER.debug(f"Data fetched successfully: Keys={list(data.keys())}")
            # Simplified validation for now
            return data
        except UpdateFailed as err:
            _LOGGER.error(f"Update failed during _async_update_data: {err}")
            # Don't generate mock data, let HA handle the update failure
            raise # Re-raise UpdateFailed to HASS
        except Exception as err:
            _LOGGER.error(f"Unexpected error updating Finance Assistant data: {err}", exc_info=True)
            raise UpdateFailed(f"Error communicating with API: {err}") from err
