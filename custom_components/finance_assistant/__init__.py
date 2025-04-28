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

    addon_slug = "finance_assistant"
    _LOGGER.debug(f"Using addon slug: {addon_slug}")
    session = async_get_clientsession(hass)
    supervisor_token = os.getenv("SUPERVISOR_TOKEN")

    # Initialize use_supervisor_api to False
    use_supervisor_api = False

    # --- Perform Initial Supervisor Ping Check ---
    if supervisor_token:
        _LOGGER.debug("Attempting initial Supervisor addon direct ping...")
        headers = {"Authorization": f"Bearer {supervisor_token}"}
        # Use the *root* ping endpoint for this initial check
        supervisor_ping_url = f"http://supervisor/addons/{addon_slug}/ping"
        _LOGGER.debug(f"Pinging Supervisor URL: {supervisor_ping_url}")
        try:
            async with session.request("GET", supervisor_ping_url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    use_supervisor_api = True
                    _LOGGER.info("Supervisor addon ping successful. Will prioritize Supervisor API.")
                else:
                    _LOGGER.warning(f"Supervisor addon ping failed with status: {resp.status}. Will attempt direct connection.")
        except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
            _LOGGER.warning(f"Supervisor addon ping failed with connection error: {err}. Will attempt direct connection.")
        except Exception as err: # Catch unexpected errors during ping
            _LOGGER.error(f"Unexpected error during Supervisor addon ping: {err}", exc_info=True)
            _LOGGER.warning("Will attempt direct connection due to unexpected error during ping.")
    else:
        _LOGGER.debug("SUPERVISOR_TOKEN not found. Assuming direct connection needed.")

    if not use_supervisor_api:
         _LOGGER.warning("Supervisor addon ping failed or token missing. Direct connection will be primary method.")
    # --- End Revised Environment Check ---

    # Create the coordinator instance
    # Pass the determined 'use_supervisor_api' flag - though the coordinator itself will handle fallback
    coordinator = FinanceAssistantDataUpdateCoordinator(
        hass,
        addon_slug,
        entry # Pass the config entry
        # use_supervisor_api # Flag not strictly needed by coordinator now
    )

    # Perform initial connection verification (will try Supervisor then Direct)
    try:
        await coordinator.verify_connection()
        _LOGGER.info("Coordinator connection verified successfully.")
    except ConfigEntryNotReady as err:
        _LOGGER.error(f"Coordinator connection verification failed: {err}")
        raise # Re-raise ConfigEntryNotReady
    except Exception as err:
        _LOGGER.error(f"Unexpected error during coordinator connection verification: {err}", exc_info=True)
        raise ConfigEntryNotReady(f"Unexpected error verifying connection: {err}") from err

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

    def __init__(self, hass: HomeAssistant, addon_slug: str, entry: ConfigEntry):
        """Initialize the coordinator."""
        _LOGGER.info(f"Initializing Finance Assistant Coordinator for slug: {addon_slug}")
        self.addon_slug = addon_slug
        self.supervisor_token = os.environ.get('SUPERVISOR_TOKEN')
        # Define headers for Supervisor API calls
        self.supervisor_headers = {}
        if self.supervisor_token:
            self.supervisor_headers = {"Authorization": f"Bearer {self.supervisor_token}"}

        # Define the port used for direct connection (from addon config)
        self.direct_port = 8000

        # Determine Base URLs based on environment and settings
        self.supervisor_url = f"http://supervisor/addons/{addon_slug}/api"
        # !!! WORKAROUND !!! Use 'homeassistant:8000' instead of slug due to DNS issues
        # self.direct_url = f"http://{addon_slug}:{self.direct_port}/api"
        self.direct_url = f"http://homeassistant:{self.direct_port}/api"
        _LOGGER.info(f"Supervisor URL: {self.supervisor_url}")
        _LOGGER.info(f"Direct URL (WORKAROUND): {self.direct_url}")
        self.websession = async_get_clientsession(hass)

        # If no supervisor token, assume dev environment and set direct URL to host.docker.internal
        if not self.supervisor_token:
             _LOGGER.info("No SUPERVISOR_TOKEN found. Assuming dev environment, setting direct URL to host.docker.internal.")
             self.direct_url = "http://host.docker.internal:8000/api"
        # else: # Optional: Log if we are in Supervisor mode
        #     _LOGGER.info("SUPERVISOR_TOKEN found. Assuming Supervisor environment.")

        _LOGGER.debug(f"Coordinator initialized. Supervisor URL base: {self.supervisor_url}, Direct URL base: {self.direct_url}")

        # Call super().__init__ AFTER defining attributes used by it
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
            # config_entry=entry # Pass the config entry here
        )
        self.config_entry = entry # Store config_entry if needed elsewhere

    async def _request(self, method, endpoint, params=None, data=None, json_data=None):
        """Make an API request, trying the appropriate method based on environment."""
        headers = {}
        last_error = None
        endpoint_clean = endpoint.lstrip('/')

        # Determine primary and secondary URLs/methods based on environment
        primary_url = None
        secondary_url = None
        primary_method = "Unknown"
        secondary_method = "Unknown"

        if self.supervisor_token:
            # Production/Supervisor: Try Supervisor first, fallback to Direct (slug)
            primary_url = f"{self.supervisor_url}/{endpoint_clean}"
            primary_method = "Supervisor"
            secondary_url = f"{self.direct_url}/{endpoint_clean}" # Uses slug hostname here
            secondary_method = "Direct (via slug)"
            headers = {"Authorization": f"Bearer {self.supervisor_token}"}
            _LOGGER.debug(f"Supervisor env detected. Primary: {primary_method}, Secondary: {secondary_method}")
        else:
            # Dev environment: Prioritize Direct (host.docker.internal)
            primary_url = f"{self.direct_url}/{endpoint_clean}" # Uses host.docker.internal here
            primary_method = "Direct (via host.docker.internal)"
            # No Supervisor fallback possible without token
            secondary_url = None
            secondary_method = "None"
            _LOGGER.debug(f"Dev env detected. Primary: {primary_method}, No Secondary.")

        # --- 1. Try Primary Method ---
        if primary_url:
            _LOGGER.debug(f"Attempting {primary_method} API request to: {primary_url}")
            primary_headers = headers.copy() # Use appropriate headers for primary
            try:
                async with self.websession.request(
                    method, primary_url, headers=primary_headers, params=params, data=data, json=json_data, timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if 200 <= response.status < 300:
                        _LOGGER.debug(f"{primary_method} API success ({response.status}) for {endpoint}")
                        try:
                            if response.status == 204: return {} # Handle No Content
                            return await response.json()
                        except (aiohttp.ContentTypeError, json.JSONDecodeError) as json_err:
                            content_text = await response.text()
                            _LOGGER.error(f"{primary_method} API returned non-JSON (status {response.status}): {json_err}. Content: {content_text[:100]}...")
                            last_error = UpdateFailed(f"{primary_method} API returned non-JSON: {content_text[:100]}...")
                        except Exception as err:
                            _LOGGER.error(f"Unexpected {primary_method} API error for {endpoint}: {err}", exc_info=True)
                            last_error = UpdateFailed(f"Unexpected {primary_method} API error: {err}")
                    # --- Handle specific non-success codes before fallback ---
                    elif response.status == 404:
                         _LOGGER.warning(f"{primary_method} API 404 for {endpoint}. Check slug/endpoint/token. Falling back if possible.")
                         last_error = UpdateFailed(f"{primary_method} API 404 for {endpoint}")
                    elif response.status == 401:
                         _LOGGER.warning(f"{primary_method} API 401 for {endpoint}. Check token. Falling back if possible.")
                         last_error = UpdateFailed(f"{primary_method} API 401 for {endpoint}")
                    else:
                        response_text = await response.text()
                        _LOGGER.warning(f"{primary_method} API failed ({response.status}) for {endpoint}. Response: {response_text[:200]}... Falling back if possible.")
                        last_error = UpdateFailed(f"{primary_method} API failed ({response.status}): {response_text[:100]}...")

            except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
                _LOGGER.warning(f"{primary_method} API connection error for {endpoint}: {err}. Falling back if possible.")
                last_error = UpdateFailed(f"{primary_method} API connection error: {err}")
            except Exception as err:
                 _LOGGER.error(f"Unexpected {primary_method} API error for {endpoint}: {err}", exc_info=True)
                 last_error = UpdateFailed(f"Unexpected {primary_method} API error: {err}")
        else:
             # Should not happen if logic above is correct, but good to handle
             _LOGGER.error("Primary URL was not determined. Cannot make request.")
             raise UpdateFailed("Internal configuration error: Primary URL not set.")

        # --- 2. Try Secondary Method (if Primary failed and Secondary exists) ---
        if last_error and secondary_url:
            _LOGGER.info(f"Primary method failed ({last_error}). Attempting {secondary_method} API fallback to: {secondary_url}")
            secondary_headers = {} # Direct fallback doesn't use Supervisor token
            try:
                async with self.websession.request(
                    method, secondary_url, headers=secondary_headers, params=params, data=data, json=json_data, timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if 200 <= response.status < 300:
                        _LOGGER.info(f"{secondary_method} API success ({response.status}) for {endpoint}") # Log fallback success as info
                        # Fallback succeeded, clear the error and return data
                        last_error = None
                        try:
                             if response.status == 204: return {} # Handle No Content
                             return await response.json()
                        except (aiohttp.ContentTypeError, json.JSONDecodeError) as json_err:
                            content_text = await response.text()
                            _LOGGER.error(f"{secondary_method} API returned non-JSON (status {response.status}): {json_err}. Content: {content_text[:100]}...")
                            # Even though fallback connection worked, data is bad, raise UpdateFailed
                            raise UpdateFailed(f"{secondary_method} API returned non-JSON: {content_text[:100]}...")
                    else:
                        # Fallback attempt also failed
                        response_text = await response.text()
                        _LOGGER.error(f"{secondary_method} API request failed ({response.status}) for {endpoint}. Response: {response_text[:200]}...")
                        # Raise an error indicating the fallback failure, potentially including the original primary error?
                        # For now, just raise the secondary error.
                        raise UpdateFailed(f"{secondary_method} API failed ({response.status}): {response_text[:100]}...")

            except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
                _LOGGER.error(f"{secondary_method} API connection error for {endpoint}: {err}")
                # Raise error indicating secondary connection failure
                raise UpdateFailed(f"{secondary_method} API connection error: {err}")
            except UpdateFailed as err:
                 _LOGGER.error(f"Secondary API UpdateFailed: {err}")
                 raise err # Re-raise UpdateFailed from non-200/non-JSON secondary response
            except Exception as err:
                 _LOGGER.exception(f"Unexpected {secondary_method} API error for {endpoint}: {err}")
                 # Raise error indicating unexpected secondary failure
                 raise UpdateFailed(f"Unexpected {secondary_method} API error: {err}")

        # --- 3. Final Outcome ---
        if last_error:
             # If we get here, it means Primary failed AND (Secondary doesn't exist OR Secondary also failed)
             _LOGGER.error(f"API request failed for {endpoint} after all attempts. Last error: {last_error}")
             raise last_error # Raise the final error (either from Primary or Secondary)
        else:
             # This should only happen if Primary succeeded on the first try.
             # The function should have returned within the first 'try' block.
             _LOGGER.error(f"Reached unexpected end of _request function for {endpoint} without error but without returning data.")
             raise UpdateFailed("Unexpected state in API request logic")

    async def verify_connection(self):
        """Verify connection to the addon API by trying the ping endpoint."""
        _LOGGER.info("Verifying connection to Finance Assistant API...")
        try:
            # Use the ROOT '/ping' endpoint for verification now
            _LOGGER.info(f"Attempting API ping (using root /ping endpoint)...")
            # Use the _request method, which handles supervisor/direct logic automatically
            # It needs the relative path (without /api prefix) for the root endpoint
            # Construct the full URL manually for the root ping, respecting the workaround
            ping_url_supervisor = f"http://supervisor/addons/{self.addon_slug}/ping"
            ping_url_direct = f"http://homeassistant:{self.direct_port}/ping" # WORKAROUND URL

            last_error = None
            # Try Supervisor first
            try:
                _LOGGER.debug(f"Trying Supervisor ping: {ping_url_supervisor}")
                async with self.websession.get(ping_url_supervisor, headers=self.supervisor_headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if 200 <= response.status < 300:
                        _LOGGER.info("Supervisor ping successful.")
                        return True
                    else:
                        response_text = await response.text()
                        _LOGGER.warning(f"Supervisor ping failed ({response.status}): {response_text[:100]}...")
                        last_error = UpdateFailed(f"Supervisor ping failed ({response.status})")
            except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
                _LOGGER.warning(f"Supervisor ping connection error: {err}")
                last_error = UpdateFailed(f"Supervisor ping connection error: {err}")
            except Exception as err:
                _LOGGER.error(f"Unexpected Supervisor ping error: {err}", exc_info=True)
                last_error = UpdateFailed(f"Unexpected Supervisor ping error: {err}")

            # Try Direct (Workaround) if Supervisor failed
            if last_error:
                try:
                    _LOGGER.info(f"Supervisor ping failed. Trying Direct (Workaround) ping: {ping_url_direct}")
                    async with self.websession.get(ping_url_direct, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if 200 <= response.status < 300:
                            _LOGGER.info("Direct (Workaround) ping successful.")
                            return True
                        else:
                            response_text = await response.text()
                            _LOGGER.error(f"Direct (Workaround) ping failed ({response.status}): {response_text[:100]}...")
                            raise UpdateFailed(f"Direct (Workaround) ping failed ({response.status})") # Raise error if direct fails
                except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
                    _LOGGER.error(f"Direct (Workaround) ping connection error: {err}")
                    raise UpdateFailed(f"Direct (Workaround) ping connection error: {err}") # Raise error if direct fails
                except Exception as err:
                    _LOGGER.error(f"Unexpected Direct (Workaround) ping error: {err}", exc_info=True)
                    raise UpdateFailed(f"Unexpected Direct (Workaround) ping error: {err}") # Raise error if direct fails

            # If we reach here, both attempts failed
            _LOGGER.error(f"Finance Assistant API connection verification failed after all attempts. Last Supervisor error: {last_error}")
            raise ConfigEntryNotReady(f"Finance Assistant API connection failed: {last_error}")
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
            # Data fetching still uses the /api prefix via the base URLs in _request
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
