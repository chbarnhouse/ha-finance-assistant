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

    # --- Revised Environment Check: Try pinging addon via Supervisor ---
    use_supervisor_api = False
    if supervisor_token:
        _LOGGER.debug("SUPERVISOR_TOKEN found, attempting Supervisor addon ping...")
        ping_url = f"http://supervisor/addons/{addon_slug}/api/ping"
        headers = {"Authorization": f"Bearer {supervisor_token}"}
        try:
            # Short timeout for the initial check
            async with session.get(ping_url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    # Ensure response is expected (optional: check content)
                    # pong_data = await resp.json() # Add if ping returns json
                    # if pong_data.get("message") == "pong":
                    use_supervisor_api = True
                    _LOGGER.info("Supervisor addon ping successful. Will prioritize Supervisor API.")
                    # else:
                    #     _LOGGER.warning(f"Supervisor addon ping returned 200 but unexpected content: {pong_data}")
                else:
                    # Log specific failure but don't raise here, fallback will be used
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

    def __init__(self, hass: HomeAssistant, addon_slug: str):
        """Initialize."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
        )
        self.addon_slug = addon_slug
        self.websession = async_get_clientsession(hass)
        self.supervisor_token = os.getenv("SUPERVISOR_TOKEN")

        # Define BOTH base URLs regardless of initial check
        self.supervisor_api_base_url = f"http://supervisor/addons/{self.addon_slug}/api"
        # Always use host.docker.internal for direct fallback in this setup
        self.direct_api_base_url = "http://host.docker.internal:8000/api"

        _LOGGER.debug(f"Coordinator initialized. Supervisor URL base: {self.supervisor_api_base_url}, Direct URL base: {self.direct_api_base_url}")

    async def _request(self, method, endpoint, params=None, data=None, json_data=None):
        """Make an API request, trying Supervisor first, then direct fallback."""
        headers = {}
        last_error = None
        endpoint_clean = endpoint.lstrip('/')

        # 1. Try Supervisor API
        if self.supervisor_token:
            headers = {"Authorization": f"Bearer {self.supervisor_token}"}
            supervisor_url = f"{self.supervisor_api_base_url}/{endpoint_clean}"
            _LOGGER.debug(f"Attempting Supervisor API request to: {supervisor_url}")
            try:
                async with self.websession.request(
                    method, supervisor_url, headers=headers, params=params, data=data, json=json_data, timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if 200 <= response.status < 300:
                        _LOGGER.debug(f"Supervisor API success ({response.status}) for {endpoint}")
                        try:
                            if response.status == 204: return {}
                            return await response.json()
                        except (aiohttp.ContentTypeError, json.JSONDecodeError) as json_err:
                            content_text = await response.text()
                            _LOGGER.error(f"Supervisor API non-JSON content (status {response.status}): {json_err}. Content: {content_text[:100]}")
                            last_error = UpdateFailed(f"Supervisor API returned non-JSON: {content_text[:100]}")
                    # --- Handle specific non-success codes before fallback ---
                    elif response.status == 404:
                         _LOGGER.warning(f"Supervisor API 404 for {endpoint}. Check addon slug/endpoint. Falling back.")
                         last_error = UpdateFailed(f"Supervisor API 404 for {endpoint}")
                    elif response.status == 401:
                         _LOGGER.warning(f"Supervisor API 401 for {endpoint}. Check token. Falling back.")
                         last_error = UpdateFailed(f"Supervisor API 401 for {endpoint}")
                    else:
                        response_text = await response.text()
                        _LOGGER.warning(f"Supervisor API failed ({response.status}) for {endpoint}. Response: {response_text[:200]}. Falling back.")
                        last_error = UpdateFailed(f"Supervisor API failed ({response.status}): {response_text[:100]}")

            except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
                _LOGGER.warning(f"Supervisor API connection error for {endpoint}: {err}. Falling back.")
                last_error = UpdateFailed(f"Supervisor API connection error: {err}")
            except Exception as err:
                 _LOGGER.error(f"Unexpected Supervisor API error for {endpoint}: {err}", exc_info=True)
                 last_error = UpdateFailed(f"Unexpected Supervisor API error: {err}")
        else:
            _LOGGER.debug("SUPERVISOR_TOKEN not found, skipping Supervisor API attempt.")
            # Set a dummy error to trigger fallback logic
            last_error = UpdateFailed("Supervisor token not found")

        # 2. Try Direct API (if Supervisor failed or wasn't attempted)
        if last_error:
            direct_url = f"{self.direct_api_base_url}/{endpoint_clean}"
            _LOGGER.info(f"Attempting direct API fallback to: {direct_url}")
            # Clear Supervisor auth header if it was set
            headers = {}
            try:
                async with self.websession.request(
                    method, direct_url, headers=headers, params=params, data=data, json=json_data, timeout=aiohttp.ClientTimeout(total=15) # Slightly longer timeout for direct?
                ) as response:
                    if 200 <= response.status < 300:
                        _LOGGER.debug(f"Direct API success ({response.status}) for {endpoint}")
                        try:
                             if response.status == 204: return {}
                             return await response.json()
                        except (aiohttp.ContentTypeError, json.JSONDecodeError) as json_err:
                            content_text = await response.text()
                            _LOGGER.error(f"Direct API non-JSON content (status {response.status}): {json_err}. Content: {content_text[:100]}")
                            raise UpdateFailed(f"Direct API returned non-JSON: {content_text[:100]}") # Raise failure
                    else:
                        response_text = await response.text()
                        _LOGGER.error(f"Direct API request failed ({response.status}) for {endpoint}. Response: {response_text[:200]}")
                        raise UpdateFailed(f"Direct API failed ({response.status}): {response_text[:100]}") # Raise failure

            except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
                _LOGGER.error(f"Direct API connection error for {endpoint}: {err}")
                raise UpdateFailed(f"Direct API connection error: {err}") # Raise failure
            except UpdateFailed as err:
                 _LOGGER.error(f"Direct API UpdateFailed: {err}")
                 raise err # Re-raise UpdateFailed from non-200/non-JSON direct response
            except Exception as err:
                 _LOGGER.exception(f"Unexpected direct API error for {endpoint}: {err}")
                 raise UpdateFailed(f"Unexpected direct API error: {err}") # Raise failure

        # If last_error is still set here, it means Supervisor failed AND Direct wasn't attempted or also failed.
        # The logic above should raise UpdateFailed in all direct failure cases.
        # This part should ideally not be reached if Supervisor succeeded initially or Direct failed properly.
        if last_error:
             _LOGGER.error(f"API request failed after attempting Supervisor and/or Direct. Last error: {last_error}")
             raise last_error # Raise the originally captured error if Direct path didn't execute or failed silently
        else:
            # This case means Supervisor succeeded, which should have returned earlier.
            _LOGGER.error("Reached unexpected end of _request function: Supervisor must have succeeded but did not return.")
            raise UpdateFailed("Unexpected state in API request logic after Supervisor success")

    async def verify_connection(self):
        """Verify connection to the addon API by trying the ping endpoint."""
        _LOGGER.info("Verifying connection to Finance Assistant API...")
        try:
            # Use '/api/ping' or similar simple GET endpoint in the addon
            # Addon needs a simple endpoint like /api/ping
            _LOGGER.info(f"Attempting API ping...") # Log less detail here
            await self._request("GET", "ping") # Use ping endpoint
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
