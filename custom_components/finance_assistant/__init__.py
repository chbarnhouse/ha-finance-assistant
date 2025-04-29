"""The Finance Assistant integration."""
import asyncio
import logging
from datetime import timedelta, datetime, date
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
from homeassistant.helpers.service import async_register_admin_service

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# Define the update interval for fetching data from the addon
SCAN_INTERVAL = timedelta(minutes=5)

PLATFORMS = [Platform.SENSOR]

# --- Service Handler --- NEW ---
def async_register_services(hass: HomeAssistant, coordinator):
    """Register services for the integration."""

    async def async_handle_update_stock_assets(call) -> None:
        """Handle the service call to update stock assets."""
        _LOGGER.info("Service finance_assistant.update_stock_assets called.")

        # 1. Fetch all data from the coordinator (which gets it from the addon)
        if not coordinator.last_update_success or not coordinator.data:
            _LOGGER.error("Cannot update stock assets: Coordinator data is unavailable or update failed.")
            persistent_notification.async_create(
                hass,
                "Finance Assistant stock update failed: Could not fetch latest data from addon.",
                title="Finance Assistant Error",
                notification_id="fa_stock_update_error",
            )
            return

        all_data = coordinator.data
        all_ynab_accounts = all_data.get("accounts", [])
        manual_assets = all_data.get("manual_assets", {})
        asset_types = all_data.get("asset_types", [])

        # Find the ID for the "Stocks" asset type
        stock_type_id = next((t.get("id") for t in asset_types if t.get("name") == "Stocks"), None)
        if not stock_type_id:
            _LOGGER.error("Cannot update stock assets: 'Stocks' asset type ID not found in addon data.")
            persistent_notification.async_create(
                hass,
                "Finance Assistant stock update failed: Could not find 'Stocks' asset type.",
                title="Finance Assistant Error",
                notification_id="fa_stock_update_error",
            )
            return

        _LOGGER.debug(f"Found 'Stocks' asset type ID: {stock_type_id}")

        # 2. Filter YNAB accounts to find eligible stock assets
        assets_to_update = []
        for acc in all_ynab_accounts:
            # Ensure it's a dictionary and not closed/deleted
            if not isinstance(acc, dict) or acc.get("closed") or acc.get("deleted"):
                continue

            # Check if it's a known YNAB asset (tracking accounts)
            # YNAB types: checking, savings, cash, creditCard, lineOfCredit, otherAsset, otherLiability, mortgage, autoLoan, studentLoan, personalLoan
            # We only care about 'otherAsset' for this purpose, but we filter by manual type ID
            ynab_account_id = acc.get("id")
            if not ynab_account_id:
                continue

            # Get manual details for this asset
            asset_details = manual_assets.get(ynab_account_id)
            if not asset_details:
                continue # No manual details, skip

            # Check if the manual type is Stocks
            if asset_details.get("type_id") != stock_type_id:
                continue # Not a stock asset based on manual type

            # Check for entity_id and shares
            entity_id = asset_details.get("entity_id")
            shares_str = asset_details.get("shares") # Shares are stored as string in JSON?
            if not entity_id or not shares_str:
                _LOGGER.debug(f"Skipping asset {acc.get('name')} ({ynab_account_id}): Missing entity_id or shares in manual details.")
                continue

            try:
                shares = float(shares_str)
                if shares <= 0:
                    raise ValueError("Shares must be positive")
            except (ValueError, TypeError):
                _LOGGER.warning(f"Skipping asset {acc.get('name')} ({ynab_account_id}): Invalid shares value '{shares_str}'.")
                continue

            # If all checks pass, add to list
            assets_to_update.append({
                "ynab_account_id": ynab_account_id,
                "name": acc.get("name"),
                "entity_id": entity_id,
                "shares": shares,
            })

        _LOGGER.info(f"Found {len(assets_to_update)} stock assets linked to HA entities to update.")

        if not assets_to_update:
            _LOGGER.info("No eligible stock assets found to update.")
            return # Nothing more to do

        # 3. Iterate and update each asset
        successful_updates = 0
        failed_updates = 0
        for asset in assets_to_update:
            _LOGGER.debug(f"Processing asset: {asset['name']}")
            entity_id = asset["entity_id"]
            shares = asset["shares"]
            ynab_id = asset["ynab_account_id"]

            # 4. Get HA entity state
            entity_state = hass.states.get(entity_id)
            if not entity_state:
                _LOGGER.error(f"Failed to update {asset['name']}: Entity {entity_id} not found.")
                failed_updates += 1
                continue

            try:
                current_price = float(entity_state.state)
            except (ValueError, TypeError):
                _LOGGER.error(f"Failed to update {asset['name']}: Entity {entity_id} state '{entity_state.state}' is not a valid number.")
                failed_updates += 1
                continue

            # 5. Calculate new value
            new_value_milliunits = int(round(current_price * shares * 1000))
            _LOGGER.debug(f"Asset: {asset['name']}, Entity: {entity_id}, Price: {current_price}, Shares: {shares}, New Value (milliunits): {new_value_milliunits}")

            # 6. Fetch current balance and calculate adjustment
            current_ynab_balance_milliunits = None
            account_data = next((acc for acc in all_ynab_accounts if acc.get("id") == ynab_id), None)
            if account_data and isinstance(account_data.get("balance"), int):
                current_ynab_balance_milliunits = account_data["balance"]
            else:
                _LOGGER.warning(f"Could not find current balance for YNAB account {asset['name']} ({ynab_id}). Skipping adjustment.")
                failed_updates += 1
                continue

            adjustment_milliunits = new_value_milliunits - current_ynab_balance_milliunits

            # Skip if difference is zero (or very small to avoid noise)
            if abs(adjustment_milliunits) < 10: # Less than 1 cent difference
                _LOGGER.info(f"Skipping adjustment for {asset['name']}: Calculated value matches YNAB balance.")
                # Consider this a success? Or just skip?
                continue

            # 7. Construct Adjustment Transaction
            today_date = date.today().isoformat()
            # Create a unique import ID: fa-adj-<accountId>-<date>
            import_id = f"fa-adj-{ynab_id}-{today_date}"
            # Limit import_id to 36 chars as per YNAB API spec
            if len(import_id) > 36:
                import_id = import_id[:36]

            # Need SaveTransaction model
            # Use ynab_api namespace
            transaction_payload_model = ynab_api.SaveTransaction(
                account_id=ynab_id,
                date=today_date,
                amount=adjustment_milliunits,
                payee_name="Market Adjustment", # Or "Stock Value Update"
                cleared="cleared",
                approved=True,
                memo=f"HA Update: {shares} shares @ ${current_price:.2f} = ${new_value_milliunits/1000:.2f}",
                import_id=import_id
            )

            # 8. Call YNAB API to create transaction
            try:
                # Get the ApiClient from the coordinator
                api_client = await coordinator._get_ynab_client()
                if not api_client:
                     _LOGGER.error(f"Failed to create adjustment for {asset['name']}: Could not get YNAB ApiClient.")
                     failed_updates += 1
                     continue

                # Instantiate the TransactionsApi
                # Use ynab_api namespace
                transactions_api_instance = ynab_api.TransactionsApi(api_client)

                # budget_id should be available from config entry
                budget_id = coordinator.config_entry.data.get("ynab_budget_id")
                if not budget_id:
                    _LOGGER.error(f"Failed to create adjustment for {asset['name']}: YNAB Budget ID not found.")
                    failed_updates += 1
                    continue

                _LOGGER.info(f"Creating adjustment transaction for {asset['name']} ({ynab_id}) in budget {budget_id} for amount {adjustment_milliunits}")
                # Call create_transaction - expects SaveTransactionsWrapper
                # Use .to_dict() on the model instance
                await transactions_api_instance.create_transaction(budget_id, {"transaction": transaction_payload_model.to_dict()})
                _LOGGER.info(f"Successfully submitted adjustment transaction for asset {asset['name']} to YNAB.")
                successful_updates += 1

            except Exception as e:
                _LOGGER.exception(f"Failed to create adjustment transaction for {asset['name']} ({ynab_id}): {e}")
                failed_updates += 1

        # 9. Final Notification
        if failed_updates > 0:
            persistent_notification.async_create(
                hass,
                f"Finance Assistant stock update completed with {failed_updates} errors and {successful_updates} successes.",
                title="Finance Assistant Stock Update Issues",
                notification_id="fa_stock_update_error", # Reuse ID to replace previous error
            )
        elif successful_updates > 0:
            persistent_notification.async_create(
                hass,
                f"Finance Assistant successfully updated {successful_updates} stock asset(s) in YNAB.",
                title="Finance Assistant Stock Update",
                notification_id="fa_stock_update_success",
            )
        else:
             _LOGGER.info("Stock update service ran, but no assets were updated (either none eligible or all failed).")
             # Optionally notify that nothing was updated?
             pass

    # Register the service
    hass.services.async_register(
        DOMAIN,
        "update_stock_assets",
        async_handle_update_stock_assets,
    )

# --- END Service Handler --- NEW ---


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
                    _LOGGER.info(f"Supervisor addon ping failed with status: {resp.status}. Will attempt direct connection.")
        except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
            _LOGGER.warning(f"Supervisor addon ping failed with connection error: {err}. Will attempt direct connection.")
        except Exception as err: # Catch unexpected errors during ping
            _LOGGER.error(f"Unexpected error during Supervisor addon ping: {err}", exc_info=True)
            _LOGGER.warning("Will attempt direct connection due to unexpected error during ping.")
    else:
        _LOGGER.debug("SUPERVISOR_TOKEN not found. Assuming direct connection needed.")

    if not use_supervisor_api:
         # Log as INFO since fallback is working as expected
         _LOGGER.info("Supervisor addon ping failed or token missing. Direct connection will be primary method.")
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

    # --- Register Services --- NEW ---
    async_register_services(hass, coordinator)
    # --- END Register Services --- NEW ---

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    # Remove services
    hass.services.async_remove(DOMAIN, "update_stock_assets") # NEW

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok

# --- Need to add YNAB client initialization and access --- NEW ---
# Corrected import: Import only the base package
import ynab_api
# Removed: from ynab_api import ApiClient, Configuration, AccountsApi
# Removed: from ynab_api.api import transactions_api # Import TransactionsApi module
# Removed: from ynab_api.model.save_transaction import SaveTransaction # Import SaveTransaction model


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

        # Initialize YNAB Client placeholder
        self._ynab_client = None

        # Call super().__init__ AFTER defining attributes used by it
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
            # config_entry=entry # Pass the config entry here
        )
        self.config_entry = entry # Store config_entry if needed elsewhere

    async def _get_ynab_client(self):
        """Initializes and returns the YNAB API **ApiClient** instance if config is valid."""
        if self._ynab_client:
            return self._ynab_client

        ynab_api_key = self.config_entry.data.get("ynab_api_key")
        if not ynab_api_key:
            _LOGGER.error("YNAB API key not found in config entry.")
            return None

        # Use ynab_api namespace
        configuration = ynab_api.Configuration()
        configuration.api_key['Authorization'] = ynab_api_key
        configuration.api_key_prefix['Authorization'] = 'Bearer'

        # Return the configured ApiClient instance
        # Use ynab_api namespace
        self._ynab_client = ynab_api.ApiClient(configuration)
        _LOGGER.info("YNAB ApiClient initialized.")
        return self._ynab_client

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
                         # Downgrade 404 from warning to info, as fallback is expected sometimes
                         _LOGGER.info(f"{primary_method} API 404 for {endpoint}. Check slug/endpoint/token. Falling back if possible.")
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
                        # Re-raising the *last_error* (from primary) might be more informative here
                        raise last_error or UpdateFailed(f"{secondary_method} API failed ({response.status}): {response_text[:100]}...")

            except (aiohttp.ClientConnectorError, asyncio.TimeoutError, socket.gaierror) as err:
                _LOGGER.error(f"{secondary_method} API connection error for {endpoint}: {err}. Raising original error.")
                raise last_error or UpdateFailed(f"{secondary_method} API connection error: {err}") # Raise original or new error
            except Exception as err:
                _LOGGER.error(f"Unexpected {secondary_method} API error for {endpoint}: {err}", exc_info=True)
                raise last_error or UpdateFailed(f"Unexpected {secondary_method} API error: {err}") # Raise original or new error

        # If we reached here and last_error still exists, it means primary failed and no secondary was attempted OR secondary also failed
        if last_error:
            _LOGGER.error(f"API request failed for {endpoint} after all attempts. Final error: {last_error}")
            raise last_error

        # If we somehow get here without returning data or raising an error (shouldn't happen)
        _LOGGER.error(f"API request for {endpoint} finished unexpectedly without result or error.")
        raise UpdateFailed("API request finished unexpectedly.")

    async def verify_connection(self):
        """Verify connection to the addon API by trying to ping it."""
        _LOGGER.info("Verifying connection to Finance Assistant addon API...")
        try:
            # Use the _request method which handles fallback logic
            await self._request("GET", "/ping")
            _LOGGER.info("Addon API connection successful.")
        except UpdateFailed as err:
            _LOGGER.error(f"Failed to connect to addon API: {err}")
            persistent_notification.async_create(
                self.hass,
                f"Could not connect to the Finance Assistant addon. Please ensure it is running and configured correctly. Error: {err}",
                title="Finance Assistant Connection Error",
                notification_id="fa_connection_error",
            )
            raise ConfigEntryNotReady(f"Failed to connect to addon API: {err}") from err
        except Exception as err:
            _LOGGER.error(f"Unexpected error during connection verification: {err}", exc_info=True)
            raise ConfigEntryNotReady(f"Unexpected error verifying connection: {err}") from err

    async def _async_update_data(self):
        """Fetch data from the Finance Assistant addon API."""
        _LOGGER.debug("Fetching all data from addon...")
        try:
            # Use the internal request method
            data = await self._request("GET", "/all_data")
            _LOGGER.debug("Successfully fetched data from addon.")
            return data
        except UpdateFailed as err:
            _LOGGER.error(f"Error fetching data from Finance Assistant addon: {err}")
            persistent_notification.async_create(
                self.hass,
                f"Could not fetch data from the Finance Assistant addon. Error: {err}",
                title="Finance Assistant Update Error",
                notification_id="fa_update_error",
            )
            raise # Re-raise the UpdateFailed exception
        except Exception as err:
             _LOGGER.error(f"Unexpected error fetching data: {err}", exc_info=True)
             raise UpdateFailed(f"Unexpected error fetching data: {err}") from err
