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
# Import the API client and exceptions
from .api import (
    FinanceAssistantApiClient,
    FinanceAssistantApiClientAuthenticationError,
    FinanceAssistantApiClientError,
    ApiException,
)

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

        # Instantiate the API client object and assign to self.api
        self.api = FinanceAssistantApiClient(
            session=self.websession,
            supervisor_url=self.supervisor_url,
            direct_url=self.direct_url,
            supervisor_token=self.supervisor_token
        )

        _LOGGER.debug("Coordinator initialized. API client created.")

        # Call super().__init__ AFTER defining attributes used by it
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
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

    async def verify_connection(self):
        """Verify connection to the addon API by trying to ping it."""
        _LOGGER.info("Verifying connection to Finance Assistant addon API (via API client)...")
        try:
            # Use the API client's request method for ping
            await self.api.async_ping()
            _LOGGER.info("Addon API connection successful (via API client).")
        except (FinanceAssistantApiClientError, FinanceAssistantApiClientAuthenticationError) as err:
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

    async def _async_update_data(self) -> dict:
        """Fetch data from API endpoint and YNAB.

        This is the place to fetch data from your API endpoint,
        or correlate data from multiple sources (e.g. YNAB and local addon data).
        """
        _LOGGER.debug("COORDINATOR: Starting data update...") # Log start
        try:
            # Attempt to fetch data from the addon's API first
            # Initialize data structure
            combined_data = {
                "accounts": [],
                "assets": [],
                "liabilities": [],
                "credit_cards": [],
                "manual_assets": {}, # Store manual asset details keyed by YNAB ID
                "ynab_accounts": [], # Keep raw YNAB accounts separate initially
                "banks": [], # Add banks list
                "account_types": [], # Add account_types list
                "asset_types": [], # Add asset_types list
                "liability_types": [] # Add liability_types list
            }

            _LOGGER.debug("COORDINATOR: Fetching data from addon API via client...")
            # Use the API client instance (self.api)
            addon_data = await self.api.async_get_all_data()
            _LOGGER.debug(f"COORDINATOR: Received addon data: {addon_data}")

            # Process data from addon
            if addon_data:
                 # Extract different data types, handling potential missing keys gracefully
                 combined_data["accounts"] = addon_data.get("accounts", [])
                 combined_data["banks"] = addon_data.get("banks", [])
                 combined_data["account_types"] = addon_data.get("account_types", [])
                 combined_data["asset_types"] = addon_data.get("asset_types", [])
                 combined_data["liability_types"] = addon_data.get("liability_types", [])

                 # Process manual assets specifically
                 if isinstance(addon_data.get("manual_assets"), list):
                     for asset_detail in addon_data["manual_assets"]:
                         if asset_detail and isinstance(asset_detail, dict) and asset_detail.get("ynab_id"):
                             combined_data["manual_assets"][asset_detail["ynab_id"]] = asset_detail
                         else:
                             _LOGGER.warning(f"COORDINATOR: Skipping manual asset detail due to missing ynab_id: {asset_detail}")
                 else:
                     _LOGGER.debug("COORDINATOR: No valid manual_assets list found in addon data.")
            else:
                _LOGGER.warning("COORDINATOR: No data received from addon API.")

            # Fetch data from YNAB if API key is configured
            ynab_api_key = self.config_entry.data.get("ynab_api_key")
            ynab_budget_id = self.config_entry.data.get("ynab_budget_id")

            if ynab_api_key and ynab_budget_id:
                _LOGGER.debug("COORDINATOR: YNAB key and budget found, fetching YNAB accounts...")
                ynab_client = await self._get_ynab_client()
                if ynab_client:
                    accounts_api = ynab_api.AccountsApi(ynab_client)
                    try:
                        # Note: get_accounts may need error handling if budget_id is invalid
                        api_response = await self.hass.async_add_executor_job(
                            accounts_api.get_accounts, ynab_budget_id
                        )
                        _LOGGER.debug(f"COORDINATOR: YNAB API response received.") # Add log here
                        if hasattr(api_response, 'data') and hasattr(api_response.data, 'accounts'):
                            combined_data["ynab_accounts"] = [
                                account.to_dict()
                                for account in api_response.data.accounts
                                if not account.closed # Exclude closed accounts
                            ]
                            _LOGGER.debug(f"COORDINATOR: Processed {len(combined_data['ynab_accounts'])} open YNAB accounts.")
                            # TODO: Merge/correlate YNAB accounts with addon data if needed
                            # For now, just use YNAB accounts directly as assets/liabilities etc.
                            # Separate YNAB accounts into assets/liabilities based on type?
                            # YNAB Types: checking, savings, creditCard, cash, lineOfCredit, otherAsset, otherLiability, mortgage, autoLoan, studentLoan, personalLoan

                            # Clear existing assets/liabilities before populating from YNAB
                            combined_data["assets"] = []
                            combined_data["liabilities"] = []
                            combined_data["credit_cards"] = []

                            for acc in combined_data["ynab_accounts"]:
                                if acc.get('type') in ['checking', 'savings', 'cash', 'otherAsset']:
                                    combined_data["assets"].append(acc)
                                elif acc.get('type') in ['creditCard', 'lineOfCredit', 'otherLiability', 'mortgage', 'autoLoan', 'studentLoan', 'personalLoan']:
                                    # Maybe separate credit cards?
                                    if acc.get('type') == 'creditCard':
                                        combined_data["credit_cards"].append(acc)
                                    else:
                                        combined_data["liabilities"].append(acc)
                                else:
                                     _LOGGER.warning(f"COORDINATOR: Unhandled YNAB account type: {acc.get('type')} for account {acc.get('name')}")

                        else:
                            _LOGGER.error("COORDINATOR: Invalid YNAB API response structure.")
                    except ApiException as e:
                        _LOGGER.error(f"COORDINATOR: Exception when calling YNAB AccountsApi->get_accounts: {e}")
                    except Exception as e:
                        _LOGGER.error(f"COORDINATOR: Unexpected error fetching YNAB accounts: {e}")
                else:
                    _LOGGER.warning("COORDINATOR: Failed to initialize YNAB client.")
            else:
                _LOGGER.debug("COORDINATOR: YNAB API key or budget ID not configured. Skipping YNAB fetch.")

            # TODO: Add fetching logic for other addon data types (accounts, liabilities, credit cards) if needed
            # For now, focus on merging manual asset details with YNAB asset data

            _LOGGER.debug(f"COORDINATOR: Final combined data before return: {combined_data}")
            return combined_data

        # Use the imported exception classes
        except FinanceAssistantApiClientAuthenticationError as exception:
            _LOGGER.error("COORDINATOR: Addon authentication error.")
            raise UpdateFailed("Addon authentication error") from exception
        except FinanceAssistantApiClientError as exception:
            _LOGGER.error(f"COORDINATOR: Addon API communication error: {exception}")
            raise UpdateFailed("Addon API communication error") from exception
        except Exception as exception:
            _LOGGER.error(f"COORDINATOR: Unexpected error during data update: {exception}")
            # Log traceback for unexpected errors
            import traceback
            _LOGGER.error(traceback.format_exc())
            raise UpdateFailed("Unexpected error during data update") from exception
