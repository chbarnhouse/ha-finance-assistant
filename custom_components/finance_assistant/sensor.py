"""Finance Assistant sensor platform."""
import logging
from datetime import datetime, timedelta, date
import traceback # Added for more detailed exception logging
from typing import Optional # Import Optional

from homeassistant.components.sensor import SensorEntity, SensorDeviceClass, SensorStateClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    ATTR_ATTRIBUTION,
    CURRENCY_DOLLAR,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util # Import datetime utilities
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers import device_registry as dr # Import device registry

from .const import DOMAIN
from . import FinanceAssistantDataUpdateCoordinator # Import directly from __init__.py

_LOGGER = logging.getLogger(__name__)

# Define sensor types
SENSOR_TYPES = {
    "ynab_cash_balance": {"name": "YNAB Cash Balance", "icon": "mdi:cash", "category": "YNAB Summary"},
    "ynab_cash_liquid": {"name": "YNAB Cash Liquid", "icon": "mdi:cash-fast", "category": "YNAB Summary"},
    "ynab_cash_frozen": {"name": "YNAB Cash Frozen", "icon": "mdi:cash-lock", "category": "YNAB Summary"},
    "ynab_cash_deep_freeze": {"name": "YNAB Cash Deep Freeze", "icon": "mdi:cash-lock-open", "category": "YNAB Summary"},
    "ynab_credit_balance": {"name": "YNAB Credit Balance", "icon": "mdi:credit-card", "category": "YNAB Summary"},
    # Transaction Summaries - Today
    "transactions_today_inflow": {"name": "Transactions Today Inflow", "icon": "mdi:arrow-down-bold-circle-outline", "category": "Transaction Summary"},
    "transactions_today_outflow": {"name": "Transactions Today Outflow", "icon": "mdi:arrow-up-bold-circle-outline", "category": "Transaction Summary"},
    "transactions_today_net": {"name": "Transactions Today Net", "icon": "mdi:swap-vertical-bold", "category": "Transaction Summary"},
    # Transaction Summaries - Next 7 Days (Scheduled)
    "scheduled_next_7_days_inflow": {"name": "Scheduled Next 7 Days Inflow", "icon": "mdi:arrow-down-bold-circle-outline", "category": "Transaction Summary"},
    "scheduled_next_7_days_outflow": {"name": "Scheduled Next 7 Days Outflow", "icon": "mdi:arrow-up-bold-circle-outline", "category": "Transaction Summary"},
    "scheduled_next_7_days_net": {"name": "Scheduled Next 7 Days Net", "icon": "mdi:swap-vertical-bold", "category": "Transaction Summary"},
    # Transaction Summaries - Next 30 Days (Scheduled)
    "scheduled_next_30_days_inflow": {"name": "Scheduled Next 30 Days Inflow", "icon": "mdi:arrow-down-bold-circle-outline", "category": "Transaction Summary"},
    "scheduled_next_30_days_outflow": {"name": "Scheduled Next 30 Days Outflow", "icon": "mdi:arrow-up-bold-circle-outline", "category": "Transaction Summary"},
    "scheduled_next_30_days_net": {"name": "Scheduled Next 30 Days Net", "icon": "mdi:swap-vertical-bold", "category": "Transaction Summary"},
    # Next Inflow/Outflow (Scheduled)
    "scheduled_next_inflow_date": {"name": "Scheduled Next Inflow Date", "icon": "mdi:calendar-arrow-down", "category": "Transaction Summary"},
    "scheduled_next_inflow_amount": {"name": "Scheduled Next Inflow Amount", "icon": "mdi:cash-plus", "category": "Transaction Summary"},
    "scheduled_next_outflow_date": {"name": "Scheduled Next Outflow Date", "icon": "mdi:calendar-arrow-up", "category": "Transaction Summary"},
    "scheduled_next_outflow_amount": {"name": "Scheduled Next Outflow Amount", "icon": "mdi:cash-minus", "category": "Transaction Summary"},
    # Calculated Financial Metrics
    "total_outflow_until_next_inflow": {"name": "Total Outflow Until Next Inflow", "icon": "mdi:cash-sync", "category": "Analytics"},
    "can_pay_off_cards_in_full": {"name": "Can Pay Off Cards In Full", "icon": "mdi:credit-card-check-outline", "category": "Analytics"},
    # Analytics
    "analytics_net_worth": {"name": "Analytics Net Worth", "icon": "mdi:chart-line", "category": "Analytics"},
    "analytics_total_student_debt": {"name": "Analytics Total Student Debt", "icon": "mdi:school-outline", "category": "Analytics"},
    "analytics_total_car_loan": {"name": "Analytics Total Car Loan", "icon": "mdi:car-outline", "category": "Analytics"},
    "analytics_sps_stock": {"name": "Analytics SPS Stock Value", "icon": "mdi:finance", "category": "Analytics"},
    # Add other sensor types later
}

# Helper function to generate device info
def _get_device_info(config_entry_id: str, category_key: str, category_name: str) -> DeviceInfo:
    """Return device information for a specific category."""
    return DeviceInfo(
        identifiers={(DOMAIN, f"{config_entry_id}-{category_key}")},
        name=f"Finance Assistant {category_name}",
        manufacturer="Finance Assistant Addon",
        via_device=(DOMAIN, config_entry_id) # Link to the main integration config entry device
    )

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Finance Assistant sensors based on a config entry."""
    _LOGGER.debug("Setting up Finance Assistant sensors")
    coordinator: FinanceAssistantDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]

    # --- Create the main integration device FIRST --- PREVIOUSLY MISSING
    device_registry = dr.async_get(hass)
    device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, entry.entry_id)},
        name="Finance Assistant", # Name for the main device
        manufacturer="Finance Assistant Addon",
        # model="Addon Integration", # Optional: Add model if desired
        # sw_version=coordinator.data.get("addon_version", "Unknown"), # Optional: If addon version is available
    )
    _LOGGER.debug(f"Ensured main device exists for entry ID: {entry.entry_id}")
    # --- End main device creation ---

    # Wait for coordinator to do its first update
    await coordinator.async_config_entry_first_refresh()

    # Add explicit check and logging immediately after refresh
    if coordinator.data is None:
        _LOGGER.error("Coordinator data is None after first refresh. Cannot set up sensors.")
        return
    if not isinstance(coordinator.data, dict):
        _LOGGER.error(f"Coordinator data is not a dictionary after first refresh (Type: {type(coordinator.data)}). Content: {str(coordinator.data)[:500]}")
        return

    _LOGGER.debug(f"DEBUG: Coordinator data type *after* first refresh: {type(coordinator.data)}")
    _LOGGER.debug(f"DEBUG: Coordinator data content *after* first refresh: {str(coordinator.data)[:1000]}")

    entities = []
    config_entry_id = entry.entry_id

    # 1. Create main account sensors
    _LOGGER.debug(f"DEBUG: Coordinator data type before account sensor setup: {type(coordinator.data)}")
    _LOGGER.debug(f"DEBUG: Coordinator data content before account sensor setup: {str(coordinator.data)[:1000]}") # Log first 1000 chars

    if coordinator.data and isinstance(coordinator.data, dict) and coordinator.data.get("accounts") is not None:
        accounts_data = coordinator.data.get("accounts", [])
        if isinstance(accounts_data, list):
            _LOGGER.debug(f"Setting up {len(accounts_data)} Account sensors")
            account_device_info = _get_device_info(config_entry_id, "accounts", "Accounts")
            entities.extend(
                FinanceAssistantAccountSensor(
                    coordinator,
                    account_data["id"],
                    account_data.get("name", "Unknown Account"),
                    account_device_info, # Pass device info
                )
                for account_data in accounts_data
                if isinstance(account_data, dict) and not account_data.get("deleted", False)
            )
        else:
            _LOGGER.warning(f"'accounts' key found in coordinator data, but it's not a list (Type: {type(accounts_data)}). Skipping account sensors.")
    else:
        _LOGGER.warning("No valid 'accounts' data found in coordinator (or data is not a dict), cannot setup account sensors.")

    # Log coordinator data type and content right before creating the entities list
    _LOGGER.debug(f"SENSOR_SETUP: Final check before entity creation - Data type: {type(coordinator.data)}")
    _LOGGER.debug(f"SENSOR_SETUP: Final check before entity creation - Data content: {str(coordinator.data)[:1000]}")

    # 2. Create asset sensors
    _LOGGER.debug(f"DEBUG: Coordinator data type before asset sensor setup: {type(coordinator.data)}")
    _LOGGER.debug(f"DEBUG: Coordinator data content before asset sensor setup: {str(coordinator.data)[:1000]}") # Log first 1000 chars

    if coordinator.data and isinstance(coordinator.data, dict) and coordinator.data.get("assets") is not None:
        assets_data = coordinator.data.get("assets", [])
        if isinstance(assets_data, list):
            _LOGGER.debug(f"Setting up {len(assets_data)} Asset sensors")
            asset_device_info = _get_device_info(config_entry_id, "assets", "Assets")
            entities.extend(
                FinanceAssistantAssetSensor(
                    coordinator,
                    asset_data["id"],
                    asset_data.get("name", "Unknown Asset"),
                    asset_device_info, # Pass device info
                )
                for asset_data in assets_data
                if isinstance(asset_data, dict) and not asset_data.get("deleted", False)
            )
        else:
             _LOGGER.warning(f"'assets' key found in coordinator data, but it's not a list (Type: {type(assets_data)}). Skipping asset sensors.")
    else:
        _LOGGER.warning("No valid 'assets' data found in coordinator (or data is not a dict), cannot setup asset sensors.")

    # 3. Create Liability sensors
    _LOGGER.debug(f"DEBUG: Coordinator data type before liability sensor setup: {type(coordinator.data)}")
    _LOGGER.debug(f"DEBUG: Coordinator data content before liability sensor setup: {str(coordinator.data)[:1000]}")

    if coordinator.data and isinstance(coordinator.data, dict) and coordinator.data.get("liabilities") is not None:
        liabilities_data = coordinator.data.get("liabilities", [])
        if isinstance(liabilities_data, list):
            _LOGGER.debug(f"Setting up {len(liabilities_data)} Liability sensors")
            liability_device_info = _get_device_info(config_entry_id, "liabilities", "Liabilities")
            entities.extend(
                FinanceAssistantLiabilitySensor(
                    coordinator,
                    liability_data["id"],
                    liability_data.get("name", "Unknown Liability"),
                    liability_device_info, # Pass device info
                )
                for liability_data in liabilities_data
                if isinstance(liability_data, dict) and not liability_data.get("deleted", False) and not liability_data.get("closed", False)
            )
        else:
             _LOGGER.warning(f"'liabilities' key found in coordinator data, but it's not a list (Type: {type(liabilities_data)}). Skipping liability sensors.")
    else:
        _LOGGER.warning("No valid 'liabilities' data found in coordinator (or data is not a dict), cannot setup liability sensors.")

    # 4. Create Credit Card sensors
    _LOGGER.debug(f"DEBUG: Coordinator data type before credit card sensor setup: {type(coordinator.data)}")
    _LOGGER.debug(f"DEBUG: Coordinator data content before credit card sensor setup: {str(coordinator.data)[:1000]}")

    if coordinator.data and isinstance(coordinator.data, dict) and coordinator.data.get("credit_cards") is not None:
        credit_cards_data = coordinator.data.get("credit_cards", [])
        if isinstance(credit_cards_data, list):
            _LOGGER.debug(f"Setting up {len(credit_cards_data)} Credit Card sensors")
            credit_card_device_info = _get_device_info(config_entry_id, "credit_cards", "Credit Cards")
            entities.extend(
                FinanceAssistantCreditCardSensor(
                    coordinator,
                    card_data["id"],
                    card_data.get("name", "Unknown Credit Card"),
                    credit_card_device_info, # Pass device info
                )
                for card_data in credit_cards_data
                if isinstance(card_data, dict) and not card_data.get("deleted", False) and not card_data.get("closed", False)
            )
        else:
             _LOGGER.warning(f"'credit_cards' key found in coordinator data, but it's not a list (Type: {type(credit_cards_data)}). Skipping credit card sensors.")
    else:
        _LOGGER.warning("No valid 'credit_cards' data found in coordinator (or data is not a dict), cannot setup credit card sensors.")

    # 5. Create Summary/Calculated Sensors by Category
    _LOGGER.debug("Setting up Summary sensors by category")
    if coordinator.data and isinstance(coordinator.data, dict):
        # Group sensors by category
        sensors_by_category = {}
        for sensor_key, details in SENSOR_TYPES.items():
            category_name = details.get("category")
            if not category_name:
                _LOGGER.warning(f"Sensor type '{sensor_key}' missing 'category' definition. Skipping.")
                continue
            if category_name not in sensors_by_category:
                sensors_by_category[category_name] = []
            sensors_by_category[category_name].append((sensor_key, details))

        # Create entities for each category with appropriate device info
        for category_name, sensors in sensors_by_category.items():
            category_key = category_name.lower().replace(" ", "_") # e.g., ynab_summary
            device_info = _get_device_info(config_entry_id, category_key, category_name)
            _LOGGER.debug(f"Creating {len(sensors)} sensors for category '{category_name}' with device {category_key}")
            entities.extend(
                FinanceAssistantSummarySensor(
                    coordinator,
                    sensor_key,
                    details["name"],
                    details["icon"],
                    device_info, # Pass device info
                )
                for sensor_key, details in sensors
            )
    else:
        _LOGGER.warning("Coordinator data is not a valid dictionary. Skipping summary sensors.")

    _LOGGER.debug(f"Adding {len(entities)} Finance Assistant sensors")
    if entities:
        async_add_entities(entities)
    else:
        _LOGGER.info("No entities to add for Finance Assistant.")


# --- Helper Functions ---
def safe_parse_ynab_date(date_str: str) -> Optional[date]:
    """Safely parse YNAB date string, handling potential errors and formats."""
    if not date_str:
        return None
    try:
        # Try standard ISO format first
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        try:
            # Try RFC 1123 format (e.g., "Fri, 10 Nov 2023 00:00:00 GMT")
            # This might need adjustment based on the exact format observed
            # Python's %Z directive behavior can vary. Consider dateutil if needed.
            parsed_dt = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')
            # Convert to local timezone if necessary, though YNAB dates are usually just dates
            return parsed_dt.date()
        except ValueError as e:
            _LOGGER.warning(f"Could not parse date string: '{date_str}'. Error: {e}")
            _LOGGER.debug(f"Traceback: {traceback.format_exc()}") # Add traceback for debugging
            return None

def ynab_milliunits_to_float(milliunits):
    """Convert YNAB milliunits to float."""
    if milliunits is None:
        return 0.0
    return float(milliunits) / 1000.0

# --- Base Class for Coordinator Sensors ---
class FinanceAssistantBaseSensor(CoordinatorEntity):
    """Base class for Finance Assistant sensors using the coordinator."""
    def __init__(self, coordinator: FinanceAssistantDataUpdateCoordinator):
        super().__init__(coordinator)
        # No context handling - it conflicts with HA's internal context system
        # Device info is now handled by specific sensor classes

    @property
    def unit_of_measurement(self):
        """Return the unit of measurement."""
        if self._attr_device_class == SensorDeviceClass.MONETARY:
            return "$"
        return self._attr_native_unit_of_measurement

# --- Account Sensor (Existing, slightly modified base) ---
class FinanceAssistantAccountSensor(FinanceAssistantBaseSensor):
    """Representation of a Finance Assistant account balance sensor."""

    _attr_has_entity_name = True  # Use helper property for name
    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = CURRENCY_DOLLAR
    _attr_suggested_display_precision = 2 # Set precision via class attribute

    def __init__(self, coordinator, account_id, account_name, device_info: DeviceInfo):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._account_id = account_id
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_account_{account_id}"
        self._attr_name = account_name
        self._attr_device_info = device_info # Assign device info passed from setup
        self._attr_native_value = None
        self._attr_extra_state_attributes = {}
        self._entity_picture = None
        self._original_name = account_name
        self._last_account_data = None

        _LOGGER.debug(f"AccountSensor initialized: ID={account_id}, Name={account_name}")

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added to hass."""
        await super().async_added_to_hass()
        # Initial update using cached coordinator data
        self._handle_coordinator_update()

    @property
    def state(self):
        """Return the state of the entity."""
        # Format value as string with exactly 2 decimal places
        try:
            return f"{float(self._attr_native_value):.2f}"
        except (TypeError, ValueError):
            _LOGGER.warning(f"Could not format state for {self._attr_name}: {self._attr_native_value}")
            return "0.00"

    def _update_internal_state(self, account_name):
        """Update the sensor's internal state based on the latest data."""
        if not self.coordinator.data or not isinstance(self.coordinator.data, dict):
            _LOGGER.warning(f"Coordinator data missing or not a dict for account {self._account_id}")
            self._attr_available = False
            return

        accounts = self.coordinator.data.get("accounts", [])
        if not isinstance(accounts, list):
            _LOGGER.warning(f"\'accounts\' data is not a list for account {self._account_id}")
            self._attr_available = False
            return

        account_data = self._find_data_by_id(accounts, self._account_id)

        if account_data:
            # Check if data has actually changed
            if account_data == self._last_account_data:
                 _LOGGER.debug(f"Account data for {self._account_id} hasn't changed. Skipping update.")
                 self._attr_available = True # Still available even if data is the same
                 return

            _LOGGER.debug(f"Updating state for account {self._account_id} with data: {account_data}")
            self._attr_native_value = ynab_milliunits_to_float(account_data.get("balance"))

            # Determine display name based on manual settings
            manual_bank_name = account_data.get("bank")
            include_bank = account_data.get("include_bank_in_name", True) # Default true if missing
            base_name = account_data.get("name", self._original_name) # Fallback to original name

            if include_bank and manual_bank_name:
                self._attr_name = f"{manual_bank_name} {base_name}"
            else:
                self._attr_name = base_name

            # --- Populate Attributes ---
            new_attributes = {
                "ynab_id": account_data.get("id"),
                "ynab_type": account_data.get("type"), # Original YNAB type
                "account_type": account_data.get("account_type"), # Potentially manual override
                "bank": account_data.get("bank"),
                "last_4_digits": account_data.get("last_4_digits"),
                "on_budget": account_data.get("on_budget"),
                "closed": account_data.get("closed"),
                "cleared_balance": ynab_milliunits_to_float(account_data.get("cleared_balance")),
                "uncleared_balance": ynab_milliunits_to_float(account_data.get("uncleared_balance")),
                "transfer_payee_id": account_data.get("transfer_payee_id"),
                "direct_import_linked": account_data.get("direct_import_linked"),
                "direct_import_in_error": account_data.get("direct_import_in_error"),
                "last_reconciled_at": account_data.get("last_reconciled_at"),
                "debt_original_balance": ynab_milliunits_to_float(account_data.get("debt_original_balance")),
                "debt_interest_rates": account_data.get("debt_interest_rates"), # This might be a dict
                "debt_minimum_payments": account_data.get("debt_minimum_payments"), # This might be a dict
                "debt_escrow_amounts": account_data.get("debt_escrow_amounts"), # This might be a dict
                "deleted": account_data.get("deleted"),
                # Allocation details (ensure defaults or proper handling if None)
                "allocation_liquid": ynab_milliunits_to_float(account_data.get("allocation_liquid")), # Convert if needed, depends on source
                "allocation_frozen": ynab_milliunits_to_float(account_data.get("allocation_frozen")),
                "allocation_deep_freeze": ynab_milliunits_to_float(account_data.get("allocation_deep_freeze")),
                "notes": account_data.get("notes"), # Use the combined notes field
            }
            # Filter out None values before setting attributes
            self._attr_extra_state_attributes = {k: v for k, v in new_attributes.items() if v is not None}
            # --- End Populate Attributes ---

            self._attr_available = True
            self._last_account_data = account_data # Cache the data
            _LOGGER.debug(f"State updated for account {self._account_id}. New Value: {self._attr_native_value}, New Attrs: {self._attr_extra_state_attributes}")

        else:
            _LOGGER.warning(f"No data found for account ID {self._account_id} in coordinator update.")
            self._attr_available = False
            self._last_account_data = None # Reset cache if data disappears

    def _find_data_by_id(self, data_list, target_id):
        """Helper to find the specific account data by ID."""
        return next((item for item in data_list if isinstance(item, dict) and item.get("id") == target_id), None)

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        # Even if we can't find the account in the current data, we still want to show the sensor
        # as available with the last known value if we have coordinator data
        return (
            self.coordinator.last_update_success
            and self.coordinator.data is not None
            and isinstance(self.coordinator.data, dict)
        )

    async def async_update(self) -> None:
        """Update the entity.
        Only used by the generic entity update service.
        """
        await self.coordinator.async_request_refresh()

    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        if not self.coordinator.data or "accounts" not in self.coordinator.data:
            # self._attr_available = False # Availability is handled by @property
            return

        # Find the updated account data that matches this sensor's account ID
        account_data = next((acc for acc in self.coordinator.data["accounts"] if acc.get('id') == self._account_id), None)

        if account_data:
            self._update_internal_state(account_data.get('name', 'Unknown Account'))
            self.async_write_ha_state()
        # else: # Handle case where account might disappear from API response (e.g., closed and filtered out)
            # self._attr_available = False # Availability handled by @property
            # self.async_write_ha_state()


class FinanceAssistantAssetSensor(FinanceAssistantBaseSensor):
    """Implementation of a YNAB asset sensor."""

    _attr_has_entity_name = True
    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = CURRENCY_DOLLAR
    _attr_suggested_display_precision = 2 # Set precision via class attribute

    def __init__(self, coordinator, asset_id, asset_name, device_info: DeviceInfo):
        """Initialize the sensor."""
        super().__init__(coordinator) # Pass coordinator to base class
        self._asset_id = asset_id
        self._original_name = asset_name # Store original name for logging
        self._last_asset_data = None # Cache last known good data

        # Attributes to store detailed info
        self._asset_data = None
        self._manual_details = None # To store details from manual_assets.json
        # Initialize extra attributes dictionary - NEW
        self._attr_extra_state_attributes = {
            "ynab_id": self._asset_id,
            "linked_entity_id": None,
            "shares": None,
            "calculated_value": None,
            "ynab_value": None, # Add YNAB value attribute
            "ynab_value_last_updated_on": None # Add last updated attribute
        }
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_{asset_id}"
        self._attr_device_info = device_info # Set device info
        self._attr_has_entity_name = True
        self._attr_name = asset_name

        _LOGGER.debug(f"AssetSensor initialized: ID={asset_id}, Name={asset_name}")

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added to hass."""
        await super().async_added_to_hass()
        # Initial update using cached coordinator data
        self._handle_coordinator_update()

    def state(self):
        """Return the state (balance)."""
        # The native value is set in _handle_coordinator_update
        return self._attr_native_value

    def _find_data_by_id(self, data_list, target_id):
        """Helper to find the specific asset data by ID."""
        return next((item for item in data_list if isinstance(item, dict) and item.get("id") == target_id), None)

    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        _LOGGER.debug(f"Handling coordinator update for asset: {self.name} ({self._asset_id})")
        # Assume unavailable until data is verified
        self._attr_available = False
        self._attr_native_value = None # Reset state

        if self.coordinator.data and isinstance(self.coordinator.data, dict):
            # Find the specific asset data using the asset_id
            assets_list = self.coordinator.data.get("assets", [])
            self._asset_data = self._find_data_by_id(assets_list, self._asset_id)

            # Find the manual details for this asset
            manual_assets_dict = self.coordinator.data.get("manual_assets", {})
            self._manual_details = manual_assets_dict.get(self._asset_id)
            _LOGGER.debug(f"Asset {self.name}: Fetched YNAB Data = {self._asset_data}")
            _LOGGER.debug(f"Asset {self.name}: Fetched Manual Details = {self._manual_details}")

            # --- Combined State and Attribute Logic --- NEW/REVISED ---
            if self._asset_data: # Requires YNAB data to exist
                # Set Main State (YNAB Value)
                ynab_value_milliunits = self._asset_data.get("balance")
                if isinstance(ynab_value_milliunits, int):
                    self._attr_native_value = ynab_milliunits_to_float(ynab_value_milliunits)
                    _LOGGER.debug(f"Asset {self.name}: Set native value (state) to {self._attr_native_value}")
                else:
                    _LOGGER.warning(f"Asset {self.name}: YNAB balance is not an integer: {ynab_value_milliunits}")
                    self._attr_native_value = None # Explicitly set to None if invalid

                # Prepare Extra Attributes
                linked_entity_id = self._manual_details.get("entity_id") if self._manual_details else None
                shares_str = self._manual_details.get("shares") if self._manual_details else None
                last_updated_ts = self._manual_details.get("ynab_value_last_updated_on") if self._manual_details else None
                shares = None
                calculated_value = None

                # Calculate value if possible
                if linked_entity_id and shares_str:
                    try:
                        shares = float(shares_str)
                        if shares <= 0:
                            raise ValueError("Shares must be positive")

                        entity_state = self.hass.states.get(linked_entity_id)
                        if entity_state is None:
                            _LOGGER.warning(f"Entity {linked_entity_id} not found for asset {self.name}")
                            calculated_value = None # Set to None if entity not found
                        else:
                            try:
                                current_price = float(entity_state.state)
                                calculated_value = round(current_price * shares, 2)
                            except (ValueError, TypeError):
                                _LOGGER.warning(f"Entity {linked_entity_id} state '{entity_state.state}' is not a valid number for asset {self.name}")
                                calculated_value = None # Set to None if price invalid

                    except (ValueError, TypeError) as e:
                        _LOGGER.warning(f"Invalid shares value '{shares_str}' for asset {self.name}: {e}")
                        calculated_value = None # Set to None if shares invalid
                    except Exception as e:
                         _LOGGER.error(f"Unexpected error calculating value for asset {self.name}: {e}")
                         calculated_value = None # Set to None on unexpected error
                else:
                    calculated_value = None # No calculation possible

                # Update attributes dictionary
                # Start fresh each update to avoid stale attributes if data disappears
                self._attr_extra_state_attributes = {
                    "ynab_id": self._asset_id,
                    "ynab_type": self._asset_data.get("ynab_type", self._asset_data.get("type")), # Use YNAB type if available
                    "on_budget": self._asset_data.get("on_budget"),
                    "cleared_balance": ynab_milliunits_to_float(self._asset_data.get("cleared_balance")),
                    "uncleared_balance": ynab_milliunits_to_float(self._asset_data.get("uncleared_balance")),
                    "deleted": self._asset_data.get("deleted"),
                    "linked_entity_id": linked_entity_id,
                    "shares": shares_str, # Store the raw value from config
                    "ynab_value": self._attr_native_value, # Use the value we set for the state
                    "ynab_value_last_updated_on": last_updated_ts,
                    "calculated_value": calculated_value
                }
                # Remove None attributes for cleaner display
                self._attr_extra_state_attributes = {k: v for k, v in self._attr_extra_state_attributes.items() if v is not None}

                self._attr_available = True # Mark available since we processed data
                _LOGGER.debug(f"Asset {self.name}: Final attributes = {self._attr_extra_state_attributes}")
                # --- End Combined Logic ---

            else:
                _LOGGER.warning(f"Asset {self.name}: Did not find YNAB data in coordinator update.")
                # Keep state None and unavailable
        else:
            _LOGGER.warning(f"Asset {self.name}: Coordinator data unavailable or not a dict.")
            # Keep state None and unavailable

        # Write state regardless of availability
        self.async_write_ha_state()


# --- Liability Sensor ---
class FinanceAssistantLiabilitySensor(FinanceAssistantBaseSensor):
    """Representation of a Finance Assistant liability balance sensor."""

    _attr_has_entity_name = True
    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = CURRENCY_DOLLAR
    _attr_suggested_display_precision = 2

    def __init__(self, coordinator, liability_id, liability_name, device_info: DeviceInfo):
        """Initialize the liability sensor."""
        super().__init__(coordinator)
        self._liability_id = liability_id
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_liability_{liability_id}"
        self._attr_name = liability_name
        self._attr_device_info = device_info
        self._attr_native_value = None
        self._attr_extra_state_attributes = {}
        self._original_name = liability_name
        self._last_liability_data = None

        _LOGGER.debug(f"LiabilitySensor initialized: ID={liability_id}, Name={liability_name}")

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added to hass."""
        await super().async_added_to_hass()
        self._handle_coordinator_update()

    @property
    def state(self):
        """Return the state (balance)."""
        try:
            # Liabilities balance is negative in YNAB, show as positive debt value
            return f"{abs(float(self._attr_native_value)):.2f}"
        except (TypeError, ValueError):
            _LOGGER.warning(f"Could not format state for liability {self._attr_name}: {self._attr_native_value}")
            return "0.00"

    def _update_internal_state(self, liability_name):
        """Update the sensor's internal state."""
        if not self.coordinator.data or not isinstance(self.coordinator.data, dict):
            self._attr_available = False
            return

        liabilities = self.coordinator.data.get("liabilities", [])
        liability_data = self._find_data_by_id(liabilities, self._liability_id)

        if liability_data:
            if liability_data == self._last_liability_data:
                self._attr_available = True
                return

            self._attr_native_value = ynab_milliunits_to_float(liability_data.get("balance"))
            self._attr_name = liability_data.get("name", self._original_name)

            new_attributes = {
                "ynab_id": liability_data.get("id"),
                "ynab_type": liability_data.get("type"),
                "liability_type": liability_data.get("liability_type"), # Manual type
                "bank": liability_data.get("bank"), # Manual bank
                "on_budget": liability_data.get("on_budget"),
                "closed": liability_data.get("closed"),
                "cleared_balance": ynab_milliunits_to_float(liability_data.get("cleared_balance")),
                "uncleared_balance": ynab_milliunits_to_float(liability_data.get("uncleared_balance")),
                "transfer_payee_id": liability_data.get("transfer_payee_id"),
                "last_reconciled_at": liability_data.get("last_reconciled_at"),
                "deleted": liability_data.get("deleted"),
                "starting_balance": liability_data.get("starting_balance"),
                "start_date": liability_data.get("start_date"),
                "interest_rate": liability_data.get("interest_rate"),
                # Original YNAB debt fields for reference
                "debt_original_balance": ynab_milliunits_to_float(liability_data.get("debt_original_balance")),
                "debt_interest_rates": liability_data.get("debt_interest_rates"),
                "debt_minimum_payments": liability_data.get("debt_minimum_payments"),
                "debt_escrow_amounts": liability_data.get("debt_escrow_amounts"),
            }
            self._attr_extra_state_attributes = {k: v for k, v in new_attributes.items() if v is not None}
            self._attr_available = True
            self._last_liability_data = liability_data
        else:
            self._attr_available = False
            self._last_liability_data = None

    def _find_data_by_id(self, data_list, target_id):
        """Helper to find the specific liability data by ID."""
        return next((item for item in data_list if isinstance(item, dict) and item.get("id") == target_id), None)

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return (
            self.coordinator.last_update_success
            and self.coordinator.data is not None
            and isinstance(self.coordinator.data, dict)
        )

    async def async_update(self) -> None:
        await self.coordinator.async_request_refresh()

    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        if not self.coordinator.data or "liabilities" not in self.coordinator.data:
            return

        liabilities_list = self.coordinator.data.get("liabilities", [])
        if not isinstance(liabilities_list, list):
            return

        liability_data = next((item for item in liabilities_list if item.get('id') == self._liability_id), None)

        if liability_data:
            self._update_internal_state(liability_data.get('name', 'Unknown Liability'))
            self.async_write_ha_state()


# --- Credit Card Sensor ---
class FinanceAssistantCreditCardSensor(FinanceAssistantBaseSensor):
    """Representation of a Finance Assistant credit card balance sensor."""

    _attr_has_entity_name = True
    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = CURRENCY_DOLLAR
    _attr_suggested_display_precision = 2

    def __init__(self, coordinator, card_id, card_name, device_info: DeviceInfo):
        """Initialize the credit card sensor."""
        super().__init__(coordinator)
        self._card_id = card_id
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_credit_card_{card_id}"
        self._attr_name = card_name
        self._attr_device_info = device_info
        self._attr_native_value = None
        self._attr_extra_state_attributes = {}
        self._original_name = card_name
        self._last_card_data = None

        _LOGGER.debug(f"CreditCardSensor initialized: ID={card_id}, Name={card_name}")

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added to hass."""
        await super().async_added_to_hass()
        self._handle_coordinator_update()

    @property
    def state(self):
        """Return the state (balance)."""
        try:
             # Credit card balance is negative in YNAB, show as positive debt value
            return f"{abs(float(self._attr_native_value)):.2f}"
        except (TypeError, ValueError):
            _LOGGER.warning(f"Could not format state for credit card {self._attr_name}: {self._attr_native_value}")
            return "0.00"

    def _update_internal_state(self, card_name):
        """Update the sensor's internal state."""
        if not self.coordinator.data or not isinstance(self.coordinator.data, dict):
            self._attr_available = False
            return

        credit_cards = self.coordinator.data.get("credit_cards", [])
        card_data = self._find_data_by_id(credit_cards, self._card_id)

        if card_data:
            if card_data == self._last_card_data:
                self._attr_available = True
                return

            self._attr_native_value = ynab_milliunits_to_float(card_data.get("balance"))

            # Determine display name based on manual settings
            manual_bank_name = card_data.get("bank")
            include_bank = card_data.get("include_bank_in_name", True) # Default true if missing
            base_name = card_data.get("card_name", self._original_name) # Use manual card_name first

            if include_bank and manual_bank_name:
                self._attr_name = f"{manual_bank_name} {base_name}"
            else:
                self._attr_name = base_name

            new_attributes = {
                "ynab_id": card_data.get("id"),
                "ynab_name": card_data.get("name"), # Original YNAB name
                "ynab_type": card_data.get("type"),
                "bank": card_data.get("bank"),
                "last_4_digits": card_data.get("last_4_digits"),
                "expiration_date": card_data.get("expiration_date"),
                "auto_pay_day_1": card_data.get("auto_pay_day_1"),
                "auto_pay_day_2": card_data.get("auto_pay_day_2"),
                "credit_limit": card_data.get("credit_limit"),
                "payment_methods": card_data.get("payment_methods"),
                "notes": card_data.get("notes"), # Manual notes
                "ynab_note": card_data.get("note"), # YNAB notes
                "on_budget": card_data.get("on_budget"),
                "closed": card_data.get("closed"),
                "cleared_balance": ynab_milliunits_to_float(card_data.get("cleared_balance")),
                "uncleared_balance": ynab_milliunits_to_float(card_data.get("uncleared_balance")),
                "transfer_payee_id": card_data.get("transfer_payee_id"),
                "last_reconciled_at": card_data.get("last_reconciled_at"),
                "deleted": card_data.get("deleted"),
                # Basic Reward Info (more complex structure later)
                "reward_structure_type": card_data.get("reward_structure_type"),
                "base_rate": card_data.get("base_rate"),
            }
            self._attr_extra_state_attributes = {k: v for k, v in new_attributes.items() if v is not None}
            self._attr_available = True
            self._last_card_data = card_data
        else:
            self._attr_available = False
            self._last_card_data = None

    def _find_data_by_id(self, data_list, target_id):
        """Helper to find the specific credit card data by ID."""
        return next((item for item in data_list if isinstance(item, dict) and item.get("id") == target_id), None)

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return (
            self.coordinator.last_update_success
            and self.coordinator.data is not None
            and isinstance(self.coordinator.data, dict)
        )

    async def async_update(self) -> None:
        await self.coordinator.async_request_refresh()

    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        if not self.coordinator.data or "credit_cards" not in self.coordinator.data:
            return

        credit_cards_list = self.coordinator.data.get("credit_cards", [])
        if not isinstance(credit_cards_list, list):
            return

        card_data = next((item for item in credit_cards_list if item.get('id') == self._card_id), None)

        if card_data:
            self._update_internal_state(card_data.get('name', 'Unknown Credit Card'))
            self.async_write_ha_state()


# --- Summary/Calculated Sensor ---
class FinanceAssistantSummarySensor(FinanceAssistantBaseSensor):
    """Sensor for summary values calculated from coordinator data."""
    def __init__(self, coordinator, sensor_key, sensor_name, sensor_icon, device_info: DeviceInfo):
        """Initialize the summary sensor."""
        super().__init__(coordinator)
        self._sensor_key = sensor_key
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_summary_{sensor_key}"
        self._attr_name = sensor_name
        self._attr_icon = sensor_icon
        self._attr_device_info = device_info # Assign device info passed from setup

        # Determine device class and state class based on key
        if "balance" in sensor_key or "amount" in sensor_key or "inflow" in sensor_key or "outflow" in sensor_key or "net" in sensor_key or "_value" in sensor_key or "_debt" in sensor_key or sensor_key == "can_pay_off_cards_in_full" or sensor_key == "total_outflow_until_next_inflow":
            self._attr_device_class = SensorDeviceClass.MONETARY
            self._attr_state_class = SensorStateClass.TOTAL # Measurement or Total?
            self._attr_native_unit_of_measurement = CURRENCY_DOLLAR
            self._attr_suggested_display_precision = 2
        elif "date" in sensor_key:
            self._attr_device_class = SensorDeviceClass.DATE
            self._attr_state_class = None # Dates don't have state class
            self._attr_native_unit_of_measurement = None
        else:
            # Default for other types (e.g., counts, maybe ratios later)
            self._attr_device_class = None
            self._attr_state_class = SensorStateClass.MEASUREMENT
            self._attr_native_unit_of_measurement = None

        self._attr_native_value = self._default_state()
        self._attr_extra_state_attributes = {ATTR_ATTRIBUTION: "Data provided by YNAB"}

        _LOGGER.debug(f"SummarySensor initialized: Key={sensor_key}, Name={sensor_name}, Device={device_info['name']}")

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added to hass."""
        await super().async_added_to_hass()
        # Initial update using cached coordinator data
        self._handle_coordinator_update()

    @property
    def state(self):
        """Return the state of the entity."""
        value = None # Initialize value
        try:
            # Determine which calculation to run based on sensor key
            if self._sensor_key in [
                "ynab_cash_balance", "ynab_cash_liquid", "ynab_cash_frozen",
                "ynab_cash_deep_freeze", "ynab_credit_balance"
            ]:
                value = self._calculate_ynab_balances()
            elif self._sensor_key in [
                "transactions_today_inflow", "transactions_today_outflow", "transactions_today_net"
            ]:
                value = self._calculate_today_transactions()
            elif "scheduled_next_7_days" in self._sensor_key:
                value = self._calculate_scheduled_transactions(7)
            elif "scheduled_next_30_days" in self._sensor_key:
                value = self._calculate_scheduled_transactions(30)
            elif self._sensor_key in [
                "scheduled_next_inflow_date", "scheduled_next_inflow_amount",
                "scheduled_next_outflow_date", "scheduled_next_outflow_amount"
            ]:
                value = self._calculate_next_scheduled()
            elif self._sensor_key == "total_outflow_until_next_inflow":
                value = self._calculate_total_outflow_until_next_inflow()
            elif self._sensor_key == "can_pay_off_cards_in_full":
                value = self._calculate_can_pay_off_cards()
            elif self._sensor_key.startswith("analytics_"):
                # Placeholder for future analytics calculations
                 _LOGGER.debug(f"Analytics sensor {self._sensor_key} requested, returning default.")
                 value = self._default_state() # Use default for now
            else:
                _LOGGER.warning(f"Calculation logic missing for sensor key: {self._sensor_key}")
                value = self._default_state()

            # --- Add type check BEFORE formatting ---
            if not isinstance(value, (datetime, date, int, float, bool, type(None))):
                 _LOGGER.error(f"Invalid type ({type(value)}) returned from calculation for {self._sensor_key}. Value: {value}. Using default.")
                 value = self._default_state() # Fallback to default if type is wrong
            # -----------------------------------------

            # Format the output based on type
            formatted_state = self._format_state(value)
            # For date types returning None, state should be 'Unknown' or similar, not Python None
            # Handled within _format_state now, but good to be aware
            return formatted_state if formatted_state is not None else "Unknown"

        except Exception as e:
            _LOGGER.error(f"Error calculating state property for {self._sensor_key}: {e}", exc_info=True)
            # Return default state on any calculation error
            return self._default_state()

    def _default_state(self):
        """Return the default state based on sensor type."""
        # Ensure default state matches expected type for _format_state
        if "date" in self._sensor_key:
            return None # None is handled by _format_state for dates
        elif "can_pay" in self._sensor_key:
            return False # Return boolean False as default
        else:
            # Default to 0.0 for monetary/numeric sensors
            return 0.0

    def _format_state(self, value):
        """Format the calculated value based on sensor type."""
        key = self._sensor_key
        # Handle None specifically for date types first
        if value is None and "date" in key:
            return "Unknown" # Explicitly return "Unknown" string for UI
        # Handle expected types
        elif isinstance(value, (datetime, date)):
             return value.isoformat()
        elif isinstance(value, bool): # Handle boolean for can_pay_off_cards
            return "True" if value else "False"
        elif isinstance(value, (int, float)):
            try:
                # Always format monetary values to 2 decimal places
                if self._attr_device_class == SensorDeviceClass.MONETARY or 'amount' in key or 'balance' in key or 'net' in key or 'outflow' in key or 'inflow' in key:
                     return f"{float(value):.2f}"
                else:
                     # Handle potential non-monetary floats/ints if needed later
                     return str(value)
            except (TypeError, ValueError):
                _LOGGER.error(f"Could not format numeric state for {self._attr_name}: {value}")
                return "0.00" # Default numeric format
        # Handle unexpected None for non-date sensors or other invalid types
        elif value is None:
             _LOGGER.warning(f"Got unexpected None value for non-date sensor {key}. Returning default.")
             return self._default_state() # Return default state string representation
        else:
             _LOGGER.warning(f"Unexpected value type ({type(value)}) for formatting sensor {key}. Value: {value}")
             # Return default state string representation
             default_val = self._default_state()
             return self._format_state(default_val) # Re-format the default value

    def _get_helper_data(self):
        """Prepare common data needed for calculations. Returns defaults if data missing."""
        hass_tz = dt_util.get_time_zone(self.hass.config.time_zone)
        now = dt_util.now(time_zone=hass_tz)
        today = now.date()
        # --- Provide default empty lists if data is missing ---
        coordinator_data = self.coordinator.data if isinstance(self.coordinator.data, dict) else {}
        transactions = coordinator_data.get("transactions", [])
        scheduled_transactions = coordinator_data.get("scheduled_transactions", [])
        accounts = coordinator_data.get("accounts", []) # Cash/Checking/Savings
        assets = coordinator_data.get("assets", [])
        liabilities = coordinator_data.get("liabilities", [])
        credit_cards = coordinator_data.get("credit_cards", [])
        # Ensure they are lists
        transactions = transactions if isinstance(transactions, list) else []
        scheduled_transactions = scheduled_transactions if isinstance(scheduled_transactions, list) else []
        accounts = accounts if isinstance(accounts, list) else []
        assets = assets if isinstance(assets, list) else []
        liabilities = liabilities if isinstance(liabilities, list) else []
        credit_cards = credit_cards if isinstance(credit_cards, list) else []
        # ----------------------------------------------------

        def safe_parse_ynab_date(date_str):
            if not date_str or not isinstance(date_str, str): return None
            try:
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                try:
                    dt_obj = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S GMT')
                    return dt_obj.date()
                except ValueError:
                    _LOGGER.debug(f"Could not parse date string in known formats: {date_str}", exc_info=True)
                    return None

        return hass_tz, now, today, transactions, scheduled_transactions, accounts, assets, liabilities, credit_cards, safe_parse_ynab_date

    def _calculate_ynab_balances(self):
        """Calculates YNAB cash and credit balances."""
        _, _, _, _, _, accounts, _, _, credit_cards, _ = self._get_helper_data()
        balance = 0.0 # Default to float 0.0

        try:
            if self._sensor_key == "ynab_cash_balance":
                cash_accounts = [a for a in accounts if isinstance(a, dict) and a.get("account_type", "").lower() in ["checking", "savings", "cash"] and not a.get("closed")]
                balance = sum(ynab_milliunits_to_float(a.get("balance")) for a in cash_accounts if isinstance(a, dict)) # Added check
            elif self._sensor_key == "ynab_cash_liquid":
                 cash_accounts = [a for a in accounts if isinstance(a, dict) and a.get("account_type", "").lower() in ["checking", "savings", "cash"]]
                 balance = sum(ynab_milliunits_to_float(a.get("allocation_liquid")) for a in cash_accounts if isinstance(a, dict)) # Added check
            elif self._sensor_key == "ynab_cash_frozen":
                 cash_accounts = [a for a in accounts if isinstance(a, dict) and a.get("account_type", "").lower() in ["checking", "savings", "cash"]]
                 balance = sum(ynab_milliunits_to_float(a.get("allocation_frozen")) for a in cash_accounts if isinstance(a, dict)) # Added check
            elif self._sensor_key == "ynab_cash_deep_freeze":
                 cash_accounts = [a for a in accounts if isinstance(a, dict) and a.get("account_type", "").lower() in ["checking", "savings", "cash"]]
                 balance = sum(ynab_milliunits_to_float(a.get("allocation_deep_freeze")) for a in cash_accounts if isinstance(a, dict)) # Added check
            elif self._sensor_key == "ynab_credit_balance":
                active_cards = [c for c in credit_cards if isinstance(c, dict) and not c.get("closed")]
                balance = sum(ynab_milliunits_to_float(c.get("balance")) for c in active_cards if isinstance(c, dict)) # Added check
            else:
                # Should not happen, but return default numeric
                 _LOGGER.warning(f"_calculate_ynab_balances called for unexpected key: {self._sensor_key}")
                 return 0.0
            # Ensure result is always float
            return float(round(balance, 2))
        except Exception as e:
             _LOGGER.error(f"Error in _calculate_ynab_balances for {self._sensor_key}: {e}", exc_info=True)
             return 0.0 # Return default numeric on error

    def _calculate_today_transactions(self):
        """Calculates transaction summaries for today."""
        _, _, today, transactions, _, _, _, _, _, safe_parse_ynab_date = self._get_helper_data()
        amounts = []
        total = 0.0 # Default to float

        try:
            if self._sensor_key == "transactions_today_inflow":
                amounts = [ynab_milliunits_to_float(t.get("amount")) for t in transactions if isinstance(t, dict) and safe_parse_ynab_date(t.get("date")) == today and t.get("amount", 0) > 0]
            elif self._sensor_key == "transactions_today_outflow":
                # Check t is dict before accessing keys
                amounts = [ynab_milliunits_to_float(t.get("amount")) for t in transactions if isinstance(t, dict) and safe_parse_ynab_date(t.get("date")) == today and t.get("amount", 0) < 0]
                return float(round(abs(sum(amounts)), 2))
            elif self._sensor_key == "transactions_today_net":
                # Check t is dict before accessing keys
                amounts = [ynab_milliunits_to_float(t.get("amount")) for t in transactions if isinstance(t, dict) and safe_parse_ynab_date(t.get("date")) == today]
            else:
                _LOGGER.warning(f"_calculate_today_transactions called for unexpected key: {self._sensor_key}")
                return 0.0
            total = sum(amounts)
            return float(round(total, 2))
        except Exception as e:
             _LOGGER.error(f"Error in _calculate_today_transactions for {self._sensor_key}: {e}", exc_info=True)
             return 0.0 # Default numeric

    def _calculate_scheduled_transactions(self, days):
        """Calculates scheduled transaction summaries for the next N days."""
        _, _, today, _, scheduled_transactions, _, _, _, _, safe_parse_ynab_date = self._get_helper_data()
        total = 0.0 # Default
        amounts = []

        try:
            end_date = today + timedelta(days=days)
            relevant_key_part = self._sensor_key.replace(f"scheduled_next_{days}_days_", "")

            for st in scheduled_transactions:
                 if not isinstance(st, dict): continue # Skip non-dict items
                 next_date = safe_parse_ynab_date(st.get("date_next"))
                 if next_date and today <= next_date < end_date:
                     amount = ynab_milliunits_to_float(st.get("amount")) # Handles None
                     if relevant_key_part == "inflow" and amount > 0:
                         amounts.append(amount)
                     elif relevant_key_part == "outflow" and amount < 0:
                         amounts.append(amount)
                     elif relevant_key_part == "net":
                         amounts.append(amount)

            total = sum(amounts)
            if relevant_key_part == "outflow":
                return float(round(abs(total), 2))
            else:
                return float(round(total, 2))
        except Exception as e:
             _LOGGER.error(f"Error in _calculate_scheduled_transactions for {self._sensor_key}: {e}", exc_info=True)
             return 0.0 # Default numeric

    def _calculate_next_scheduled(self):
        """Finds the next scheduled inflow or outflow."""
        _, _, today, _, scheduled_transactions, _, _, _, _, safe_parse_ynab_date = self._get_helper_data()
        default_return = None if "date" in self._sensor_key else 0.0

        try:
            if "inflow" in self._sensor_key:
                sign_filter = lambda amount: amount > 0
            else: # outflow
                sign_filter = lambda amount: amount < 0

            valid_scheduled_tx = []
            for st in scheduled_transactions:
                 if not isinstance(st, dict): continue
                 next_date = safe_parse_ynab_date(st.get("date_next"))
                 # Check date validity and amount sign
                 if next_date is not None and next_date >= today and sign_filter(st.get("amount", 0)):
                     valid_scheduled_tx.append(st)

            if not valid_scheduled_tx:
                return default_return

            def safe_date_key(st):
                dt = safe_parse_ynab_date(st.get("date_next"))
                return dt if dt is not None else datetime.max.date()

            next_tx = min(valid_scheduled_tx, key=safe_date_key)

            if "date" in self._sensor_key:
                parsed_date = safe_parse_ynab_date(next_tx.get("date_next"))
                return parsed_date # Returns None if parsing fails inside, handled by _format_state
            else: # amount
                amount = ynab_milliunits_to_float(next_tx.get("amount"))
                result = abs(amount) if "outflow" in self._sensor_key else amount
                return float(round(result, 2))
        except Exception as e:
             _LOGGER.error(f"Error in _calculate_next_scheduled for {self._sensor_key}: {e}", exc_info=True)
             return default_return

    def _calculate_total_outflow_until_next_inflow(self):
        """Calculates total outflow between now and the next scheduled inflow."""
        _, _, today, _, scheduled_transactions, _, _, _, _, safe_parse_ynab_date = self._get_helper_data()
        total_outflow = 0.0 # Default

        try:
            # Find next inflow date safely
            next_inflow_date = None
            valid_inflow_tx = []
            for st in scheduled_transactions:
                 if not isinstance(st, dict): continue
                 next_date = safe_parse_ynab_date(st.get("date_next"))
                 if next_date is not None and next_date >= today and st.get("amount", 0) > 0:
                      valid_inflow_tx.append(st)

            if valid_inflow_tx:
                def safe_date_key(st):
                    dt = safe_parse_ynab_date(st.get("date_next"))
                    return dt if dt is not None else datetime.max.date()
                next_inflow_tx = min(valid_inflow_tx, key=safe_date_key)
                next_inflow_date = safe_parse_ynab_date(next_inflow_tx.get("date_next"))

            if not next_inflow_date:
                return 0.0 # No upcoming inflow

            # Sum outflows before that date
            outflow_amounts = []
            for st in scheduled_transactions:
                if not isinstance(st, dict): continue
                next_date = safe_parse_ynab_date(st.get("date_next"))
                # Check date validity, range, and amount sign
                if next_date and today <= next_date < next_inflow_date and st.get("amount", 0) < 0:
                     outflow_amounts.append(ynab_milliunits_to_float(st.get("amount")))

            total_outflow = abs(sum(outflow_amounts))
            return float(round(total_outflow, 2))
        except Exception as e:
             _LOGGER.error(f"Error in _calculate_total_outflow_until_next_inflow: {e}", exc_info=True)
             return 0.0 # Default numeric

    def _calculate_can_pay_off_cards(self):
        """Check if liquid cash balance covers total credit card debt."""
        try:
            liquid_cash = self._calculate_ynab_balances_for_key("ynab_cash_liquid")
            credit_balance = self._calculate_ynab_balances_for_key("ynab_credit_balance")
            # Ensure comparison with numeric types
            if isinstance(liquid_cash, (int, float)) and isinstance(credit_balance, (int, float)):
                 # Positive credit balance means debt, so we check if cash >= debt
                 # Note: YNAB balance for CC is negative for debt, positive for overpayment.
                 # So check if liquid_cash + credit_balance >= 0
                 return (liquid_cash + credit_balance) >= 0
            else:
                 _LOGGER.warning("Could not calculate can_pay_off_cards due to non-numeric inputs.")
                 return False # Default to False if inputs are invalid
        except Exception as e:
             _LOGGER.error(f"Error in _calculate_can_pay_off_cards: {e}", exc_info=True)
             return False # Default to False on error

    # Helper to call calculation logic internally without recursive state calls
    def _calculate_ynab_balances_for_key(self, key):
        original_key = self._sensor_key
        self._sensor_key = key
        try:
            return self._calculate_ynab_balances()
        finally:
            self._sensor_key = original_key

    @property
    def available(self) -> bool:
        """Return if entity is available based on the data it needs."""
        # Basic availability check
        if not self.coordinator.last_update_success or self.coordinator.data is None:
            return False

        # For transaction-based sensors, availability depends on having transaction data
        if self._sensor_key.startswith("transaction_") and not self._has_transaction_data():
            # Still report as available but with zero values
            return True

        # For scheduled transaction-based sensors
        if self._sensor_key.startswith("scheduled_") and not self._has_scheduled_transaction_data():
            # Still report as available but with zero values
            return True

        # For account-based sensors, check if we have accounts data
        if self._sensor_key.startswith("ynab_") and "accounts" not in self.coordinator.data:
            return False

        return True

    def _has_transaction_data(self):
        """Check if we have transaction data available."""
        if not self.coordinator.data:
            return False

        # Check if transactions key exists and has content
        if "transactions" not in self.coordinator.data or not self.coordinator.data["transactions"]:
            if self._sensor_key.startswith("transaction_"):
                _LOGGER.warning(f"Transaction data not available for {self._sensor_key}. "
                              f"This may be due to a YNAB API change. The sensor will show zero values.")
            return False

        return True

    def _has_scheduled_transaction_data(self):
        """Check if we have scheduled transaction data available."""
        if not self.coordinator.data:
            return False

        # Check if scheduled_transactions key exists and has content
        if "scheduled_transactions" not in self.coordinator.data or not self.coordinator.data["scheduled_transactions"]:
            if self._sensor_key.startswith("scheduled_"):
                _LOGGER.warning(f"Scheduled transaction data not available for {self._sensor_key}. "
                              f"This may be due to a YNAB API change. The sensor will show zero values.")
            return False

        return True

    # No extra_state_attributes needed for simple summary sensors for now
