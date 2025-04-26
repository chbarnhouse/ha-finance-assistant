"""Config flow for Finance Assistant integration."""
import logging

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class FinanceAssistantConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Finance Assistant."""

    VERSION = "0.2.10"

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        # Check if already configured
        await self.async_set_unique_id(DOMAIN)
        self._abort_if_unique_id_configured()

        if user_input is not None:
            _LOGGER.info("Setting up Finance Assistant integration")
            # No data needed from user for now, just create the entry
            return self.async_create_entry(title="Finance Assistant", data={})

        # Show the form to the user (even if it's empty for now)
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({}), # No fields needed yet
        )

    # If you wanted to add options later (e.g., configure update interval):
    # @staticmethod
    # @callback
    # def async_get_options_flow(config_entry):
    #     """Get the options flow for this handler."""
    #     return OptionsFlowHandler(config_entry)

    async def async_step_options_flow_confirm(self, user_input=None):
        """Handle a flow initialized by the user."""
        if user_input is not None:
            # Update the config entry with new options
            return self.async_create_entry(title="", data=user_input)

        # Form to confirm options changes
        return self.async_show_form(
            step_id="options_flow_confirm",
            data_schema=vol.Schema({
                # Define your options schema here, similar to config flow
            })
        )