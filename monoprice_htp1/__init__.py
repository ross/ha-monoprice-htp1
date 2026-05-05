"""The Monoprice HTP-1 integration."""

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, Platform
from homeassistant.core import HomeAssistant

from .aiohtp1 import Htp1
from .const import DOMAIN
from .helpers import async_get_clientsession

PLATFORMS: list[Platform] = [Platform.MEDIA_PLAYER]


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Set up Monoprice HTP-1 from a config entry."""

    htp1 = Htp1(
        host=config_entry.data[CONF_HOST], session=async_get_clientsession(hass)
    )

    await htp1.try_connect()

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][config_entry.entry_id] = htp1

    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(
        config_entry, PLATFORMS
    )
    if unload_ok:
        hass.data[DOMAIN].pop(config_entry.entry_id)

    return unload_ok
