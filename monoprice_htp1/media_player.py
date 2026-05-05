"""Support for the Monoprice HTP-1."""

from homeassistant.components.media_player import (
    MediaPlayerEntity,
    MediaPlayerEntityFeature,
    MediaPlayerState,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .aiohtp1 import Htp1
from .const import DOMAIN, LOGGER


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Monoprice HTP-1 config entry."""
    htp1 = hass.data[DOMAIN][entry.entry_id]
    async_add_entities((Htp1MediaPlayer(htp1=htp1),), True)


class Htp1MediaPlayer(MediaPlayerEntity):
    """HTP-1 Media Player Entity."""

    def __init__(self, htp1: Htp1) -> None:
        """Initialize."""
        self.htp1 = htp1

    @property
    def should_poll(self) -> bool:
        """Return the polling state."""
        return False

    async def async_added_to_hass(self) -> None:
        """Run when this Entity has been added to HA."""
        await super().async_added_to_hass()

        htp1 = self.htp1

        htp1.subscribe("/muted", self._updated)
        htp1.subscribe("/powerIsOn", self._updated)
        htp1.subscribe("/volume", self._updated)
        htp1.subscribe("#connection", self._updated)

    async def async_will_remove_from_hass(self) -> None:
        """Entity being removed from hass."""
        await super().async_will_remove_from_hass()

        LOGGER.debug("async_will_remove_from_hass: stopping")
        await self.htp1.stop()

    async def _updated(self, *args, **kwargs) -> None:
        # https://developers.home-assistant.io/docs/integration_fetching_data/
        LOGGER.debug("_updated")
        htp1 = self.htp1
        self._attr_available = htp1.connected
        if htp1.connected:
            # set the volume step so that it'll be equivalent to 1db
            self._attr_volume_step = 1 / (htp1.cal_vph - htp1.cal_vpl)
        # self.async_write_ha_state()
        self.async_schedule_update_ha_state()

    @property
    def supported_features(self) -> MediaPlayerEntityFeature:
        """Flag media player features that are supported."""
        return (
            MediaPlayerEntityFeature.TURN_OFF
            | MediaPlayerEntityFeature.TURN_ON
            | MediaPlayerEntityFeature.SELECT_SOUND_MODE
            | MediaPlayerEntityFeature.SELECT_SOURCE
            | MediaPlayerEntityFeature.VOLUME_MUTE
            | MediaPlayerEntityFeature.VOLUME_SET
            | MediaPlayerEntityFeature.VOLUME_STEP
        )

    @property
    def unique_id(self) -> str:
        """Return the unique ID for this media_player."""
        return f"{self.htp1.host}"

    ## Power

    async def async_turn_on(self) -> None:
        """Turn the media player on."""
        LOGGER.debug("async_turn_on:")
        async with self.htp1 as tx:
            tx.power = True
            await tx.commit()

    async def async_turn_off(self) -> None:
        """Turn the media player off."""
        LOGGER.debug("async_turn_off:")
        async with self.htp1 as tx:
            tx.power = False
            await tx.commit()

    @property
    def state(self) -> MediaPlayerState:
        """Return the state of the player."""
        return MediaPlayerState.ON if self.htp1.power else MediaPlayerState.OFF

    ## Volume

    @property
    def volume_level(self):
        """Return the volume level of the media player (0..1)."""
        volume = self.htp1.volume
        return (volume - self.htp1.cal_vpl) / (self.htp1.cal_vph - self.htp1.cal_vpl)

    async def async_set_volume_level(self, volume: float) -> None:
        """Set the volume level, range 0..1."""
        volume = volume * (self.htp1.cal_vph - self.htp1.cal_vpl) + self.htp1.cal_vpl
        async with self.htp1 as tx:
            tx.volume = volume
            await tx.commit()

    ## Mute

    @property
    def is_volume_muted(self):
        """Whether the entity is muted."""
        return self.htp1.muted

    async def async_mute_volume(self, mute: bool) -> None:
        """Mute the entity."""
        async with self.htp1 as tx:
            tx.muted = mute
            await tx.commit()

    ## Sound Mode

    async def async_select_sound_mode(self, sound_mode: str) -> None:
        """Select sound mode."""
        async with self.htp1 as tx:
            tx.upmix = sound_mode
            await tx.commit()

    @property
    def sound_mode(self):
        """Return the current sound mode."""
        return self.htp1.upmix

    @property
    def sound_mode_list(self):
        """Return a list of available sound modes."""
        return self.htp1.upmixes

    ## Source

    async def async_select_source(self, source: str) -> None:
        """Select input source."""
        async with self.htp1 as tx:
            tx.input = source
            await tx.commit()

    @property
    def source(self):
        """Name of the current input source."""
        return self.htp1.input

    @property
    def source_list(self):
        """List of available input sources."""
        return self.htp1.inputs
