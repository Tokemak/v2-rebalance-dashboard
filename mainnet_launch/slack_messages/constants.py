from enum import Enum


class CircleEmoji(Enum):
    RED = "🔴"
    GREEN = "🟢"
    YELLOW = "🟡"


_TESTING_CHANNEL_ID = "C09MHUS35V0"
_PRODUCTION_CHANNEL_ID = "C09JUJDJYQH"


class SlackChannel(Enum):
    TESTING = _TESTING_CHANNEL_ID
    PRODUCTION = _PRODUCTION_CHANNEL_ID
