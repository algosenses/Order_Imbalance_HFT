from enum import Enum, unique

@unique
class Session(Enum):
    FullDay   = 0
    Morning   = 1
    Afternoon = 2
    Night     = 3