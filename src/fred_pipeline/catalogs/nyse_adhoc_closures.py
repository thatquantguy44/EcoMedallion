"""Reviewed NYSE full-day closures outside the recurring holiday rules.

This is a partial, maintained catalog, not a complete exchange calendar. Add
an entry only with an exchange announcement that covers the NYSE equity market.
"""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class NyseAdhocClosure:
    date: date
    name: str
    source_url: str


NYSE_ADHOC_CLOSURES = (
    NyseAdhocClosure(
        date(2018, 12, 5),
        "National Day of Mourning - President George H. W. Bush",
        "https://ir.theice.com/press/news-details/2018/New-York-Stock-Exchange-to-Honor-President-George-H-W-Bush/default.aspx",
    ),
    NyseAdhocClosure(
        date(2025, 1, 9),
        "National Day of Mourning - President Jimmy Carter",
        "https://ir.theice.com/press/news-details/2024/The-New-York-Stock-Exchange-Will-Close-Markets-on-January-9-to-Honor-the-Passing-of-Former-President-Jimmy-Carter-on-National-Day-of-Mourning/default.aspx",
    ),
)
