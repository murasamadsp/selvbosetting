#!/usr/bin/env python3
"""
Calculate estimated settlement probability by municipality using IMDi 2026 request figures.

Input source:
https://www.imdi.no/bosetting/bosettingstall/nokkeltall-bosetting-2026/

The script extracts:
- Kommune
- value from column "Antall personer kommunen har vedtatt å bosette:"

and writes a text file with probabilities:
    kommune - percentage_of_total (%)
"""

from __future__ import annotations

import argparse
import html
import json
import urllib.error
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import List, Optional


URL_DEFAULT = "https://www.imdi.no/bosetting/bosettingstall/nokkeltall-bosetting-2026/"
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "bosetting-probability-script/1.0"
NORWAY_BOUNDS = (57.2, 4.0, 72.2, 32.5)

# Real administrative-center coordinates for every Norwegian municipality (2024 borders).
# Key = municipality name as it appears in IMDi tables.
# Duplicate names carry a qualifier in parentheses matching the parsed label.
MUNICIPALITY_COORDS: dict[str, tuple[float, float]] = {
    # Oslo
    "Oslo": (59.9139, 10.7522),
    # Akershus
    "Bærum": (59.9117, 10.5252),
    "Lillestrøm": (59.9558, 11.0493),
    "Nittedal": (59.9724, 10.8533),
    "Frogn": (59.6731, 10.6671),
    "Nordre Follo": (59.7151, 10.8300),
    "Ullensaker": (60.1321, 11.1708),
    "Vestby": (59.6039, 10.7537),
    "Enebakk": (59.7403, 11.1488),
    "Nes": (60.1202, 11.4631),
    "Nesodden": (59.8581, 10.6569),
    "Eidsvoll": (60.3229, 11.2625),
    "Jevnaker": (60.2364, 10.3777),
    "Hurdal": (60.4047, 11.0710),
    "Nannestad": (60.2218, 11.0920),
    "Gjerdrum": (60.0757, 11.0287),
    "Lørenskog": (59.8866, 10.9635),
    "Ås": (59.6660, 10.7895),
    # Østfold
    "Indre Østfold": (59.6133, 11.3883),
    "Moss": (59.4323, 10.6591),
    "Våler": (59.4862, 10.8501),
    "Råde": (59.3497, 10.8373),
    "Aremark": (59.2319, 11.6870),
    "Sarpsborg": (59.2833, 11.1097),
    "Skiptvet": (59.5683, 11.2530),
    "Hvaler": (59.0338, 10.9997),
    "Rakkestad": (59.3782, 11.3458),
    "Fredrikstad": (59.2181, 10.9298),
    "Marker": (59.4164, 11.6308),
    # Buskerud
    "Kongsberg": (59.6627, 9.6529),
    "Ringerike": (60.1707, 10.2596),
    "Drammen": (59.7441, 10.2045),
    "Modum": (59.9796, 10.0100),
    "Hol": (60.6235, 8.3058),
    "Sigdal": (60.0892, 9.7506),
    "Øvre Eiker": (59.7714, 9.9083),
    "Nore og Uvdal": (60.3602, 8.9758),
    "Lier": (59.7954, 10.2540),
    "Gol": (60.7007, 8.9456),
    "Rollag": (59.9568, 9.2536),
    "Ål": (60.6274, 8.5665),
    "Flesberg": (59.8309, 9.5877),
    "Hemsedal": (60.8605, 8.5603),
    "Nesbyen": (60.5695, 9.1122),
    "Krødsherad": (60.1709, 9.5797),
    "Hole": (60.1059, 10.2497),
    "Flå": (60.3766, 9.4984),
    # Vestfold
    "Tønsberg": (59.2671, 10.4076),
    "Larvik": (59.0530, 10.0337),
    "Horten": (59.4169, 10.4858),
    "Færder": (59.2127, 10.4065),
    "Holmestrand": (59.4886, 10.3147),
    # Telemark
    "Bamble": (59.0108, 9.6055),
    "Skien": (59.2098, 9.6066),
    "Porsgrunn": (59.1427, 9.6563),
    "Tinn": (59.8741, 8.7694),
    "Fyresdal": (59.1823, 8.0903),
    "Notodden": (59.5581, 9.2643),
    "Kragerø": (58.8669, 9.4083),
    "Hjartdal": (59.5939, 8.7889),
    "Nissedal": (59.1895, 8.4747),
    "Seljord": (59.3848, 8.8308),
    "Vinje": (59.5655, 7.9730),
    "Tokke": (59.3780, 8.0670),
    "Drangedal": (59.0917, 9.0700),
    "Midt-Telemark": (59.3857, 9.0821),
    # Agder
    "Kristiansand": (58.1599, 7.9956),
    "Arendal": (58.4617, 8.7669),
    "Grimstad": (58.3405, 8.5931),
    "Lindesnes": (58.0290, 7.4610),
    "Vennesla": (58.2853, 7.9606),
    "Risør": (58.7218, 9.2278),
    "Farsund": (58.0944, 6.8048),
    "Kvinesdal": (58.3040, 6.8949),
    "Flekkefjord": (58.2962, 6.6607),
    "Vegårshei": (58.7223, 8.7823),
    "Lillesand": (58.2494, 8.3775),
    "Sirdal": (58.7618, 6.7169),
    "Tvedestrand": (58.6167, 8.9328),
    "Hægebostad": (58.4828, 7.1972),
    "Iveland": (58.4459, 8.0527),
    "Valle": (59.2021, 7.5306),
    "Åmli": (58.8213, 8.4831),
    "Åseral": (58.4909, 7.3695),
    "Birkenes": (58.3452, 8.2305),
    "Bykle": (59.5684, 7.3828),
    "Evje og Hornnes": (58.5865, 7.8033),
    "Lyngdal": (58.1408, 7.0730),
    # Rogaland
    "Stavanger": (58.9700, 5.7331),
    "Sandnes": (58.8521, 5.7351),
    "Haugesund": (59.4138, 5.2680),
    "Gjesdal": (58.8272, 5.9372),
    "Karmøy": (59.2823, 5.2989),
    "Vindafjord": (59.5112, 5.8538),
    "Hå": (58.6111, 5.6508),
    "Klepp": (58.7671, 5.6348),
    "Time": (58.7261, 5.7711),
    "Tysvær": (59.3780, 5.5046),
    "Randaberg": (59.0021, 5.6170),
    "Sauda": (59.6475, 6.3504),
    "Suldal": (59.4886, 6.2510),
    "Eigersund": (58.4541, 6.0002),
    "Strand": (59.0265, 6.0462),
    "Bjerkreim": (58.6612, 6.0410),
    "Hjelmeland": (59.2263, 6.1564),
    "Lund": (58.3595, 6.0525),
    "Bokn": (59.2341, 5.4464),
    "Sokndal": (58.3399, 6.2367),
    # Vestland
    "Bergen": (60.3913, 5.3221),
    "Alver": (60.5878, 5.1194),
    "Kinn": (61.5988, 5.0199),
    "Øygarden": (60.3906, 5.0847),
    "Askøy": (60.3989, 5.1882),
    "Bømlo": (59.7756, 5.2083),
    "Luster": (61.4484, 7.4530),
    "Kvinnherad": (59.8750, 6.2643),
    "Sogndal": (61.2298, 7.0942),
    "Sunnfjord": (61.4526, 5.8433),
    "Voss": (60.6290, 6.4118),
    "Kvam": (60.3846, 6.2239),
    "Stryn": (61.9050, 6.7227),
    "Bjørnafjorden": (60.0635, 5.6246),
    "Gloppen": (61.7756, 6.2189),
    "Høyanger": (61.2182, 6.0726),
    "Osterøy": (60.5126, 5.5236),
    "Stad": (62.0490, 5.2316),
    "Sveio": (59.6195, 5.3594),
    "Lærdal": (61.0996, 7.4821),
    "Tysnes": (59.9956, 5.5155),
    "Vaksdal": (60.4796, 5.7585),
    "Vik": (61.0864, 6.5769),
    "Austevoll": (60.0862, 5.2555),
    "Årdal": (61.2382, 7.6936),
    "Aurland": (60.9050, 7.1850),
    "Austrheim": (60.7614, 4.9560),
    "Fjaler": (61.3685, 5.7124),
    "Hyllestad": (61.2177, 5.3032),
    "Masfjorden": (60.8316, 5.3927),
    "Solund": (61.0700, 4.8379),
    "Stord": (59.7791, 5.4915),
    "Eidfjord": (60.4666, 7.0700),
    "Samnanger": (60.3966, 5.7762),
    "Fitjar": (59.8283, 5.3113),
    "Ullensvang": (60.1308, 6.6463),
    "Ulvik": (60.5668, 6.9193),
    # Møre og Romsdal
    "Ålesund": (62.4723, 6.1549),
    "Kristiansund": (63.1102, 7.7278),
    "Hareid": (62.3731, 6.0278),
    "Hustadvika": (63.0072, 7.0972),
    "Ulstein": (62.3422, 5.9638),
    "Averøy": (63.0492, 7.5264),
    "Volda": (62.1460, 6.0695),
    "Haram": (62.5389, 6.2055),
    "Sunndal": (62.6736, 8.5647),
    "Ørsta": (62.1982, 6.1288),
    "Stranda": (62.3099, 6.9413),
    "Aure": (63.2948, 8.3364),
    "Fjord": (62.2513, 7.0637),
    "Giske": (62.4850, 6.0333),
    "Gjemnes": (62.9498, 7.6437),
    "Rauma": (62.5676, 7.6916),
    "Surnadal": (62.9685, 8.7127),
    "Aukra": (62.8660, 6.5742),
    "Smøla": (63.3663, 7.7861),
    "Tingvoll": (63.0285, 8.2039),
    "Sande (Møre og Romsdal)": (62.2119, 5.7738),
    "Vanylven": (62.1488, 5.6985),
    # Innlandet
    "Gjøvik": (60.7957, 10.6914),
    "Kongsvinger": (60.1924, 12.0045),
    "Elverum": (60.8811, 11.5625),
    "Søndre Land": (60.6974, 10.2461),
    "Vestre Toten": (60.6546, 10.5574),
    "Østre Toten": (60.7257, 10.7081),
    "Gran": (60.3767, 10.5574),
    "Nordre Land": (60.8801, 10.1753),
    "Ringsaker": (60.8900, 10.7941),
    "Stange": (60.7088, 11.1669),
    "Hamar": (60.7945, 11.0678),
    "Lillehammer": (61.1153, 10.4662),
    "Sør-Odal": (60.2250, 11.6005),
    "Øystre Slidre": (61.1168, 9.0271),
    "Nord-Fron": (61.5845, 9.9534),
    "Rendalen": (61.2553, 11.1459),
    "Sel": (61.8652, 10.0127),
    "Tynset": (62.2783, 10.7743),
    "Vågå": (61.8784, 9.1373),
    "Nord-Aurdal": (60.8889, 9.3102),
    "Ringebu": (61.5158, 10.0638),
    "Sør-Fron": (61.5592, 10.0142),
    "Vestre Slidre": (61.0868, 8.9919),
    "Åmot": (61.1267, 11.5047),
    "Folldal": (62.1329, 10.0022),
    "Os (Hedm.)": (62.4831, 11.2239),
    "Tolga": (62.4181, 11.0382),
    "Trysil": (61.3152, 12.2656),
    "Alvdal": (62.1097, 10.6328),
    "Dovre": (62.0706, 9.6860),
    "Etnedal": (60.8800, 9.6854),
    "Gausdal": (61.2468, 10.1800),
    "Lesja": (62.2289, 8.8691),
    "Løten": (60.8145, 11.3480),
    "Skjåk": (61.9054, 8.2289),
    "Stor-Elvdal": (61.3939, 11.0549),
    "Sør-Aurdal": (60.6358, 9.7932),
    "Øyer": (61.2615, 10.4212),
    "Lom": (61.8369, 8.5713),
    "Vang": (61.1254, 8.5582),
    "Nord-Odal": (60.3896, 11.5616),
    "Våler (Innlandet)": (60.6432, 11.9326),
    "Åsnes": (60.7433, 11.8745),
    # Trøndelag
    "Trondheim": (63.4305, 10.3951),
    "Levanger": (63.7468, 11.2992),
    "Verdal": (63.7925, 11.4843),
    "Heim": (63.3464, 9.2700),
    "Oppdal": (62.5933, 9.6907),
    "Steinkjer": (64.0150, 11.4953),
    "Stjørdal": (63.4690, 10.9170),
    "Namsos": (64.4667, 11.4961),
    "Orkland": (63.2989, 9.8499),
    "Røros": (62.5744, 11.3848),
    "Hitra": (63.5740, 8.8350),
    "Ørland": (63.7009, 9.6152),
    "Malvik": (63.4270, 10.6785),
    "Åfjord": (63.9755, 10.2311),
    "Inderøy": (63.8694, 11.2803),
    "Meråker": (63.4151, 11.7458),
    "Melhus": (63.2899, 10.2844),
    "Skaun": (63.2790, 10.2890),
    "Midtre Gauldal": (63.1543, 10.6546),
    "Overhalla": (64.4694, 11.9656),
    "Grong": (64.4591, 12.3083),
    "Frosta": (63.5929, 10.6940),
    "Frøya": (63.7282, 8.6520),
    "Lierne": (64.4736, 13.8194),
    "Rindal": (63.0436, 9.2122),
    "Selbu": (63.2086, 11.0364),
    "Rennebu": (62.8298, 10.0084),
    "Holtålen": (62.8020, 11.2830),
    "Indre Fosen": (63.5843, 10.2230),
    "Namsskogan": (64.9326, 13.1646),
    "Nærøysund": (64.8695, 11.4028),
    "Osen": (64.4244, 10.7032),
    "Flatanger": (64.4860, 10.9700),
    # Nordland
    "Bodø": (67.2804, 14.4049),
    "Narvik": (68.4385, 17.4270),
    "Alstahaug": (65.8806, 12.4948),
    "Vefsn": (65.8347, 13.1931),
    "Vågan": (68.2338, 14.5691),
    "Vestvågøy": (68.1540, 13.9590),
    "Hadsel": (68.5596, 14.7929),
    "Andøy": (69.1421, 15.7920),
    "Bø (Nordland)": (68.7531, 14.4654),
    "Gildeskål": (67.0764, 14.7277),
    "Øksnes": (68.7230, 15.0080),
    "Fauske": (67.2598, 15.3920),
    "Dønna": (66.0616, 12.3835),
    "Evenes": (68.5339, 16.5452),
    "Hemnes": (66.1944, 13.8965),
    "Lødingen": (68.4110, 16.0006),
    "Saltdal": (66.8460, 15.3840),
    "Steigen": (67.8073, 15.0530),
    "Sømna": (65.2862, 12.1226),
    "Sortland": (68.6930, 15.4133),
    "Brønnøy": (65.4624, 12.2095),
    "Hamarøy": (68.0613, 15.5224),
    "Lurøy": (66.4037, 12.8936),
    "Meløy": (66.8601, 13.7610),
    "Beiarn": (67.0091, 15.5459),
    "Bindal": (65.0646, 12.3239),
    "Grane": (65.4437, 13.5263),
    "Hattfjelldal": (65.5677, 14.0606),
    "Herøy (Nordland)": (66.0167, 12.3028),
    # Troms
    "Harstad": (68.7984, 16.5415),
    "Tromsø": (69.6496, 18.9560),
    "Senja": (69.0573, 17.1163),
    "Balsfjord": (69.2008, 19.1698),
    "Lyngen": (69.5685, 20.1949),
    "Målselv": (69.0252, 18.2688),
    "Nordreisa": (69.7628, 21.0330),
    "Sørreisa": (69.0001, 18.1915),
    "Salangen": (68.7956, 18.2437),
    "Skjervøy": (70.0333, 20.9833),
    "Tjeldsund": (68.6582, 16.5670),
    "Kvæfjord": (68.7802, 16.5371),
    "Kåfjord": (69.3997, 20.4744),
    "Lavangen": (68.7461, 18.0044),
    "Storfjord": (69.2006, 19.9048),
    "Dyrøy": (69.1112, 18.1260),
    # Finnmark
    "Alta": (69.9689, 23.2716),
    "Vadsø": (70.0743, 29.7509),
    "Sør-Varanger": (69.7252, 30.0469),
    "Hammerfest": (70.6634, 23.6821),
    "Nordkapp": (70.9787, 25.7839),
    "Tana": (70.0087, 27.0111),
    "Båtsfjord": (70.6342, 29.7124),
    "Karasjok": (69.4698, 25.5125),
    "Lebesby": (70.3827, 26.8500),
    "Loppa": (70.2385, 21.3459),
    "Porsanger": (70.0589, 25.0806),
}


@dataclass
class Record:
    county: str
    municipality: str
    accepted_to_settle: int


def clean_text(value: str) -> str:
    """Remove html tags, decode entities, normalize spaces."""
    value = html.unescape(value)
    value = repair_mojibake(value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def parse_int_cell(value: str) -> Optional[int]:
    """
    Parse integer from a table cell.
    Non-numeric values (avventer vedtak, -, :) are treated as missing.
    """
    text = clean_text(value).strip().lower()
    if text in {"", "-", ":", ":", "avventer vedtak", "avventer", "n/a", "na"}:
        return None
    # remove trailing/leading non-digit characters (just in case)
    if re.fullmatch(r"[0-9]+", text):
        return int(text)
    # Try extracting first integer token, for edge cases.
    number_match = re.search(r"\d+", text)
    if number_match is not None:
        return int(number_match.group(0))
    return None


def repair_mojibake(value: str) -> str:
    """
    Fix common UTF-8 bytes that were decoded with latin1/cp1252.
    """
    if "Ã" not in value:
        return value
    try:
        repaired = value.encode("latin1", errors="ignore").decode("utf-8")
    except Exception:
        return value
    # Keep repair only if it looks better for Nordic characters.
    if any(ch in repaired for ch in "ØøÆæÅåÃ") and "Ã" not in repaired:
        return repaired
    # If still looks like mojibake, fallback.
    if "Ã" in repaired:
        return value
    return repaired


def parse_header_candidates(raw_header: str) -> List[str]:
    cleaned = clean_text(raw_header)
    # Normalize norwegian title noise that sometimes appears with different hyphens/spaces.
    normalized = cleaned.lower().replace("\u2013", "-").replace("\u2014", "-")
    return [segment.strip() for segment in re.split(r"\|", normalized)]


def extract_county_name_before_table(html_body: str, table_start: int) -> str:
    """
    Try to infer county name from the nearest previous section heading:
    "<h3>Agder - oversikt over bosettingen i 2026</h3>" -> "Agder".
    """
    heading_pattern = re.compile(r"(?is)<h[1-6][^>]*>(.*?)</h[1-6]>")
    matches = list(heading_pattern.finditer(html_body, 0, table_start))
    if not matches:
        return ""

    heading_text = clean_text(matches[-1].group(1))
    heading_lower = heading_text.lower()
    if "oversikt over bosettingen i 2026" not in heading_lower:
        return ""
    if "fylkene i 2026" in heading_lower:
        return ""
    if "anmodning 2026" in heading_lower:
        return ""

    county_heading_pattern = re.compile(
        r"^(?P<county>.+?)\s*[-\u2013\u2014]\s*oversikt over bosettingen i 2026$",
        re.IGNORECASE,
    )
    heading_match = county_heading_pattern.match(heading_text)
    if heading_match is None:
        return ""

    county = heading_match.group("county").strip()
    return county


def is_municipality_table(headers: List[str]) -> bool:
    h_join = " | ".join(headers).lower()
    if not headers:
        return False
    # The first column for municipality tables is exactly "Kommune".
    # Fylke-level tables should be excluded.
    first_header = headers[0].strip().lower()
    if not first_header.startswith("kommune"):
        return False
    if "har vedtatt" in h_join and "bosette" in h_join:
        return True
    # fallback for changed wording
    return "antall personer kommunen har vedtatt" in h_join


def find_target_column(headers: List[str]) -> Optional[int]:
    target_hints = [
        "antall personer kommunen har vedtatt a",
        "kommunen har vedtatt a",
        "antall personer kommunen har vedtatt",
        "har vedtatt",
    ]
    h_lower = [h.lower() for h in headers]
    for i, header in enumerate(h_lower):
        if "har vedtatt" in header and "bosette" in header:
            return i
    for needle in target_hints:
        for i, header in enumerate(h_lower):
            if needle in header:
                return i
    # fallback: second column in a kommune table
    if len(h_lower) > 1:
        return 1
    return None


def decode_web_content(raw: bytes, charset: Optional[str]) -> str:
    """
    Decode bytes with fallback encodings and a small mojibake recovery step.
    """
    candidates = []
    if charset:
        candidates.append(charset)
    candidates.extend(["utf-8", "iso-8859-1", "cp1252", "windows-1252"])

    last_error = None
    for enc in candidates:
        if not enc:
            continue
        try:
            decoded = raw.decode(enc, errors="strict")
            if "Ã" in decoded and "ø" not in decoded and "Ø" not in decoded and "æ" not in decoded:
                # Typical latinized UTF-8 bytes decoded as latin1/cp1252
                latin_repair = decoded.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
                if "Ã" not in latin_repair:
                    decoded = latin_repair
            return decoded
        except Exception as exc:  # pragma: no cover - compatibility fallback
            last_error = exc

    # Last resort: decode with latin1 to avoid hard failure.
    if last_error is not None:
        print(f"WARNING: decoding issues ({last_error}), fallback to latin1", file=sys.stderr)
    return raw.decode("latin1", errors="replace")


def parse_tables(html_body: str, include_zero: bool = False) -> List[Record]:
    table_pattern = re.compile(r"(?is)<table\b[^>]*>(.*?)</table>")
    row_pattern = re.compile(r"(?is)<tr\b[^>]*>(.*?)</tr>")
    cell_pattern = re.compile(r"(?is)<t[hd]\b[^>]*>(.*?)</t[hd]>")

    records: List[Record] = []
    seen: dict = {}

    for table_match in table_pattern.finditer(html_body):
        table_html = table_match.group(0)
        table_inner = table_match.group(1)
        rows = row_pattern.findall(table_inner)
        if not rows:
            continue

        # Header is normally the first row
        headers_raw = cell_pattern.findall(rows[0])
        if not headers_raw:
            continue
        headers = parse_header_candidates(clean_text("|".join(headers_raw)))
        headers = [h for h in headers if h]
        if not is_municipality_table(headers):
            continue

        target_idx = find_target_column(headers)
        if target_idx is None:
            continue

        try:
            kommune_idx = headers.index("kommune")
        except ValueError:
            # sometimes encoding may alter whitespace/case; do fuzzy scan.
            kommune_idx = -1
            for i, h in enumerate(headers):
                if "kommune" in h:
                    kommune_idx = i
                    break
            if kommune_idx == -1:
                continue

        county_name = extract_county_name_before_table(html_body, table_match.start())
        for row in rows[1:]:
            cells = cell_pattern.findall(row)
            if len(cells) <= max(target_idx, kommune_idx):
                continue

            kommune_name = clean_text(cells[kommune_idx])
            if not kommune_name or kommune_name == "-":
                continue

            value = parse_int_cell(cells[target_idx])
            if value is None:
                continue
            if not include_zero and value == 0:
                continue

            records.append(
                Record(
                    county=county_name or "Ukjent fylke",
                    municipality=kommune_name,
                    accepted_to_settle=value,
                )
            )
            seen_key = (county_name or "Ukjent fylke", kommune_name)
            previous = seen.get(seen_key)
            if previous is None or value > previous:
                seen[seen_key] = value

    # Deduplicate municipality entries if the same table appears multiple times.
    deduped: List[Record] = [
        Record(county=k[0], municipality=k[1], accepted_to_settle=v)
        for k, v in seen.items()
    ]
    return deduped


def build_probabilities(records: List[Record], decimals: int = 2) -> List[tuple]:
    if not records:
        return []

    total = sum(r.accepted_to_settle for r in records)
    if total <= 0:
        return []

    records_sorted = sorted(records, key=lambda r: r.accepted_to_settle, reverse=True)
    return [
        (
            f"{r.county} | {r.municipality}",
            r.accepted_to_settle,
            round((r.accepted_to_settle / total) * 100, decimals),
        )
        for r in records_sorted
    ]


def write_report(path: str, lines: List[tuple], total: int, decimals: int) -> None:
    with open(path, "w", encoding="utf-8") as file:
        file.write("Kommune - sannsynlighet for bosetting (basert pa antall vedtatte flyktninger)\n")
        file.write(f"Total (sum av vedtatte tall): {total}\n")
        file.write(f"Presisjon: {decimals} desimaler\n")
        file.write("-" * 80 + "\n")
        for name, value, percent in lines:
            file.write(
                f"{name}: {percent:.{decimals}f}%  "
                f"(vedtatt å bosette: {value})\n"
            )


def parse_municipality_query(label: str) -> tuple[str, str]:
    if " | " in label:
        county, municipality = label.split(" | ", 1)
    else:
        county, municipality = "", label
    return county.strip(), municipality.strip()


def lookup_municipality_coords(municipality: str) -> Optional[tuple[float, float]]:
    """Look up real coordinates from the built-in dictionary."""
    coords = MUNICIPALITY_COORDS.get(municipality)
    if coords is not None:
        return coords
    for key, val in MUNICIPALITY_COORDS.items():
        if key.lower() == municipality.lower():
            return val
        if key.startswith(municipality) or municipality.startswith(key):
            return val
    return None


def load_geocode_cache(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as cache_file:
            data = json.load(cache_file)
            if isinstance(data, dict):
                return data
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"WARNING: cannot read geocode cache {path}: {exc}", file=sys.stderr)
    return {}


def save_geocode_cache(path: str, cache: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as cache_file:
            json.dump(cache, cache_file, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"WARNING: cannot write geocode cache {path}: {exc}", file=sys.stderr)


def geocode_with_nominatim(
    query: str,
    cache: dict,
    delay_seconds: float = 1.0,
    max_retries: int = 3,
) -> Optional[tuple[float, float]]:
    key = query.lower().strip()
    cached = cache.get(key)
    if cached == "not_found":
        return None
    if isinstance(cached, list) and len(cached) == 2:
        try:
            return float(cached[0]), float(cached[1])
        except (TypeError, ValueError):
            pass

    params = urllib.parse.urlencode(
        {
            "q": query,
            "format": "json",
            "limit": 1,
            "addressdetails": 0,
            "countrycodes": "no",
            "viewbox": "4.0,57.8,32.0,72.2",
            "bounded": 1,
        }
    )
    request_url = f"{NOMINATIM_SEARCH_URL}?{params}"

    for retry in range(max_retries + 1):
        wait = delay_seconds * (1 if retry == 0 else (retry + 1) * 2)
        if wait > 0:
            time.sleep(wait)
        request = urllib.request.Request(
            request_url,
            headers={"User-Agent": NOMINATIM_USER_AGENT},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                payload = decode_web_content(raw, response.headers.get_content_charset())
                items = json.loads(payload)
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and retry < max_retries:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                wait = delay_seconds * (retry + 2)
                if retry_after:
                    try:
                        wait = max(wait, int(retry_after))
                    except ValueError:
                        pass
                time.sleep(wait)
                continue
            print(f"WARNING: geocode failed for '{query}': HTTP {exc.code}", file=sys.stderr)
            cache[key] = "not_found"
            return None
        except Exception as exc:
            print(f"WARNING: geocode failed for '{query}': {exc}", file=sys.stderr)
            cache[key] = "not_found"
            return None

        break

    if not isinstance(items, list) or not items:
        cache[key] = "not_found"
        return None

    first = items[0]
    lat = first.get("lat")
    lon = first.get("lon")
    if lat is None or lon is None:
        cache[key] = "not_found"
        return None

    try:
        point = (float(lat), float(lon))
    except (TypeError, ValueError):
        cache[key] = "not_found"
        return None
    min_lat, min_lon, max_lat, max_lon = NORWAY_BOUNDS
    if not (min_lat <= point[0] <= max_lat and min_lon <= point[1] <= max_lon):
        cache[key] = "not_found"
        return None

    cache[key] = list(point)
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    return point


def build_heatmap_html(
    rows: List[tuple],
    output_path: str,
    geocode_cache_path: str,
    geocode_delay: float = 1.0,
    skip_geocode: bool = False,
    show_markers: bool = False,
) -> tuple[int, int]:
    geocode_cache = load_geocode_cache(geocode_cache_path)
    points: List[dict] = []
    not_found: List[str] = []
    max_value = 0

    for name, value, percent in rows:
        county, municipality = parse_municipality_query(name)

        # 1) Built-in dictionary (real coordinates)
        location = lookup_municipality_coords(municipality)

        # 2) Nominatim as optional fallback for unknown municipalities
        if location is None and not skip_geocode:
            attempts = []
            if county and county.lower() != "ukjent fylke":
                attempts.append(f"{municipality}, {county}, Norway")
            attempts.append(f"{municipality}, Norway")
            for q in attempts:
                try:
                    location = geocode_with_nominatim(q, geocode_cache, delay_seconds=geocode_delay)
                    if location is not None:
                        break
                except Exception as exc:
                    print(f"WARNING: geocode failed for '{q}': {exc}", file=sys.stderr)
                    location = None

        if location is None:
            not_found.append(municipality)
            print(f"WARNING: no coordinates for '{municipality}' ({county})", file=sys.stderr)
            continue

        lat, lon = location
        max_value = max(max_value, value)
        points.append(
            {
                "name": name,
                "lat": lat,
                "lon": lon,
                "value": value,
                "percent": percent,
            }
        )

    save_geocode_cache(geocode_cache_path, geocode_cache)

    if not points:
        return 0, len(not_found)

    points_json = json.dumps(points, ensure_ascii=False)
    template = f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>IMDi bosetting 2026 – heatmap</title>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" />
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script src=\"https://unpkg.com/leaflet.heat/dist/leaflet-heat.js\"></script>
  <style>
    html, body {{ margin: 0; padding: 0; width: 100%; height: 100%; }}
    #map {{ width: 100%; height: 100%; }}
  </style>
</head>
<body>
  <div id=\"map\"></div>
  <script>
    const points = {points_json};
    const map = L.map('map');
    const baseLayer = L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 12,
      attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);

    const markerLayer = L.layerGroup();
    const heatData = points.map(p => [p.lat, p.lon, p.value]);
    const maxValue = {max_value};
    const maxPercent = points.reduce((acc, item) => Math.max(acc, item.percent), 0);
    const showMarkers = {str(show_markers).lower()};

    const heatLayer = L.heatLayer(heatData, {{
      radius: 32,
      blur: 20,
      maxZoom: 10,
      max: maxValue,
      minOpacity: 0.35,
      gradient: {{
        0.0: '#0000ff',
        0.25: '#00ffff',
        0.5: '#00ff00',
        0.75: '#ffff00',
        1.0: '#ff0000'
      }}
    }});

    heatLayer.addTo(map);

    for (const p of points) {{
      const intensity = maxPercent > 0 ? (p.percent / maxPercent) : 0;
      const size = 3 + Math.max(2, intensity) * 10;
      const hue = 240 - 240 * intensity;
      const color = `hsl(${{hue}}, 90%, 42%)`;
      const marker = L.circleMarker([p.lat, p.lon], {{
        radius: showMarkers ? size : 0,
        color,
        fillColor: color,
        fillOpacity: showMarkers ? 0.38 : 0,
        weight: showMarkers ? 1 : 0,
        opacity: showMarkers ? 0.8 : 0
      }});
      marker.bindTooltip(
        `<b>${{p.name}}</b><br/>Вероятность: ${{p.percent.toFixed(2)}}%<br/>` +
        `Оснований: ${{p.value}}`,
        {{ sticky: true }}
      );
      marker.bindPopup(`<b>${{p.name}}</b><br/>Вероятность: ${{p.percent.toFixed(2)}}%<br/>Оснований: ${{p.value}}`);
      if (showMarkers) {{
        markerLayer.addLayer(marker);
      }}
    }}

    const bounds = L.latLngBounds(points.map(p => [p.lat, p.lon]));
    map.fitBounds(bounds, {{ padding: [24, 24] }});

    const layers = {{
      "Тепловая карта (интенсивность)": heatLayer
    }};
    if (showMarkers) {{
      layers["Точки по вероятности"] = markerLayer;
      markerLayer.addTo(map);
    }}
    L.control.layers({{}}, layers, {{ collapsed: true }}).addTo(map);

    const legend = L.control({{ position: 'bottomright' }});
    legend.onAdd = function () {{
      const div = L.DomUtil.create('div', 'info legend');
      div.style.background = 'white';
      div.style.padding = '8px';
      div.style.borderRadius = '6px';
      div.style.lineHeight = '1.25';
      div.style.fontSize = '12px';
      div.style.boxShadow = '0 0 8px rgba(0,0,0,0.2)';
      div.innerHTML = `
        <div style="font-weight:700;margin-bottom:4px;">Скала вероятности</div>
        <div style="display:flex;align-items:center;gap:4px;margin-top:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#0000ff;"></span> 0%
        </div>
        <div style="display:flex;align-items:center;gap:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#00ffff;"></span> 0.5%
        </div>
        <div style="display:flex;align-items:center;gap:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#00ff00;"></span> 1%
        </div>
        <div style="display:flex;align-items:center;gap:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#ffff00;"></span> 3%
        </div>
        <div style="display:flex;align-items:center;gap:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#ff0000;"></span> 6%+ 
        </div>
        <div style="margin-top:6px;">Слой:</div>
        <div style="display:flex;align-items:center;gap:8px;"><span style="
          width:10px;height:10px;border-radius:50%;background:#000;display:inline-block;
          opacity:0.7;"></span>Размер/цвет точки = вероятность</div>
      `;
      return div;
    }};
    legend.addTo(map);
  </script>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as file:
        file.write(template)

    return len(points), len(not_found)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate settlement probability per municipality from IMDi statistics."
    )
    parser.add_argument(
        "--url",
        default=URL_DEFAULT,
        help="Source page URL",
    )
    parser.add_argument(
        "--output",
        default="kommuner_sannsynlighet_bosetting_2026.txt",
        help="Output text file path",
    )
    parser.add_argument(
        "--decimals",
        type=int,
        default=2,
        choices=range(0, 10),
        metavar="[0-9]",
        help="Number of decimal digits in percent",
    )
    parser.add_argument(
        "--skip-zero",
        action="store_true",
        help="Do not include municipalities with accepted_to_settle == 0",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="If > 0, keep only top N municipalities",
    )
    parser.add_argument(
        "--map-output",
        help="Output HTML path for heatmap (Leaflet), e.g. kommuner_bosetting_heatmap_2026.html",
    )
    parser.add_argument(
        "--map-top",
        type=int,
        default=0,
        help="If > 0, build heatmap from top N municipalities",
    )
    parser.add_argument(
        "--geocode-cache",
        default="kommuner_geocode_cache_2026.json",
        help="JSON cache path for municipality geocoding",
    )
    parser.add_argument(
        "--geocode-delay",
        type=float,
        default=1.1,
        help="Delay in seconds between geocode requests (Nominatim etiquette)",
    )
    parser.add_argument(
        "--skip-geocode",
        action="store_true",
        help=(
            "Do not call online geocoding API. "
            "Uses cached values and county-based fallback points for map."
        ),
    )
    parser.add_argument(
        "--show-markers",
        action="store_true",
        help="Display per-commune markers in addition to heat layer",
    )
    return parser.parse_args()


def fetch_html(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; IMDiSettlementsBot/1.0)"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        raw = response.read()
    return decode_web_content(raw, charset)


def main() -> int:
    args = parse_args()
    try:
        html_body = fetch_html(args.url)
    except Exception as exc:
        print(f"ERROR: Failed to fetch URL {args.url}: {exc}", file=sys.stderr)
        return 1

    records = parse_tables(html_body, include_zero=not args.skip_zero)
    if not records:
        print("ERROR: No municipality rows were parsed.", file=sys.stderr)
        return 1

    total = sum(r.accepted_to_settle for r in records)
    rows = build_probabilities(records, decimals=args.decimals)
    rows_for_report = rows
    if args.top and args.top > 0:
        rows_for_report = rows[: args.top]

    write_report(args.output, rows_for_report, total=total, decimals=args.decimals)

    if args.map_output:
        map_rows = rows
        if args.map_top and args.map_top > 0:
            map_rows = rows[: args.map_top]
        mapped, missing = build_heatmap_html(
            map_rows,
            output_path=args.map_output,
            geocode_cache_path=args.geocode_cache,
            geocode_delay=max(0.0, args.geocode_delay),
            skip_geocode=args.skip_geocode,
            show_markers=args.show_markers,
        )
        print(f"Heatmap: {mapped} points written to {args.map_output}")
        if missing:
            print(f"Heatmap: {missing} municipalities were not geocoded")

    print(f"Done. Output: {args.output}")
    print(f"Parsed municipalities: {len(rows_for_report)}")
    if args.top and args.top > 0:
        print(f"Top limit: {args.top}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
